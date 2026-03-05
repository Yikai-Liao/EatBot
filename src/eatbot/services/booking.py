from __future__ import annotations

from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import threading
import time as mono_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from loguru import logger
from lark_oapi.api.application.v6 import P2ApplicationBotMenuV6
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from lark_oapi.card.model import Card
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from eatbot.config import RuntimeConfig
from eatbot.domain.cards import ReservationCardBuilder
from eatbot.domain.decision import MealPlanDecider, parse_meals
from eatbot.domain.models import Meal, MealScheduleRule, UserProfile
from eatbot.services.repositories import BitableRepository
from eatbot.adapters.feishu_clients import FeishuApiError, IMAdapter


@dataclass(slots=True, frozen=True)
class CronPreviewSnapshot:
    schedule_rules_count: int
    enabled_user_count: int
    stats_receiver_count: int
    rules_by_date: dict[date, set[Meal]]
    matched_rule_count_by_date: dict[date, int]


@dataclass(slots=True, frozen=True)
class MealFeeArchiveWindow:
    run_date: date
    start_date: date
    end_date: date


@dataclass(slots=True, frozen=True)
class MealFeeArchiveSummary:
    run_date: date
    start_date: date
    end_date: date
    user_count: int
    total_fee: Decimal


@dataclass(slots=True, frozen=True)
class CardCallbackUpdateContext:
    token: str | None
    open_message_id: str | None


class BookingService:
    _ALL_MEALS = {Meal.LUNCH, Meal.DINNER}
    _TODAY_CARD_TEXT_COMMANDS = frozenset({"订餐", "/eatbot today", "当日卡片"})
    _TODAY_CARD_MENU_EVENT_KEYS = frozenset({"当日卡片"})

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        repository: BitableRepository,
        im: IMAdapter,
        now_provider: Callable[[], datetime] | None = None,
        background_runner: Callable[[Callable[[], None]], None] | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._im = im
        self._card_builder = ReservationCardBuilder()
        self._decider = MealPlanDecider()
        self._timezone = ZoneInfo(config.timezone)
        self._now_provider = now_provider
        self._card_action_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="eatbot-card-action")
        self._background_runner = background_runner or self._default_background_runner
        self._processing_users: set[str] = set()
        self._processing_users_lock = threading.Lock()

    def send_daily_cards(self, target_date: date | None = None) -> None:
        target = target_date or self._now().date()
        rules = self._list_schedule_rules(force_refresh=True)
        plan = self._decider.decide(target, rules)
        if not plan.meals:
            logger.info("今天不发送订餐卡片: date={}", target.isoformat())
            return

        users = [user for user in self._repository.list_user_profiles() if user.enabled]
        for user in users:
            try:
                self._send_card_to_user(user=user, target_date=target, allowed_meals=plan.meals)
            except Exception:
                logger.exception("给用户发卡失败, user={}, open_id={}", user.display_name, user.open_id)

    def send_card_to_user_today(self, open_id: str) -> None:
        today = self._now().date()
        user = self._load_user(open_id)
        if user is None:
            self._im.send_text(open_id, "你不在用餐人员配置中，无法发起预约。")
            return

        rules = self._list_schedule_rules()
        plan = self._decider.decide(today, rules)
        if not plan.meals:
            self._im.send_text(open_id, f"{today.isoformat()} 不在订餐发送范围。")
            return

        self._send_card_to_user(user=user, target_date=today, allowed_meals=plan.meals)

    def send_stats(self, target_date: date, meal: Meal) -> None:
        count = self._repository.count_meal_records(target_date=target_date, meal=meal)
        receivers = self._repository.list_stats_receiver_open_ids()
        if not receivers:
            logger.info("无统计接收人配置，跳过统计发送")
            return

        text = f"{target_date.isoformat()} {meal.value} 预约人数: {count}（{_weekday_text(target_date)}）"
        for open_id in receivers:
            self._im.send_text(open_id, text)

    def build_cron_preview_snapshot(self, *, target_dates: set[date]) -> CronPreviewSnapshot:
        rules = self._list_schedule_rules()
        enabled_user_count = sum(1 for user in self._repository.list_user_profiles() if user.enabled)
        stats_receiver_count = len(self._repository.list_stats_receiver_open_ids())

        rules_by_date: dict[date, set[Meal]] = {}
        matched_rule_count_by_date: dict[date, int] = {}
        for target_date in target_dates:
            matched_rule_count_by_date[target_date] = sum(
                1 for rule in rules if rule.start_date <= target_date <= rule.end_date
            )
            plan = self._decider.decide(target_date, rules)
            rules_by_date[target_date] = set(plan.meals)

        return CronPreviewSnapshot(
            schedule_rules_count=len(rules),
            enabled_user_count=enabled_user_count,
            stats_receiver_count=stats_receiver_count,
            rules_by_date=rules_by_date,
            matched_rule_count_by_date=matched_rule_count_by_date,
        )

    def preview_daily_cards(
        self,
        *,
        target_date: date,
        snapshot: CronPreviewSnapshot | None = None,
    ) -> tuple[bool, str]:
        if snapshot is None:
            snapshot = self.build_cron_preview_snapshot(target_dates={target_date})
        meals = snapshot.rules_by_date.get(target_date, set())
        if not meals:
            matched_rule_count = snapshot.matched_rule_count_by_date.get(target_date, 0)
            if matched_rule_count > 0:
                return False, "规则结果=不发送; 命中规则但餐次为空"
            return (
                False,
                "规则结果=不发送; 周末默认不发送",
            )

        meals_text = _format_meals(meals)
        if snapshot.enabled_user_count <= 0:
            return (
                False,
                f"规则餐次={meals_text}; 启用用户=0",
            )
        return (
            True,
            f"规则餐次={meals_text}; 启用用户={snapshot.enabled_user_count}",
        )

    def preview_stats(
        self,
        *,
        meal: Meal,
        snapshot: CronPreviewSnapshot | None = None,
    ) -> tuple[bool, str]:
        if snapshot is None:
            snapshot = self.build_cron_preview_snapshot(target_dates=set())
        if snapshot.stats_receiver_count <= 0:
            return False, f"餐次={meal.value}; 统计接收人=0"
        return True, f"餐次={meal.value}; 统计接收人={snapshot.stats_receiver_count}"

    def preview_fee_archive(self, *, target_date: date) -> tuple[bool, str]:
        window = self._build_meal_fee_archive_window(target_date)
        if target_date != window.run_date:
            return (
                False,
                f"非归档日; 本月归档日={window.run_date.isoformat()}",
            )
        return (
            True,
            (
                f"归档区间={window.start_date.isoformat()}~"
                f"{window.end_date.isoformat()}（闭区间）"
            ),
        )

    def archive_meal_fees(self, *, target_date: date | None = None) -> MealFeeArchiveSummary | None:
        target = target_date or self._now().date()
        window = self._build_meal_fee_archive_window(target)
        if target != window.run_date:
            logger.debug(
                "今日非餐费归档日，跳过执行: target={} expected={}",
                target.isoformat(),
                window.run_date.isoformat(),
            )
            return None

        summaries = self._repository.list_meal_fee_summaries(start_date=window.start_date, end_date=window.end_date)
        summary_by_open_id = {item.open_id: item for item in summaries}
        users = self._repository.list_user_profiles()
        enabled_open_ids = {user.open_id for user in users if user.enabled}
        target_open_ids = sorted(set(summary_by_open_id.keys()) | enabled_open_ids)

        total_fee = Decimal("0")
        total_lunch_count = 0
        total_dinner_count = 0
        for open_id in target_open_ids:
            summary = summary_by_open_id.get(open_id)
            fee = summary.total_fee if summary else Decimal("0")
            lunch_count = summary.lunch_count if summary else 0
            dinner_count = summary.dinner_count if summary else 0
            meal_count = lunch_count + dinner_count
            total_fee += fee
            total_lunch_count += lunch_count
            total_dinner_count += dinner_count
            self._repository.upsert_meal_fee_archive_record(
                open_id=open_id,
                start_date=window.start_date,
                end_date=window.end_date,
                fee=fee,
                lunch_count=lunch_count,
                dinner_count=dinner_count,
            )
            try:
                self._im.send_text(
                    open_id,
                    (
                        f"餐费归档通知：{window.start_date.isoformat()}~{window.end_date.isoformat()}，"
                        f"你本月午餐 {lunch_count} 顿，晚餐 {dinner_count} 顿，共 {meal_count} 顿，"
                        f"餐费合计 {_format_decimal(fee)} 元。"
                    ),
                )
            except Exception:
                logger.exception("发送餐费归档通知失败: open_id={}", open_id)

        receivers = self._repository.list_stats_receiver_open_ids()
        if receivers:
            total_meal_count = total_lunch_count + total_dinner_count
            admin_text = (
                f"餐费归档表已更新：{window.start_date.isoformat()}~"
                f"{window.end_date.isoformat()}，"
                f"午餐 {total_lunch_count} 人次，晚餐 {total_dinner_count} 人次，"
                f"总计 {total_meal_count} 人次，总收款 {_format_decimal(total_fee)} 元。"
            )
            for open_id in receivers:
                try:
                    self._im.send_text(open_id, admin_text)
                except Exception:
                    logger.exception("发送餐费归档管理员通知失败: open_id={}", open_id)
        else:
            logger.info("无统计接收人配置，跳过餐费归档管理员通知")

        logger.info(
            "餐费归档完成: run_date={} start={} end={} users={} total_fee={}",
            window.run_date.isoformat(),
            window.start_date.isoformat(),
            window.end_date.isoformat(),
            len(target_open_ids),
            _format_decimal(total_fee),
        )
        return MealFeeArchiveSummary(
            run_date=window.run_date,
            start_date=window.start_date,
            end_date=window.end_date,
            user_count=len(target_open_ids),
            total_fee=total_fee,
        )

    def handle_message_event(self, data: P2ImMessageReceiveV1) -> None:
        message = data.event.message if data and data.event else None
        sender = data.event.sender if data and data.event else None
        sender_id = sender.sender_id if sender else None
        sender_open_id = sender_id.open_id if sender_id else None
        if not message or not sender_open_id:
            return

        if message.message_type != "text":
            return

        text = _extract_text_from_message_content(message.content)
        if text in self._TODAY_CARD_TEXT_COMMANDS:
            self.send_card_to_user_today(sender_open_id)

    def handle_bot_menu_event(self, data: P2ApplicationBotMenuV6) -> None:
        event = data.event if data else None
        operator = event.operator if event else None
        operator_id = operator.operator_id if operator else None
        operator_open_id = operator_id.open_id if operator_id else None
        event_key = str(event.event_key).strip() if event and event.event_key is not None else ""

        if not operator_open_id:
            return
        if event_key not in self._TODAY_CARD_MENU_EVENT_KEYS:
            return
        self.send_card_to_user_today(operator_open_id)

    def handle_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        started_at = mono_time.monotonic()
        try:
            event = data.event
            if not event or not event.action:
                return self._toast("error", "卡片参数缺失")

            callback_context = self._extract_callback_update_context(
                token=getattr(event, "token", None),
                context=getattr(event, "context", None),
            )
            level, content, card_payload = self._process_action_entry(
                operator_open_id=event.operator.open_id if event.operator else None,
                action_value=event.action.value or {},
                form_value=event.action.form_value or {},
                source="event",
                callback_context=callback_context,
            )
            return self._toast(level, content, card_payload)
        except ValueError as exc:
            return self._toast("error", str(exc))
        except Exception:
            logger.exception("处理卡片回调失败")
            return self._toast("error", "预约更新失败")
        finally:
            logger.debug("卡片回调处理耗时: {}ms source=event", int((mono_time.monotonic() - started_at) * 1000))

    def handle_card_frame_action(self, data: Card) -> dict[str, Any]:
        started_at = mono_time.monotonic()
        try:
            action = getattr(data, "action", None)
            if action is None:
                return self._toast_dict("error", "卡片参数缺失")

            callback_context = self._extract_callback_update_context(
                token=getattr(data, "token", None),
                context=getattr(data, "context", None),
            )
            level, content, card_payload = self._process_action_entry(
                operator_open_id=getattr(data, "open_id", None),
                action_value=action.value or {},
                form_value=action.form_value or {},
                source="card",
                callback_context=callback_context,
            )
            return self._toast_dict(level, content, card_payload)
        except ValueError as exc:
            return self._toast_dict("error", str(exc))
        except Exception:
            logger.exception("处理卡片回调失败")
            return self._toast_dict("error", "预约更新失败")
        finally:
            logger.debug("卡片回调处理耗时: {}ms source=card", int((mono_time.monotonic() - started_at) * 1000))

    def _send_card_to_user(self, *, user: UserProfile, target_date: date, allowed_meals: set[Meal]) -> None:
        defaults = user.meal_preferences & allowed_meals
        meal_prices: dict[Meal, Decimal] = {}

        if Meal.LUNCH in allowed_meals:
            meal_prices[Meal.LUNCH] = user.lunch_price
        if Meal.DINNER in allowed_meals:
            meal_prices[Meal.DINNER] = user.dinner_price

        selected, meal_record_ids = self._resolve_selected_from_records(
            target_date=target_date,
            open_id=user.open_id,
            allowed_meals=allowed_meals,
        )
        for meal in defaults:
            if meal_record_ids.get(meal) is None:
                selected.add(meal)

        for meal in selected:
            if meal_record_ids.get(meal) is not None:
                continue
            price = meal_prices.get(meal, Decimal("0"))
            record_id = self._repository.upsert_meal_record(
                target_date=target_date,
                open_id=user.open_id,
                meal=meal,
                price=price,
            )
            meal_record_ids[meal] = record_id

        card_json = self._card_builder.build(
            target_date=target_date,
            lunch_cutoff=self._config.schedule.lunch_cutoff,
            dinner_cutoff=self._config.schedule.dinner_cutoff,
            user_open_id=user.open_id,
            allowed_meals=allowed_meals,
            default_meals=defaults,
            selected_meals=selected,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
        )
        self._im.send_interactive(receive_id=user.open_id, card_json=card_json)

    def _process_action_entry(
        self,
        *,
        operator_open_id: str | None,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        source: str,
        callback_context: CardCallbackUpdateContext | None,
    ) -> tuple[str | None, str | None, dict[str, Any] | None]:
        action_name = str(action_value.get("action") or "")
        if callback_context is None or action_name not in {"toggle_meal", "refresh_state"}:
            return self._process_action(
                operator_open_id=operator_open_id,
                action_value=action_value,
                form_value=form_value,
                source=source,
            )
        if not operator_open_id:
            return ("error", "仅允许本人提交预约", None)

        target_open_id = str(action_value.get("target_open_id") or "")
        if operator_open_id != target_open_id:
            return ("error", "仅允许本人提交预约", None)

        target_date = _parse_iso_date(str(action_value.get("target_date") or ""))
        if target_date is None:
            return ("error", "日期参数无效", None)

        if not self._mark_user_processing(operator_open_id):
            return ("info", "后台处理中，请稍后", None)

        submitted_to_background = False
        try:
            if action_name == "toggle_meal":
                toggle = _parse_meal(action_value.get("toggle_meal"))
                if toggle is None:
                    return ("error", "不支持的餐次操作", None)
                allowed_meals = parse_meals(action_value.get("allowed_meals"))
                if toggle in allowed_meals and not self._is_editable(target_date=target_date, meal=toggle):
                    return ("error", f"{toggle.value} 已过截止时间，如有特殊情况请联系管理员人工处理", None)

            optimistic_card_payload = self._build_optimistic_card_payload(
                target_date=target_date,
                target_open_id=target_open_id,
                action_value=action_value,
                refresh_syncing=True,
            )
            if optimistic_card_payload is None:
                return self._process_action(
                    operator_open_id=operator_open_id,
                    action_value=action_value,
                    form_value=form_value,
                    source=source,
                )

            self._background_runner(
                lambda: self._run_action_in_background(
                    operator_open_id=operator_open_id,
                    target_date=target_date,
                    action_value=action_value,
                    form_value=form_value,
                    source=source,
                    callback_context=callback_context,
                    optimistic_card_payload=optimistic_card_payload,
                )
            )
            submitted_to_background = True
            return ("info", "处理中", optimistic_card_payload)
        finally:
            if not submitted_to_background:
                self._unmark_user_processing(operator_open_id)

    def _run_action_in_background(
        self,
        *,
        operator_open_id: str,
        target_date: date,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        source: str,
        callback_context: CardCallbackUpdateContext,
        optimistic_card_payload: dict[str, Any],
    ) -> None:
        action_name = str(action_value.get("action") or "")
        try:
            try:
                latest_payload = self._process_action_by_record_ids(
                    operator_open_id=operator_open_id,
                    target_date=target_date,
                    action_value=action_value,
                )
            except Exception:
                logger.exception(
                    "后台处理卡片回调失败: operator={} action={}",
                    operator_open_id,
                    action_name,
                )
                latest_payload = None

            fallback_payload = self._build_optimistic_card_payload(
                target_date=target_date,
                target_open_id=operator_open_id,
                action_value=action_value,
                refresh_syncing=False,
            )
            final_payload = latest_payload or fallback_payload or optimistic_card_payload
            self._push_async_card_update(
                callback_context=callback_context,
                card_payload=final_payload,
                operator_open_id=operator_open_id,
                target_date=target_date,
                toast_content="处理完成",
            )
        finally:
            self._unmark_user_processing(operator_open_id)

    def _process_action_by_record_ids(
        self,
        *,
        operator_open_id: str,
        target_date: date,
        action_value: dict[str, Any],
    ) -> dict[str, Any] | None:
        action_name = str(action_value.get("action") or "")
        allowed = parse_meals(action_value.get("allowed_meals"))
        if not allowed:
            return None
        defaults = parse_meals(action_value.get("default_meals")) & allowed
        meal_prices = self._parse_meal_prices(action_value=action_value, allowed_meals=allowed)
        selected_before = parse_meals(action_value.get("selected_meals")) & allowed
        selected = set(selected_before)
        meal_record_ids = self._parse_meal_record_ids(action_value=action_value, allowed_meals=allowed)

        if action_name == "toggle_meal":
            toggle = _parse_meal(action_value.get("toggle_meal"))
            if toggle is not None and toggle in allowed:
                if toggle in selected:
                    selected.remove(toggle)
                else:
                    selected.add(toggle)
            changed_meals = {meal for meal in allowed if (meal in selected_before) != (meal in selected)}
            meal_record_ids = self._apply_selection(
                target_date=target_date,
                operator_open_id=operator_open_id,
                changed_meals=changed_meals,
                selected=selected,
                meal_prices=meal_prices,
                meal_record_ids=meal_record_ids,
            )
        elif action_name != "refresh_state":
            return None

        record_ids = [record_id for record_id in meal_record_ids.values() if record_id]
        if record_ids:
            rows = self._repository.list_user_meal_rows_by_record_ids(
                target_date=target_date,
                open_id=operator_open_id,
                record_ids=record_ids,
            )
            if isinstance(rows, list) and rows:
                selected, resolved_ids = self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed)
                meal_record_ids = {
                    meal: resolved_ids.get(meal) or meal_record_ids.get(meal) for meal in allowed
                }

        return self._card_builder.build_payload(
            target_date=target_date,
            lunch_cutoff=self._config.schedule.lunch_cutoff,
            dinner_cutoff=self._config.schedule.dinner_cutoff,
            user_open_id=operator_open_id,
            allowed_meals=allowed,
            default_meals=defaults,
            selected_meals=selected,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
        )

    def _push_async_card_update(
        self,
        *,
        callback_context: CardCallbackUpdateContext,
        card_payload: dict[str, Any],
        operator_open_id: str,
        target_date: date,
        toast_content: str | None = None,
    ) -> None:
        if callback_context.token:
            try:
                self._im.delay_update_card(
                    token=callback_context.token,
                    card_payload=card_payload,
                    toast_content=toast_content,
                )
                logger.info(
                    "异步卡片刷新成功: mode=callback_token operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )
                return
            except FeishuApiError as exc:
                if "code=10002" in str(exc):
                    logger.warning(
                        "异步卡片刷新回退: mode=callback_token operator={} date={} reason=code_10002",
                        operator_open_id,
                        target_date.isoformat(),
                    )
                else:
                    logger.exception(
                        "异步卡片刷新失败: mode=callback_token operator={} date={}",
                        operator_open_id,
                        target_date.isoformat(),
                    )
            except Exception:
                logger.exception(
                    "异步卡片刷新失败: mode=callback_token operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )

        if callback_context.open_message_id:
            try:
                self._im.patch_interactive(message_id=callback_context.open_message_id, card_payload=card_payload)
                logger.info(
                    "异步卡片刷新成功: mode=open_message_id operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )
                return
            except Exception:
                logger.exception(
                    "异步卡片刷新失败: mode=open_message_id operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )

        logger.warning(
            "异步卡片刷新跳过: operator={} date={} reason=no_available_context",
            operator_open_id,
            target_date.isoformat(),
        )

    def _build_optimistic_card_payload(
        self,
        *,
        target_date: date,
        target_open_id: str,
        action_value: dict[str, Any],
        refresh_syncing: bool = False,
    ) -> dict[str, Any] | None:
        action_name = str(action_value.get("action") or "")
        if action_name not in {"toggle_meal", "refresh_state"}:
            return None

        allowed = parse_meals(action_value.get("allowed_meals"))
        if not allowed:
            return None
        defaults = parse_meals(action_value.get("default_meals")) & allowed
        selected = parse_meals(action_value.get("selected_meals")) & allowed
        if action_name == "toggle_meal":
            toggle_meal = _parse_meal(action_value.get("toggle_meal"))
            if toggle_meal and toggle_meal in allowed:
                if toggle_meal in selected:
                    selected.remove(toggle_meal)
                else:
                    selected.add(toggle_meal)

        meal_prices = self._parse_meal_prices(action_value=action_value, allowed_meals=allowed)
        meal_record_ids = self._parse_meal_record_ids(action_value=action_value, allowed_meals=allowed)
        return self._card_builder.build_payload(
            target_date=target_date,
            lunch_cutoff=self._config.schedule.lunch_cutoff,
            dinner_cutoff=self._config.schedule.dinner_cutoff,
            user_open_id=target_open_id,
            allowed_meals=allowed,
            default_meals=defaults,
            selected_meals=selected,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
            refresh_syncing=refresh_syncing,
        )

    @staticmethod
    def _parse_meal_prices(*, action_value: dict[str, Any], allowed_meals: set[Meal]) -> dict[Meal, Decimal]:
        raw_prices = action_value.get("meal_prices")
        if not isinstance(raw_prices, dict):
            return {meal: Decimal("0") for meal in allowed_meals}
        result: dict[Meal, Decimal] = {}
        for meal in allowed_meals:
            raw = raw_prices.get(meal.value)
            try:
                result[meal] = Decimal(str(raw))
            except Exception:
                result[meal] = Decimal("0")
        return result

    @staticmethod
    def _parse_meal_record_ids(*, action_value: dict[str, Any], allowed_meals: set[Meal]) -> dict[Meal, str | None]:
        raw_ids = action_value.get("meal_record_ids")
        if not isinstance(raw_ids, dict):
            return {meal: None for meal in allowed_meals}
        result: dict[Meal, str | None] = {}
        for meal in allowed_meals:
            raw = raw_ids.get(meal.value)
            if raw is None or raw == "":
                result[meal] = None
                continue
            result[meal] = str(raw)
        return result

    def _default_background_runner(self, task: Callable[[], None]) -> None:
        self._card_action_executor.submit(task)

    def _mark_user_processing(self, open_id: str) -> bool:
        with self._processing_users_lock:
            if open_id in self._processing_users:
                return False
            self._processing_users.add(open_id)
            return True

    def _unmark_user_processing(self, open_id: str) -> None:
        with self._processing_users_lock:
            self._processing_users.discard(open_id)

    @staticmethod
    def _extract_callback_update_context(*, token: Any, context: Any) -> CardCallbackUpdateContext | None:
        token_value = str(token or "").strip() or None
        open_message_id_value = str(getattr(context, "open_message_id", "") or "").strip() or None
        if token_value is None and open_message_id_value is None:
            return None
        return CardCallbackUpdateContext(
            token=token_value,
            open_message_id=open_message_id_value,
        )

    def _process_action(
        self,
        *,
        operator_open_id: str | None,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        source: str,
        enforce_cutoff: bool = True,
    ) -> tuple[str, str, dict[str, Any] | None]:
        action_name = str(action_value.get("action") or "")
        perf_total_started = mono_time.monotonic()
        perf_last_started = perf_total_started
        phase_cost: dict[str, int] = {}

        def _mark(phase: str) -> None:
            nonlocal perf_last_started
            now = mono_time.monotonic()
            phase_cost[phase] = int((now - perf_last_started) * 1000)
            perf_last_started = now

        try:
            logger.info(
                "收到卡片回调: source={} operator={} action={}",
                source,
                operator_open_id or "",
                action_name,
            )

            if not operator_open_id:
                return ("error", "仅允许本人提交预约", None)

            target_open_id = str(action_value.get("target_open_id") or "")
            if operator_open_id != target_open_id:
                return ("error", "仅允许本人提交预约", None)

            target_date = _parse_iso_date(str(action_value.get("target_date") or ""))
            if target_date is None:
                return ("error", "日期参数无效", None)

            user = self._load_user(operator_open_id)
            if user is None:
                return ("error", "你不在用餐人员配置中，无法发起预约。", None)

            allowed = self._allowed_meals_for_date(target_date)
            defaults = user.meal_preferences & allowed
            meal_prices = self._build_meal_prices(user=user, allowed_meals=allowed)

            rows = self._repository.list_user_meal_rows(target_date=target_date, open_id=operator_open_id)
            rows = self._sync_disallowed_meal_rows(
                target_date=target_date,
                open_id=operator_open_id,
                allowed_meals=allowed,
                rows=rows,
            )
            selected_before, meal_record_ids = self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed)
            selected = set(selected_before)

            if action_name == "toggle_meal":
                toggle = _parse_meal(action_value.get("toggle_meal"))
                if toggle is None:
                    return ("error", "不支持的餐次操作", None)
                if toggle not in allowed:
                    _mark("parse_and_validate")
                    _mark("apply_selection")
                    card_payload = self._card_builder.build_payload(
                        target_date=target_date,
                        lunch_cutoff=self._config.schedule.lunch_cutoff,
                        dinner_cutoff=self._config.schedule.dinner_cutoff,
                        user_open_id=operator_open_id,
                        allowed_meals=allowed,
                        default_meals=defaults,
                        selected_meals=selected,
                        meal_prices=meal_prices,
                        meal_record_ids=meal_record_ids,
                    )
                    _mark("build_card")
                    return ("info", f"{toggle.value} 当前不可预约，已同步最新状态", card_payload)
                if toggle in selected:
                    selected.remove(toggle)
                else:
                    selected.add(toggle)
            elif action_name == "submit_reservation":
                form_selected = parse_meals(form_value.get("meals"))
                if form_selected:
                    selected = form_selected & allowed
            elif action_name == "refresh_state":
                _mark("parse_and_validate")
                _mark("apply_selection")

                card_payload = self._card_builder.build_payload(
                    target_date=target_date,
                    lunch_cutoff=self._config.schedule.lunch_cutoff,
                    dinner_cutoff=self._config.schedule.dinner_cutoff,
                    user_open_id=operator_open_id,
                    allowed_meals=allowed,
                    default_meals=defaults,
                    selected_meals=selected,
                    meal_prices=meal_prices,
                    meal_record_ids=meal_record_ids,
                )
                _mark("build_card")
                return ("info", "已刷新最新预约状态", card_payload)
            else:
                return ("error", "不支持的卡片操作", None)

            selected &= allowed
            changed_meals = {meal for meal in allowed if (meal in selected_before) != (meal in selected)}
            if enforce_cutoff:
                blocked_meal = next(
                    (meal for meal in changed_meals if not self._is_editable(target_date=target_date, meal=meal)),
                    None,
                )
                if blocked_meal is not None:
                    return ("error", f"{blocked_meal.value} 已过截止时间，如有特殊情况请联系管理员人工处理", None)
            _mark("parse_and_validate")

            updated_record_ids = self._apply_selection(
                target_date=target_date,
                operator_open_id=operator_open_id,
                changed_meals=changed_meals,
                selected=selected,
                meal_prices=meal_prices,
                meal_record_ids=meal_record_ids,
            )
            _mark("apply_selection")

            card_payload = self._card_builder.build_payload(
                target_date=target_date,
                lunch_cutoff=self._config.schedule.lunch_cutoff,
                dinner_cutoff=self._config.schedule.dinner_cutoff,
                user_open_id=operator_open_id,
                allowed_meals=allowed,
                default_meals=defaults,
                selected_meals=selected,
                meal_prices=meal_prices,
                meal_record_ids=updated_record_ids,
            )
            _mark("build_card")
            return ("info", "预约已更新", card_payload)
        finally:
            total_cost = int((mono_time.monotonic() - perf_total_started) * 1000)
            logger.debug(
                "卡片回调分段耗时: source={} action={} parse={}ms apply={}ms build={}ms total={}ms",
                source,
                action_name,
                phase_cost.get("parse_and_validate", 0),
                phase_cost.get("apply_selection", 0),
                phase_cost.get("build_card", 0),
                total_cost,
            )

    def _apply_selection(
        self,
        *,
        target_date: date,
        operator_open_id: str,
        changed_meals: set[Meal],
        selected: set[Meal],
        meal_prices: dict[Meal, Decimal],
        meal_record_ids: dict[Meal, str | None],
    ) -> dict[Meal, str | None]:
        started_at = mono_time.monotonic()
        updated_record_ids = dict(meal_record_ids)

        write_started = mono_time.monotonic()
        upsert_count = 0
        cancel_count = 0
        for meal in changed_meals:
            record_id = updated_record_ids.get(meal)
            if meal in selected:
                price = meal_prices.get(meal)
                if price is None:
                    raise ValueError(f"{meal.value} 单价缺失")
                has_record_id = bool(record_id)
                op_started = mono_time.monotonic()
                record_id = self._repository.upsert_meal_record(
                    target_date=target_date,
                    open_id=operator_open_id,
                    meal=meal,
                    price=price,
                    record_id=record_id,
                    prefer_direct=True,
                )
                upsert_count += 1
                updated_record_ids[meal] = record_id
                logger.debug(
                    "预约写入耗时: op=upsert meal={} date={} direct={} cost={}ms",
                    meal.value,
                    target_date.isoformat(),
                    has_record_id,
                    int((mono_time.monotonic() - op_started) * 1000),
                )
            else:
                op_started = mono_time.monotonic()
                kept_id = self._repository.cancel_meal_record(
                    target_date=target_date,
                    open_id=operator_open_id,
                    meal=meal,
                    record_id=record_id,
                    prefer_direct=True,
                )
                cancel_count += 1
                if kept_id is not None:
                    updated_record_ids[meal] = kept_id
                logger.debug(
                    "预约写入耗时: op=cancel meal={} date={} has_record={} cost={}ms",
                    meal.value,
                    target_date.isoformat(),
                    bool(record_id),
                    int((mono_time.monotonic() - op_started) * 1000),
                )

        write_cost = int((mono_time.monotonic() - write_started) * 1000)
        total_cost = int((mono_time.monotonic() - started_at) * 1000)
        logger.debug(
            "预约写入分段耗时: date={} open_id={} changed={} cutoff={}ms write={}ms upsert={} cancel={} total={}ms",
            target_date.isoformat(),
            operator_open_id,
            len(changed_meals),
            0,
            write_cost,
            upsert_count,
            cancel_count,
            total_cost,
        )
        return updated_record_ids

    def _resolve_selected_from_records(
        self,
        *,
        target_date: date,
        open_id: str,
        allowed_meals: set[Meal],
    ) -> tuple[set[Meal], dict[Meal, str | None]]:
        rows = self._repository.list_user_meal_rows(target_date=target_date, open_id=open_id)
        return self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed_meals)

    @staticmethod
    def _resolve_selected_from_rows(
        *,
        rows: list[Any],
        allowed_meals: set[Meal],
    ) -> tuple[set[Meal], dict[Meal, str | None]]:
        row_by_meal = _pick_rows_by_meal(rows=rows, allowed_meals=allowed_meals)
        selected: set[Meal] = set()
        meal_record_ids: dict[Meal, str | None] = {meal: None for meal in allowed_meals}
        for meal in allowed_meals:
            row = row_by_meal.get(meal)
            if row is None:
                continue
            meal_record_ids[meal] = row.record_id
            if row.reservation_status:
                selected.add(meal)
        return selected, meal_record_ids

    def _list_schedule_rules(self, *, force_refresh: bool = False) -> list[MealScheduleRule]:
        rules = self._repository.list_schedule_rules()
        logger.debug(
            "用餐定时配置已实时拉取: force={} rules={}",
            force_refresh,
            len(rules),
        )
        return list(rules)

    def _allowed_meals_for_date(self, target_date: date) -> set[Meal]:
        rules = self._list_schedule_rules()
        plan = self._decider.decide(target_date, rules)
        return set(plan.meals)

    @staticmethod
    def _build_meal_prices(*, user: UserProfile, allowed_meals: set[Meal]) -> dict[Meal, Decimal]:
        prices: dict[Meal, Decimal] = {}
        if Meal.LUNCH in allowed_meals:
            prices[Meal.LUNCH] = user.lunch_price
        if Meal.DINNER in allowed_meals:
            prices[Meal.DINNER] = user.dinner_price
        return prices

    def _sync_disallowed_meal_rows(
        self,
        *,
        target_date: date,
        open_id: str,
        allowed_meals: set[Meal],
        rows: list[Any],
    ) -> list[Any]:
        disallowed_meals = self._ALL_MEALS - allowed_meals
        if not disallowed_meals:
            return rows

        disallowed_rows = _pick_rows_by_meal(rows=rows, allowed_meals=disallowed_meals)
        changed_meals: set[Meal] = set()
        for meal in disallowed_meals:
            row = disallowed_rows.get(meal)
            if row is None or not bool(getattr(row, "reservation_status", False)):
                continue
            self._repository.cancel_meal_record(
                target_date=target_date,
                open_id=open_id,
                meal=meal,
                record_id=row.record_id,
                prefer_direct=True,
            )
            changed_meals.add(meal)

        if not changed_meals:
            return rows

        logger.info(
            "根据用餐定时配置自动取消不可预约餐次: date={} open_id={} meals={}",
            target_date.isoformat(),
            open_id,
            _format_meals(changed_meals),
        )
        return self._repository.list_user_meal_rows(target_date=target_date, open_id=open_id)

    def _load_user(self, open_id: str) -> UserProfile | None:
        users = self._repository.list_user_profiles()
        return next((user for user in users if user.open_id == open_id), None)

    def _is_editable(self, *, target_date: date, meal: Meal) -> bool:
        now = self._now()
        today = now.date()

        if target_date > today:
            return True
        if target_date < today:
            return False

        if meal == Meal.LUNCH:
            return now.time() < self._config.schedule.lunch_cutoff_obj
        if meal == Meal.DINNER:
            return now.time() < self._config.schedule.dinner_cutoff_obj
        return False

    def _build_meal_fee_archive_window(self, target_date: date) -> MealFeeArchiveWindow:
        day_of_month = self._config.schedule.fee_archive_day_of_month
        run_date = _resolve_monthly_day(
            year=target_date.year,
            month=target_date.month,
            day_of_month=day_of_month,
        )
        if target_date.month == 1:
            prev_year = target_date.year - 1
            prev_month = 12
        else:
            prev_year = target_date.year
            prev_month = target_date.month - 1
        prev_run_date = _resolve_monthly_day(
            year=prev_year,
            month=prev_month,
            day_of_month=day_of_month,
        )
        return MealFeeArchiveWindow(
            run_date=run_date,
            start_date=prev_run_date + timedelta(days=1),
            end_date=run_date,
        )

    def _now(self) -> datetime:
        if self._now_provider is None:
            return datetime.now(self._timezone)

        now = self._now_provider()
        if now.tzinfo is None:
            return now.replace(tzinfo=self._timezone)
        return now.astimezone(self._timezone)

    @staticmethod
    def _toast(
        level: str | None,
        content: str | None,
        card_payload: dict[str, Any] | None = None,
    ) -> P2CardActionTriggerResponse:
        result: dict[str, Any] = {}
        if level and content:
            result["toast"] = {"type": level, "content": content}
        if card_payload is not None:
            result["card"] = {"type": "raw", "data": card_payload}
        return P2CardActionTriggerResponse(result)

    @staticmethod
    def _toast_dict(
        level: str | None,
        content: str | None,
        card_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if level and content:
            result["toast"] = {"type": level, "content": content}
        if card_payload is not None:
            result["card"] = {"type": "raw", "data": card_payload}
        return result


def _extract_text_from_message_content(content: str | None) -> str:
    if not content:
        return ""
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return ""
    raw = payload.get("text")
    if raw is None:
        return ""
    return str(raw).strip()


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_meal(value: object) -> Meal | None:
    if value == Meal.LUNCH.value:
        return Meal.LUNCH
    if value == Meal.DINNER.value:
        return Meal.DINNER
    return None


def _pick_rows_by_meal(rows: list[Any], allowed_meals: set[Meal]) -> dict[Meal, Any]:
    selected: dict[Meal, Any] = {}
    for row in rows:
        meal = getattr(row, "meal_type", None)
        if meal not in allowed_meals:
            continue
        selected[meal] = row
    return selected


def _weekday_text(target_date: date) -> str:
    weekdays = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
    return weekdays[target_date.weekday()]


def _format_meals(meals: set[Meal]) -> str:
    ordered: list[Meal] = []
    if Meal.LUNCH in meals:
        ordered.append(Meal.LUNCH)
    if Meal.DINNER in meals:
        ordered.append(Meal.DINNER)
    if not ordered:
        return "-"
    return "、".join(meal.value for meal in ordered)


def _resolve_monthly_day(*, year: int, month: int, day_of_month: int) -> date:
    last_day = monthrange(year, month)[1]
    return date(year, month, min(day_of_month, last_day))


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if not text:
        return "0"
    return text
