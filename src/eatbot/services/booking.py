from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import json
import time as mono_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from loguru import logger
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
from eatbot.adapters.feishu_clients import IMAdapter


@dataclass(slots=True, frozen=True)
class CronPreviewSnapshot:
    schedule_rules_count: int
    enabled_user_count: int
    stats_receiver_count: int
    rules_by_date: dict[date, set[Meal]]
    matched_rule_count_by_date: dict[date, int]


class BookingService:
    _ALL_MEALS = {Meal.LUNCH, Meal.DINNER}

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        repository: BitableRepository,
        im: IMAdapter,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._im = im
        self._card_builder = ReservationCardBuilder()
        self._decider = MealPlanDecider()
        self._timezone = ZoneInfo(config.timezone)
        self._now_provider = now_provider
        self._schedule_rules_cache: list[MealScheduleRule] | None = None
        self._schedule_rules_cache_expires_at: datetime | None = None

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
        if text in {"订餐", "/eatbot today"}:
            self.send_card_to_user_today(sender_open_id)

    def handle_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        started_at = mono_time.monotonic()
        try:
            event = data.event
            if not event or not event.action:
                return self._toast("error", "卡片参数缺失")

            level, content, card_payload = self._process_action(
                operator_open_id=event.operator.open_id if event.operator else None,
                action_value=event.action.value or {},
                form_value=event.action.form_value or {},
                source="event",
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

            level, content, card_payload = self._process_action(
                operator_open_id=getattr(data, "open_id", None),
                action_value=action.value or {},
                form_value=action.form_value or {},
                source="card",
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

    def _process_action(
        self,
        *,
        operator_open_id: str | None,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        source: str,
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
        cutoff_started = mono_time.monotonic()
        for meal in changed_meals:
            if not self._is_editable(target_date=target_date, meal=meal):
                raise ValueError(f"{meal.value} 已过截止时间，如有特殊情况请联系管理员人工处理")
        cutoff_cost = int((mono_time.monotonic() - cutoff_started) * 1000)

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
            cutoff_cost,
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
        now = self._now()
        cache_available = (
            self._schedule_rules_cache is not None
            and self._schedule_rules_cache_expires_at is not None
            and now < self._schedule_rules_cache_expires_at
        )
        if force_refresh or not cache_available:
            rules = self._repository.list_schedule_rules()
            self._schedule_rules_cache = rules
            self._schedule_rules_cache_expires_at = now + self._config.schedule.schedule_cache_ttl_obj
            logger.debug(
                "用餐定时配置缓存已刷新: force={} rules={} ttl_minutes={} expires_at={}",
                force_refresh,
                len(rules),
                self._config.schedule.schedule_cache_ttl_minutes,
                self._schedule_rules_cache_expires_at.isoformat(),
            )
        return list(self._schedule_rules_cache or [])

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

    def _now(self) -> datetime:
        if self._now_provider is None:
            return datetime.now(self._timezone)

        now = self._now_provider()
        if now.tzinfo is None:
            return now.replace(tzinfo=self._timezone)
        return now.astimezone(self._timezone)

    @staticmethod
    def _toast(
        level: str,
        content: str,
        card_payload: dict[str, Any] | None = None,
    ) -> P2CardActionTriggerResponse:
        result: dict[str, Any] = {"toast": {"type": level, "content": content}}
        if card_payload is not None:
            result["card"] = {"type": "raw", "data": card_payload}
        return P2CardActionTriggerResponse(result)

    @staticmethod
    def _toast_dict(
        level: str,
        content: str,
        card_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"toast": {"type": level, "content": content}}
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
