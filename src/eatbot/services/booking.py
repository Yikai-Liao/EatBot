from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import logging
import time as mono_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from lark_oapi.card.model import Card
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from eatbot.config import RuntimeConfig
from eatbot.domain.cards import ReservationCardBuilder
from eatbot.domain.decision import MealPlanDecider, parse_meals
from eatbot.domain.models import Meal, UserProfile
from eatbot.services.repositories import BitableRepository
from eatbot.adapters.feishu_clients import IMAdapter


logger = logging.getLogger(__name__)


class BookingService:
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
        self._timezone = ZoneInfo(config.schedule.timezone)
        self._now_provider = now_provider

    def send_daily_cards(self, target_date: date | None = None) -> None:
        target = target_date or self._now().date()
        rules = self._repository.list_schedule_rules()
        plan = self._decider.decide(target, rules)
        if not plan.meals:
            logger.info("今天不发送订餐卡片: date=%s", target.isoformat())
            return

        users = [user for user in self._repository.list_user_profiles() if user.enabled]
        for user in users:
            try:
                self._send_card_to_user(user=user, target_date=target, allowed_meals=plan.meals)
            except Exception as exc:
                logger.exception("给用户发卡失败, user=%s, open_id=%s, err=%s", user.display_name, user.open_id, exc)

    def send_card_to_user_today(self, open_id: str) -> None:
        today = self._now().date()
        user = self._load_user(open_id)
        if user is None:
            self._im.send_text(open_id, "你不在用餐人员配置中，无法发起预约。")
            return

        rules = self._repository.list_schedule_rules()
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

        text = f"{target_date.isoformat()} {meal.value} 预约人数: {count}"
        for open_id in receivers:
            self._im.send_text(open_id, text)

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
        except Exception as exc:
            logger.exception("处理卡片回调失败: %s", exc)
            return self._toast("error", "预约更新失败")
        finally:
            logger.info("卡片回调处理耗时: %dms source=event", int((mono_time.monotonic() - started_at) * 1000))

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
        except Exception as exc:
            logger.exception("处理卡片回调失败: %s", exc)
            return self._toast_dict("error", "预约更新失败")
        finally:
            logger.info("卡片回调处理耗时: %dms source=card", int((mono_time.monotonic() - started_at) * 1000))

    def _send_card_to_user(self, *, user: UserProfile, target_date: date, allowed_meals: set[Meal]) -> None:
        defaults = user.meal_preferences & allowed_meals
        meal_prices: dict[Meal, Decimal] = {}
        meal_record_ids: dict[Meal, str | None] = {meal: None for meal in allowed_meals}

        if Meal.LUNCH in allowed_meals:
            meal_prices[Meal.LUNCH] = user.lunch_price
        if Meal.DINNER in allowed_meals:
            meal_prices[Meal.DINNER] = user.dinner_price

        for meal in defaults:
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
            user_open_id=user.open_id,
            allowed_meals=allowed_meals,
            default_meals=defaults,
            selected_meals=defaults,
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
                "收到卡片回调: source=%s operator=%s action=%s",
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

            allowed = parse_meals(action_value.get("allowed_meals", []))
            if not allowed:
                return ("error", "无可预约餐次", None)

            defaults = parse_meals(action_value.get("default_meals", [])) & allowed
            selected = parse_meals(action_value.get("selected_meals", []))
            if action_name == "toggle_meal":
                toggle = _parse_meal(action_value.get("toggle_meal"))
                if toggle is None or toggle not in allowed:
                    return ("error", "不支持的餐次操作", None)
                if toggle in selected:
                    selected.remove(toggle)
                else:
                    selected.add(toggle)
            elif action_name == "submit_reservation":
                if not selected:
                    selected = parse_meals(form_value.get("meals"))
            else:
                return ("error", "不支持的卡片操作", None)

            selected &= allowed
            meal_prices = _parse_meal_price_map(action_value.get("meal_prices"), allowed)
            meal_record_ids = _parse_meal_record_id_map(action_value.get("meal_record_ids"), allowed)
            _mark("parse_and_validate")

            updated_record_ids = self._apply_selection(
                target_date=target_date,
                operator_open_id=operator_open_id,
                allowed=allowed,
                selected=selected,
                meal_prices=meal_prices,
                meal_record_ids=meal_record_ids,
            )
            _mark("apply_selection")

            card_payload = self._card_builder.build_payload(
                target_date=target_date,
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
            logger.info(
                "卡片回调分段耗时: source=%s action=%s parse=%dms apply=%dms build=%dms total=%dms",
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
        allowed: set[Meal],
        selected: set[Meal],
        meal_prices: dict[Meal, Decimal],
        meal_record_ids: dict[Meal, str | None],
    ) -> dict[Meal, str | None]:
        started_at = mono_time.monotonic()
        updated_record_ids = dict(meal_record_ids)
        cutoff_started = mono_time.monotonic()
        for meal in allowed:
            if not self._is_editable(target_date=target_date, meal=meal):
                raise ValueError(f"{meal.value} 已过截止时间")
        cutoff_cost = int((mono_time.monotonic() - cutoff_started) * 1000)

        write_started = mono_time.monotonic()
        upsert_count = 0
        cancel_count = 0
        for meal in allowed:
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
                logger.info(
                    "预约写入耗时: op=upsert meal=%s date=%s direct=%s cost=%dms",
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
                logger.info(
                    "预约写入耗时: op=cancel meal=%s date=%s has_record=%s cost=%dms",
                    meal.value,
                    target_date.isoformat(),
                    bool(record_id),
                    int((mono_time.monotonic() - op_started) * 1000),
                )

        write_cost = int((mono_time.monotonic() - write_started) * 1000)
        total_cost = int((mono_time.monotonic() - started_at) * 1000)
        logger.info(
            "预约写入分段耗时: date=%s open_id=%s cutoff=%dms write=%dms upsert=%d cancel=%d total=%dms",
            target_date.isoformat(),
            operator_open_id,
            cutoff_cost,
            write_cost,
            upsert_count,
            cancel_count,
            total_cost,
        )
        return updated_record_ids

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


def _parse_meal_price_map(value: object, allowed: set[Meal]) -> dict[Meal, Decimal]:
    result: dict[Meal, Decimal] = {}
    if not isinstance(value, dict):
        return result
    for meal in allowed:
        raw = value.get(meal.value)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            result[meal] = Decimal(str(raw))
        except Exception:
            continue
    return result


def _parse_meal_record_id_map(value: object, allowed: set[Meal]) -> dict[Meal, str | None]:
    result: dict[Meal, str | None] = {}
    if not isinstance(value, dict):
        return result
    for meal in allowed:
        raw = value.get(meal.value)
        if raw is None:
            result[meal] = None
            continue
        text = str(raw).strip()
        result[meal] = text or None
    return result
