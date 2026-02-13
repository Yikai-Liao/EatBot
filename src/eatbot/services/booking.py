from __future__ import annotations

from datetime import date, datetime
import json
import logging
from typing import Any
from zoneinfo import ZoneInfo

from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
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
    ) -> None:
        self._config = config
        self._repository = repository
        self._im = im
        self._card_builder = ReservationCardBuilder()
        self._decider = MealPlanDecider()
        self._timezone = ZoneInfo(config.schedule.timezone)

    def send_daily_cards(self, target_date: date | None = None) -> None:
        target = target_date or datetime.now(self._timezone).date()
        rules = self._repository.list_schedule_rules()
        plan = self._decider.decide(target, rules)
        if not plan.meals:
            logger.info("今天不发送订餐卡片: date=%s", target.isoformat())
            return

        users = [user for user in self._repository.list_user_profiles() if user.enabled]
        for user in users:
            self._send_card_to_user(user=user, target_date=target, allowed_meals=plan.meals)

    def send_card_to_user_today(self, open_id: str) -> None:
        today = datetime.now(self._timezone).date()
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
        try:
            event = data.event
            if not event or not event.action:
                return self._toast("error", "卡片参数缺失")

            action_value = event.action.value or {}
            if action_value.get("action") != "submit_reservation":
                return self._toast("error", "不支持的卡片操作")

            operator_open_id = event.operator.open_id if event.operator else None
            target_open_id = str(action_value.get("target_open_id") or "")
            if not operator_open_id or operator_open_id != target_open_id:
                return self._toast("error", "仅允许本人提交预约")

            target_date = _parse_iso_date(str(action_value.get("target_date") or ""))
            if target_date is None:
                return self._toast("error", "日期参数无效")

            allowed = parse_meals(action_value.get("allowed_meals", []))
            if not allowed:
                return self._toast("error", "无可预约餐次")

            selected_raw = event.action.form_value or {}
            selected = parse_meals(selected_raw.get("meals"))
            selected &= allowed

            user = self._load_user(operator_open_id)
            if user is None:
                return self._toast("error", "未找到人员配置")

            for meal in allowed:
                if not self._is_editable(target_date=target_date, meal=meal):
                    return self._toast("error", f"{meal.value} 已过截止时间")

            for meal in allowed:
                if meal in selected:
                    price = user.lunch_price if meal == Meal.LUNCH else user.dinner_price
                    self._repository.upsert_meal_record(
                        target_date=target_date,
                        open_id=operator_open_id,
                        meal=meal,
                        price=price,
                    )
                else:
                    self._repository.cancel_meal_record(
                        target_date=target_date,
                        open_id=operator_open_id,
                        meal=meal,
                    )

            return self._toast("info", "预约已更新")
        except Exception as exc:
            logger.exception("处理卡片回调失败: %s", exc)
            return self._toast("error", "预约更新失败")

    def _send_card_to_user(self, *, user: UserProfile, target_date: date, allowed_meals: set[Meal]) -> None:
        defaults = user.meal_preferences & allowed_meals

        for meal in defaults:
            price = user.lunch_price if meal == Meal.LUNCH else user.dinner_price
            self._repository.upsert_meal_record(
                target_date=target_date,
                open_id=user.open_id,
                meal=meal,
                price=price,
            )

        card_json = self._card_builder.build(
            target_date=target_date,
            user_open_id=user.open_id,
            allowed_meals=allowed_meals,
            default_meals=defaults,
        )
        self._im.send_interactive(receive_id=user.open_id, card_json=card_json)

    def _load_user(self, open_id: str) -> UserProfile | None:
        users = self._repository.list_user_profiles()
        for user in users:
            if user.open_id == open_id:
                return user
        return None

    def _is_editable(self, *, target_date: date, meal: Meal) -> bool:
        now = datetime.now(self._timezone)
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

    @staticmethod
    def _toast(level: str, content: str) -> P2CardActionTriggerResponse:
        return P2CardActionTriggerResponse({"toast": {"type": level, "content": content}})


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
