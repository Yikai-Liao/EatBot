from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from typing import Any

from .models import Meal


class ReservationCardBuilder:
    _MEAL_ORDER = {Meal.LUNCH: 0, Meal.DINNER: 1}

    def build(
        self,
        *,
        target_date: date,
        lunch_cutoff: str,
        dinner_cutoff: str,
        user_open_id: str,
        allowed_meals: set[Meal],
        default_meals: set[Meal],
        selected_meals: set[Meal],
        meal_prices: dict[Meal, Decimal],
        meal_record_ids: dict[Meal, str | None],
        refresh_syncing: bool = False,
    ) -> str:
        card = self.build_payload(
            target_date=target_date,
            lunch_cutoff=lunch_cutoff,
            dinner_cutoff=dinner_cutoff,
            user_open_id=user_open_id,
            allowed_meals=allowed_meals,
            default_meals=default_meals,
            selected_meals=selected_meals,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
            refresh_syncing=refresh_syncing,
        )
        return json.dumps(card, ensure_ascii=False)

    def build_payload(
        self,
        *,
        target_date: date,
        lunch_cutoff: str,
        dinner_cutoff: str,
        user_open_id: str,
        allowed_meals: set[Meal],
        default_meals: set[Meal],
        selected_meals: set[Meal],
        meal_prices: dict[Meal, Decimal],
        meal_record_ids: dict[Meal, str | None],
        refresh_syncing: bool = False,
    ) -> dict[str, Any]:
        allowed_sorted = self._sorted_meals(allowed_meals)
        selected = selected_meals & allowed_meals

        buttons = _build_toggle_buttons(
            target_date=target_date,
            user_open_id=user_open_id,
            allowed_meals=allowed_sorted,
            selected_meals=self._sorted_meals(selected),
            default_meals=self._sorted_meals(default_meals & allowed_meals),
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
            refresh_syncing=refresh_syncing,
        )

        return {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"食堂预约 {target_date.isoformat()} {_weekday_text(target_date)}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            "点击按钮切换预约状态\n"
                            f"预约截止时间为：午餐{lunch_cutoff}，晚餐{dinner_cutoff}"
                        ),
                    },
                    *buttons,
                ],
            },
        }

    def _sorted_meals(self, meals: set[Meal]) -> list[Meal]:
        return sorted(meals, key=lambda item: self._MEAL_ORDER.get(item, 999))


class LeaveCardBuilder:
    def build(
        self,
        *,
        user_open_id: str,
        target_date: date,
        selected_start_date: date | None = None,
        selected_end_date: date | None = None,
        readonly: bool = False,
        submitted: bool = False,
        submit_syncing: bool = False,
    ) -> str:
        return json.dumps(
            self.build_payload(
                user_open_id=user_open_id,
                target_date=target_date,
                selected_start_date=selected_start_date,
                selected_end_date=selected_end_date,
                readonly=readonly,
                submitted=submitted,
                submit_syncing=submit_syncing,
            ),
            ensure_ascii=False,
        )

    def build_payload(
        self,
        *,
        user_open_id: str,
        target_date: date,
        selected_start_date: date | None = None,
        selected_end_date: date | None = None,
        readonly: bool = False,
        submitted: bool = False,
        submit_syncing: bool = False,
    ) -> dict[str, Any]:
        readonly = readonly or submit_syncing or submitted
        start_date = selected_start_date or target_date
        end_date = selected_end_date or target_date
        submit_action_value = {
            "action": "submit_leave_range",
            "target_open_id": user_open_id,
            "target_date": target_date.isoformat(),
            "selected_start_date": start_date.isoformat(),
            "selected_end_date": end_date.isoformat(),
        }
        start_picker_value = {
            "action": "select_leave_start",
            "target_open_id": user_open_id,
            "target_date": target_date.isoformat(),
            "selected_start_date": start_date.isoformat(),
            "selected_end_date": end_date.isoformat(),
        }
        end_picker_value = {
            "action": "select_leave_end",
            "target_open_id": user_open_id,
            "target_date": target_date.isoformat(),
            "selected_start_date": start_date.isoformat(),
            "selected_end_date": end_date.isoformat(),
        }

        elements: list[dict[str, Any]] = [
            {
                "tag": "markdown",
                "content": "选择暂停自动预约的开始日期和结束日期。你仍可在当日卡片中手动点击恢复预约。",
            },
        ]

        if readonly:
            elements.extend(
                [
                    {
                        "tag": "markdown",
                        "content": f"开始日期：{start_date.isoformat()}",
                    },
                    {
                        "tag": "markdown",
                        "content": f"结束日期：{end_date.isoformat()}",
                    },
                ]
            )
        else:
            elements.extend(
                [
                    {
                        "tag": "markdown",
                        "content": "开始日期",
                    },
                    {
                        "tag": "date_picker",
                        "initial_date": start_date.isoformat(),
                        "placeholder": {"tag": "plain_text", "content": "请选择开始日期"},
                        "value": start_picker_value,
                    },
                    {
                        "tag": "markdown",
                        "content": "结束日期",
                    },
                    {
                        "tag": "date_picker",
                        "initial_date": end_date.isoformat(),
                        "placeholder": {"tag": "plain_text", "content": "请选择结束日期"},
                        "value": end_picker_value,
                    },
                ]
            )

        if readonly:
            button_text = "已提交" if submitted else "后台处理中"
            elements.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": button_text},
                    "type": "default",
                    "disabled": True,
                }
            )
        else:
            elements.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "后台处理中" if submit_syncing else "提交"},
                    "type": "primary",
                    "behaviors": [{"type": "callback", "value": submit_action_value}],
                }
            )

        return {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {
                "template": "wathet",
                "title": {
                    "tag": "plain_text",
                    "content": "请假暂停自动预约",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": elements,
            },
        }


def _build_toggle_buttons(
    *,
    target_date: date,
    user_open_id: str,
    allowed_meals: list[Meal],
    selected_meals: list[Meal],
    default_meals: list[Meal],
    meal_prices: dict[Meal, Decimal],
    meal_record_ids: dict[Meal, str | None],
    refresh_syncing: bool,
) -> list[dict[str, Any]]:
    selected_values = [meal.value for meal in selected_meals]
    allowed_values = [meal.value for meal in allowed_meals]
    default_values = [meal.value for meal in default_meals]
    meal_price_values = {meal.value: _decimal_to_string(meal_prices.get(meal)) for meal in allowed_meals}
    meal_record_id_values = {meal.value: meal_record_ids.get(meal) for meal in allowed_meals}

    def payload(toggle: Meal) -> dict[str, Any]:
        return {
            "action": "toggle_meal",
            "target_date": target_date.isoformat(),
            "target_open_id": user_open_id,
            "allowed_meals": allowed_values,
            "default_meals": default_values,
            "selected_meals": selected_values,
            "meal_prices": meal_price_values,
            "meal_record_ids": meal_record_id_values,
            "toggle_meal": toggle.value,
        }

    refresh_payload = {
        "action": "refresh_state",
        "target_date": target_date.isoformat(),
        "target_open_id": user_open_id,
        "allowed_meals": allowed_values,
        "default_meals": default_values,
        "selected_meals": selected_values,
        "meal_prices": meal_price_values,
        "meal_record_ids": meal_record_id_values,
    }

    buttons: list[dict[str, Any]] = []
    for meal in allowed_meals:
        selected = meal in selected_meals
        buttons.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": meal.value},
                "type": "primary" if selected else "default",
                "behaviors": [{"type": "callback", "value": payload(meal)}],
            }
        )
    buttons.append(
        {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "后台处理中" if refresh_syncing else "刷新"},
            "type": "primary" if refresh_syncing else "default",
            "behaviors": [{"type": "callback", "value": refresh_payload}],
        }
    )
    return buttons


def _decimal_to_string(value: Decimal | None) -> str:
    if value is None:
        return "0"
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _weekday_text(target_date: date) -> str:
    weekdays = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
    return weekdays[target_date.weekday()]
