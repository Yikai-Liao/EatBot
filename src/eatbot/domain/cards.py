from __future__ import annotations

from datetime import date
import json
from typing import Any

from .models import Meal


class ReservationCardBuilder:
    _MEAL_ORDER = {Meal.LUNCH: 0, Meal.DINNER: 1}

    def build(
        self,
        *,
        target_date: date,
        user_open_id: str,
        allowed_meals: set[Meal],
        default_meals: set[Meal],
        selected_meals: set[Meal],
    ) -> str:
        card = self.build_payload(
            target_date=target_date,
            user_open_id=user_open_id,
            allowed_meals=allowed_meals,
            default_meals=default_meals,
            selected_meals=selected_meals,
        )
        return json.dumps(card, ensure_ascii=False)

    def build_payload(
        self,
        *,
        target_date: date,
        user_open_id: str,
        allowed_meals: set[Meal],
        default_meals: set[Meal],
        selected_meals: set[Meal],
    ) -> dict[str, Any]:
        allowed_sorted = self._sorted_meals(allowed_meals)
        defaults = default_meals & allowed_meals
        selected = selected_meals & allowed_meals

        default_text = "、".join(meal.value for meal in self._sorted_meals(defaults)) or "无"
        selected_text = "、".join(meal.value for meal in self._sorted_meals(selected)) or "无"
        buttons = _build_toggle_buttons(
            target_date=target_date,
            user_open_id=user_open_id,
            allowed_meals=allowed_sorted,
            selected_meals=self._sorted_meals(selected),
        )

        return {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"食堂预约 {target_date.isoformat()}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            "点击按钮切换餐次并立即保存。\\n"
                            f"默认偏好：{default_text}\\n"
                            f"当前选择：{selected_text}"
                        ),
                    },
                    *buttons,
                ],
            },
        }

    def _sorted_meals(self, meals: set[Meal]) -> list[Meal]:
        return sorted(meals, key=lambda item: self._MEAL_ORDER.get(item, 999))


def _build_toggle_buttons(
    *,
    target_date: date,
    user_open_id: str,
    allowed_meals: list[Meal],
    selected_meals: list[Meal],
) -> list[dict[str, Any]]:
    selected_values = [meal.value for meal in selected_meals]
    allowed_values = [meal.value for meal in allowed_meals]

    def payload(toggle: Meal) -> dict[str, Any]:
        return {
            "action": "toggle_meal",
            "target_date": target_date.isoformat(),
            "target_open_id": user_open_id,
            "allowed_meals": allowed_values,
            "selected_meals": selected_values,
            "toggle_meal": toggle.value,
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
    return buttons
