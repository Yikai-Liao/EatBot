from __future__ import annotations

from datetime import date
import json

from .models import Meal


class ReservationCardBuilder:
    def build(
        self,
        *,
        target_date: date,
        user_open_id: str,
        allowed_meals: set[Meal],
        default_meals: set[Meal],
    ) -> str:
        meal_options = []
        if Meal.LUNCH in allowed_meals:
            meal_options.append(
                {
                    "text": {"tag": "plain_text", "content": Meal.LUNCH.value},
                    "value": Meal.LUNCH.value,
                    "checked": Meal.LUNCH in default_meals,
                }
            )

        if Meal.DINNER in allowed_meals:
            meal_options.append(
                {
                    "text": {"tag": "plain_text", "content": Meal.DINNER.value},
                    "value": Meal.DINNER.value,
                    "checked": Meal.DINNER in default_meals,
                }
            )

        value_payload = {
            "action": "submit_reservation",
            "target_date": target_date.isoformat(),
            "target_open_id": user_open_id,
            "allowed_meals": sorted(meal.value for meal in allowed_meals),
        }

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"食堂预约 {target_date.isoformat()}",
                },
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": "请勾选今天要预约的餐次，点击提交后会同步到用餐记录表。",
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "checkboxes",
                            "name": "meals",
                            "options": meal_options,
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "提交预约"},
                            "type": "primary",
                            "value": value_payload,
                        },
                    ],
                },
            ],
        }
        return json.dumps(card, ensure_ascii=False)
