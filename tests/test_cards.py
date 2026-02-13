from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.domain.cards import ReservationCardBuilder
from eatbot.domain.models import Meal


class CardBuilderTests(unittest.TestCase):
    def test_card_uses_two_toggle_buttons_with_callback(self) -> None:
        builder = ReservationCardBuilder()
        card_json = builder.build(
            target_date=date(2026, 2, 13),
            user_open_id="ou_test",
            allowed_meals={Meal.LUNCH, Meal.DINNER},
            default_meals={Meal.LUNCH},
            selected_meals={Meal.LUNCH},
            meal_prices={Meal.LUNCH: Decimal("20"), Meal.DINNER: Decimal("25")},
            meal_record_ids={Meal.LUNCH: "rec_lunch", Meal.DINNER: None},
        )

        card = json.loads(card_json)
        self.assertEqual(card["schema"], "2.0")
        elements = card["body"]["elements"]
        text = elements[0]["content"]
        self.assertIn("\n", text)
        self.assertNotIn("\\n", text)
        buttons = [item for item in elements if item.get("tag") == "button"]
        self.assertEqual(len(buttons), 3)
        meal_buttons = [button for button in buttons if button["text"]["content"] in {"午餐", "晚餐"}]
        self.assertEqual(len(meal_buttons), 2)
        self.assertEqual(meal_buttons[0]["text"]["content"], "午餐")
        self.assertEqual(meal_buttons[0]["type"], "primary")
        self.assertEqual(meal_buttons[1]["text"]["content"], "晚餐")
        self.assertEqual(meal_buttons[1]["type"], "default")
        refresh_buttons = [button for button in buttons if button["text"]["content"] == "刷新"]
        self.assertEqual(len(refresh_buttons), 1)
        self.assertEqual(refresh_buttons[0]["type"], "default")
        for button in buttons:
            self.assertIn("behaviors", button)
            self.assertEqual(button["behaviors"][0]["type"], "callback")
            value = button["behaviors"][0]["value"]
            self.assertIn("meal_prices", value)
            self.assertIn("meal_record_ids", value)
        self.assertEqual(refresh_buttons[0]["behaviors"][0]["value"]["action"], "refresh_state")


if __name__ == "__main__":
    unittest.main()
