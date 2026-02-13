from __future__ import annotations

from datetime import date
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
        )

        card = json.loads(card_json)
        self.assertEqual(card["schema"], "2.0")
        elements = card["body"]["elements"]
        text = elements[0]["content"]
        self.assertIn("\n", text)
        self.assertNotIn("\\n", text)
        buttons = [item for item in elements if item.get("tag") == "button"]
        self.assertEqual(len(buttons), 2)
        self.assertEqual(buttons[0]["text"]["content"], "午餐")
        self.assertEqual(buttons[0]["type"], "primary")
        self.assertEqual(buttons[1]["text"]["content"], "晚餐")
        self.assertEqual(buttons[1]["type"], "default")
        for button in buttons:
            self.assertIn("behaviors", button)
            self.assertEqual(button["behaviors"][0]["type"], "callback")


if __name__ == "__main__":
    unittest.main()
