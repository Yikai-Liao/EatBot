from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.domain.cards import ReservationCardBuilder
from eatbot.domain.models import Meal


def test_card_uses_two_toggle_buttons_with_callback() -> None:
    builder = ReservationCardBuilder()
    card_json = builder.build(
        target_date=date(2026, 2, 13),
        lunch_cutoff="10:30",
        dinner_cutoff="16:30",
        user_open_id="ou_test",
        allowed_meals={Meal.LUNCH, Meal.DINNER},
        default_meals={Meal.LUNCH},
        selected_meals={Meal.LUNCH},
        meal_prices={Meal.LUNCH: Decimal("20"), Meal.DINNER: Decimal("25")},
        meal_record_ids={Meal.LUNCH: "rec_lunch", Meal.DINNER: None},
    )

    card = json.loads(card_json)
    assert card["schema"] == "2.0"
    assert card["header"]["title"]["content"] == "食堂预约 2026-02-13（周五）"
    elements = card["body"]["elements"]
    text = elements[0]["content"]
    assert text == "点击按钮切换预约状态\n预约截止时间为：午餐10:30，晚餐16:30"
    buttons = [item for item in elements if item.get("tag") == "button"]
    assert len(buttons) == 3
    meal_buttons = [button for button in buttons if button["text"]["content"] in {"午餐", "晚餐"}]
    assert len(meal_buttons) == 2
    assert meal_buttons[0]["text"]["content"] == "午餐"
    assert meal_buttons[0]["type"] == "primary"
    assert meal_buttons[1]["text"]["content"] == "晚餐"
    assert meal_buttons[1]["type"] == "default"
    refresh_buttons = [button for button in buttons if button["text"]["content"] == "刷新"]
    assert len(refresh_buttons) == 1
    assert refresh_buttons[0]["type"] == "default"
    for button in buttons:
        assert "behaviors" in button
        assert button["behaviors"][0]["type"] == "callback"
        value = button["behaviors"][0]["value"]
        assert "meal_prices" in value
        assert "meal_record_ids" in value
    assert refresh_buttons[0]["behaviors"][0]["value"]["action"] == "refresh_state"
