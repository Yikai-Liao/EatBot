from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.domain.cards import LeaveCardBuilder, ReservationCardBuilder
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
    assert card["header"]["title"]["content"] == "食堂预约 2026-02-13 周五"
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


def test_card_refresh_button_shows_syncing_state_when_requested() -> None:
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
        refresh_syncing=True,
    )

    card = json.loads(card_json)
    buttons = [item for item in card["body"]["elements"] if item.get("tag") == "button"]
    assert len(buttons) == 3
    refresh_buttons = [button for button in buttons if button["text"]["content"] == "后台处理中"]
    assert len(refresh_buttons) == 1
    assert refresh_buttons[0]["type"] == "primary"


def test_leave_card_contains_two_date_pickers_and_submit_button() -> None:
    builder = LeaveCardBuilder()
    card_json = builder.build(user_open_id="ou_test", target_date=date(2026, 2, 13))

    card = json.loads(card_json)
    assert card["schema"] == "2.0"
    assert card["header"]["title"]["content"] == "请假暂停自动预约"
    elements = card["body"]["elements"]
    assert elements[0]["content"] == "选择暂停自动预约的开始日期和结束日期。你仍可在当日卡片中手动点击恢复预约。"
    assert elements[1]["content"] == "开始日期"
    date_pickers = [item for item in elements if item.get("tag") == "date_picker"]
    assert len(date_pickers) == 2
    assert [item["initial_date"] for item in date_pickers] == ["2026-02-13", "2026-02-13"]
    assert [item["value"]["action"] for item in date_pickers] == ["select_leave_start", "select_leave_end"]
    assert elements[3]["content"] == "结束日期"
    buttons = [item for item in elements if item.get("tag") == "button"]
    assert len(buttons) == 1
    assert buttons[0]["text"]["content"] == "提交"
    assert buttons[0]["behaviors"][0]["value"]["action"] == "submit_leave_range"


def test_leave_card_uses_selected_dates_in_picker_and_submit_payload() -> None:
    builder = LeaveCardBuilder()
    card_json = builder.build(
        user_open_id="ou_test",
        target_date=date(2026, 2, 13),
        selected_start_date=date(2026, 2, 14),
        selected_end_date=date(2026, 2, 20),
    )

    card = json.loads(card_json)
    date_pickers = [item for item in card["body"]["elements"] if item.get("tag") == "date_picker"]
    assert [item["initial_date"] for item in date_pickers] == ["2026-02-14", "2026-02-20"]
    submit_button = next(item for item in card["body"]["elements"] if item.get("tag") == "button")
    submit_value = submit_button["behaviors"][0]["value"]
    assert submit_value["selected_start_date"] == "2026-02-14"
    assert submit_value["selected_end_date"] == "2026-02-20"


def test_leave_card_submit_button_shows_syncing_state_when_requested() -> None:
    builder = LeaveCardBuilder()
    card_json = builder.build(user_open_id="ou_test", target_date=date(2026, 2, 13), submit_syncing=True)

    card = json.loads(card_json)
    date_pickers = [item for item in card["body"]["elements"] if item.get("tag") == "date_picker"]
    assert date_pickers == []
    buttons = [item for item in card["body"]["elements"] if item.get("tag") == "button"]
    assert len(buttons) == 1
    assert buttons[0]["text"]["content"] == "后台处理中"
    assert buttons[0]["disabled"] is True


def test_leave_card_submitted_state_is_readonly() -> None:
    builder = LeaveCardBuilder()
    card_json = builder.build(
        user_open_id="ou_test",
        target_date=date(2026, 2, 13),
        selected_start_date=date(2026, 2, 14),
        selected_end_date=date(2026, 2, 20),
        readonly=True,
        submitted=True,
    )

    card = json.loads(card_json)
    elements = card["body"]["elements"]
    date_pickers = [item for item in elements if item.get("tag") == "date_picker"]
    assert date_pickers == []
    assert elements[1]["content"] == "开始日期：2026-02-14"
    assert elements[2]["content"] == "结束日期：2026-02-20"
    button = next(item for item in elements if item.get("tag") == "button")
    assert button["text"]["content"] == "已提交"
    assert button["disabled"] is True
