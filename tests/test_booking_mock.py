from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.config import RuntimeConfig
from eatbot.domain.models import Meal, UserProfile
from eatbot.services.booking import BookingService


def build_config() -> RuntimeConfig:
    return RuntimeConfig.model_validate(
        {
            "app_id": "id",
            "app_secret": "secret",
            "app_token": "app",
            "tables": {
                "user_config": "t1",
                "meal_schedule": "t2",
                "meal_record": "t3",
                "stats_receivers": "t4",
            },
            "field_names": {
                "user_config": {
                    "display_name": "用餐人员名称",
                    "user": "人员",
                    "meal_preference": "餐食偏好",
                    "lunch_price": "午餐单价",
                    "dinner_price": "晚餐单价",
                    "enabled": "启用",
                },
                "meal_schedule": {
                    "start_date": "开始日期",
                    "end_date": "截止日期",
                    "meals": "当日餐食包含",
                    "remark": "备注",
                },
                "meal_record": {
                    "date": "日期",
                    "user": "用餐者",
                    "meal_type": "餐食类型",
                    "price": "价格",
                    "reservation_status": "预约状态",
                },
                "stats_receivers": {
                    "user": "人员",
                },
            },
        }
    )


def make_user(open_id: str = "ou_test", enabled: bool = True) -> UserProfile:
    return UserProfile(
        open_id=open_id,
        display_name="测试用户",
        enabled=enabled,
        lunch_price=Decimal("20"),
        dinner_price=Decimal("25"),
        meal_preferences={Meal.LUNCH},
    )


def make_meal_row(meal: Meal, *, reservation_status: bool, record_id: str) -> SimpleNamespace:
    return SimpleNamespace(meal_type=meal, reservation_status=reservation_status, record_id=record_id)


def build_action_value(
    *,
    action: str,
    target_open_id: str,
    allowed_meals: list[str],
    default_meals: list[str],
    selected_meals: list[str],
    toggle_meal: str | None = None,
    meal_record_ids: dict[str, str | None] | None = None,
) -> dict:
    value = {
        "action": action,
        "target_date": "2099-01-01",
        "target_open_id": target_open_id,
        "allowed_meals": allowed_meals,
        "default_meals": default_meals,
        "selected_meals": selected_meals,
        "meal_prices": {"午餐": "20", "晚餐": "25"},
        "meal_record_ids": meal_record_ids or {"午餐": None, "晚餐": None},
    }
    if toggle_meal is not None:
        value["toggle_meal"] = toggle_meal
    return value


class BookingServiceMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Mock()
        self.repo.upsert_meal_record.return_value = "rec_default"
        self.repo.list_user_meal_rows.return_value = []
        self.im = Mock()
        self.service = BookingService(config=build_config(), repository=self.repo, im=self.im)

    def test_send_daily_cards_writes_default_meal_record_and_send_card(self) -> None:
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [make_user()]

        self.service.send_daily_cards(target_date=date(2026, 2, 12))

        self.repo.upsert_meal_record.assert_called_once_with(
            target_date=date(2026, 2, 12),
            open_id="ou_test",
            meal=Meal.LUNCH,
            price=Decimal("20"),
        )
        self.im.send_interactive.assert_called_once()

    def test_send_daily_cards_prioritize_existing_records_for_button_state(self) -> None:
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [make_user()]
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=False, record_id="rec_lunch_off"),
            make_meal_row(Meal.DINNER, reservation_status=True, record_id="rec_dinner_on"),
        ]

        self.service.send_daily_cards(target_date=date(2026, 2, 12))

        self.repo.upsert_meal_record.assert_not_called()
        self.im.send_interactive.assert_called_once()
        sent_card = self.im.send_interactive.call_args.kwargs["card_json"]
        payload = json.loads(sent_card)
        meal_buttons = [
            item for item in payload["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        status_by_meal = {item["text"]["content"]: item["type"] for item in meal_buttons}
        self.assertEqual(status_by_meal["午餐"], "default")
        self.assertEqual(status_by_meal["晚餐"], "primary")

    def test_send_daily_cards_continue_when_one_user_send_failed(self) -> None:
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [
            make_user(open_id="ou_1"),
            make_user(open_id="ou_2"),
        ]
        self.im.send_interactive.side_effect = [RuntimeError("send failed"), None]

        self.service.send_daily_cards(target_date=date(2026, 2, 12))

        self.assertEqual(self.im.send_interactive.call_count, 2)
        self.repo.upsert_meal_record.assert_has_calls(
            [
                call(
                    target_date=date(2026, 2, 12),
                    open_id="ou_1",
                    meal=Meal.LUNCH,
                    price=Decimal("20"),
                ),
                call(
                    target_date=date(2026, 2, 12),
                    open_id="ou_2",
                    meal=Meal.LUNCH,
                    price=Decimal("20"),
                ),
            ]
        )

    def test_handle_message_event_triggers_today_card(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    message=SimpleNamespace(message_type="text", content='{"text":"订餐"}'),
                    sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_message_event(data)
            mocked.assert_called_once_with("ou_sender")

    def test_handle_card_action_updates_and_cancels_records(self) -> None:
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="submit_reservation",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=[],
                    ),
                    form_value={"meals": ["午餐"]},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.LUNCH,
            price=Decimal("20"),
            record_id=None,
            prefer_direct=True,
        )
        self.repo.cancel_meal_record.assert_not_called()
        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.toast.content, "预约已更新")

    def test_handle_card_action_selected_meals_from_action_value(self) -> None:
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="submit_reservation",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["晚餐"],
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_not_called()
        self.repo.cancel_meal_record.assert_not_called()

    def test_handle_card_action_toggle_meal_updates_and_returns_raw_card(self) -> None:
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.DINNER, reservation_status=False, record_id="rec_dinner_existing")
        ]
        self.repo.upsert_meal_record.return_value = "rec_dinner_existing"
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["午餐"],
                        toggle_meal="晚餐",
                        meal_record_ids={"午餐": "rec_lunch", "晚餐": None},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.DINNER,
            price=Decimal("25"),
            record_id="rec_dinner_existing",
            prefer_direct=True,
        )
        self.repo.list_user_meal_rows.assert_called_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
        )
        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.card.type, "raw")
        data_obj = response.card.data
        meal_buttons = [
            item for item in data_obj["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        self.assertEqual(len(meal_buttons), 2)
        self.assertTrue(all(button["type"] == "primary" for button in meal_buttons))

    def test_handle_card_frame_action_works_for_card_message_type(self) -> None:
        data = SimpleNamespace(
            open_id="ou_sender",
            action=SimpleNamespace(
                value=build_action_value(
                    action="toggle_meal",
                    target_open_id="ou_sender",
                    allowed_meals=["午餐", "晚餐"],
                    default_meals=["午餐"],
                    selected_meals=["午餐"],
                    toggle_meal="午餐",
                    meal_record_ids={"午餐": "rec_lunch", "晚餐": None},
                ),
                form_value={},
            ),
        )

        response = self.service.handle_card_frame_action(data)

        self.repo.cancel_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.LUNCH,
            record_id="rec_lunch",
            prefer_direct=True,
        )
        self.assertEqual(response["toast"]["type"], "info")
        self.assertEqual(response["card"]["type"], "raw")

    def test_handle_card_action_refresh_state_only_reads_records(self) -> None:
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=False, record_id="rec_lunch"),
            make_meal_row(Meal.DINNER, reservation_status=True, record_id="rec_dinner"),
        ]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="refresh_state",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["午餐"],
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_not_called()
        self.repo.cancel_meal_record.assert_not_called()
        self.repo.list_user_meal_rows.assert_called_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
        )
        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.toast.content, "已刷新最新预约状态")
        payload = response.card.data
        meal_buttons = [
            item for item in payload["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        status_by_meal = {item["text"]["content"]: item["type"] for item in meal_buttons}
        self.assertEqual(status_by_meal["午餐"], "default")
        self.assertEqual(status_by_meal["晚餐"], "primary")

    def test_handle_card_action_rejects_operator_mismatch(self) -> None:
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="submit_reservation",
                        target_open_id="ou_user",
                        allowed_meals=["午餐"],
                        default_meals=[],
                        selected_meals=["午餐"],
                    ),
                    form_value={"meals": ["午餐"]},
                ),
                operator=SimpleNamespace(open_id="ou_other"),
            )
        )

        response = self.service.handle_card_action(data)

        self.assertEqual(response.toast.type, "error")
        self.assertEqual(response.toast.content, "仅允许本人提交预约")
        self.repo.upsert_meal_record.assert_not_called()
        self.repo.cancel_meal_record.assert_not_called()

    def test_send_stats_to_all_receivers(self) -> None:
        self.repo.count_meal_records.return_value = 3
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_1", "ou_2"]

        self.service.send_stats(target_date=date(2026, 2, 12), meal=Meal.LUNCH)

        self.im.send_text.assert_has_calls(
            [
                call("ou_1", "2026-02-12 午餐 预约人数: 3"),
                call("ou_2", "2026-02-12 午餐 预约人数: 3"),
            ]
        )
        self.assertEqual(self.im.send_text.call_count, 2)

    def test_send_card_to_user_today_when_user_missing(self) -> None:
        self.repo.list_user_profiles.return_value = []

        self.service.send_card_to_user_today("ou_missing")

        self.im.send_text.assert_called_once_with("ou_missing", "你不在用餐人员配置中，无法发起预约。")

    def test_handle_card_action_rejects_when_after_cutoff_with_simulated_now(self) -> None:
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            now_provider=lambda: datetime(2099, 1, 1, 21, 0),
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐"],
                        default_meals=[],
                        selected_meals=[],
                        toggle_meal="午餐",
                        meal_record_ids={"午餐": None},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = service.handle_card_action(data)

        self.assertEqual(response.toast.type, "error")
        self.assertIn("已过截止时间", response.toast.content)

    def test_handle_card_action_accepts_when_before_cutoff_with_simulated_now(self) -> None:
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            now_provider=lambda: datetime(2099, 1, 1, 9, 0),
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐"],
                        default_meals=[],
                        selected_meals=[],
                        toggle_meal="午餐",
                        meal_record_ids={"午餐": None},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = service.handle_card_action(data)

        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.toast.content, "预约已更新")


if __name__ == "__main__":
    unittest.main()
