from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
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


class BookingServiceMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Mock()
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
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]

        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "submit_reservation",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_sender",
                        "allowed_meals": ["午餐", "晚餐"],
                    },
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
        )
        self.repo.cancel_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.DINNER,
        )
        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.toast.content, "预约已更新")

    def test_handle_card_action_selected_meals_from_action_value(self) -> None:
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "submit_reservation",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_sender",
                        "allowed_meals": ["午餐", "晚餐"],
                        "selected_meals": ["晚餐"],
                    },
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.DINNER,
            price=Decimal("25"),
        )
        self.repo.cancel_meal_record.assert_called_once_with(
            target_date=date(2099, 1, 1),
            open_id="ou_sender",
            meal=Meal.LUNCH,
        )

    def test_handle_card_action_toggle_meal_updates_and_returns_raw_card(self) -> None:
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "toggle_meal",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_sender",
                        "allowed_meals": ["午餐", "晚餐"],
                        "selected_meals": ["午餐"],
                        "toggle_meal": "晚餐",
                    },
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = self.service.handle_card_action(data)

        self.repo.upsert_meal_record.assert_has_calls(
            [
                call(
                    target_date=date(2099, 1, 1),
                    open_id="ou_sender",
                    meal=Meal.LUNCH,
                    price=Decimal("20"),
                ),
                call(
                    target_date=date(2099, 1, 1),
                    open_id="ou_sender",
                    meal=Meal.DINNER,
                    price=Decimal("25"),
                ),
            ],
            any_order=True,
        )
        self.assertEqual(response.toast.type, "info")
        self.assertEqual(response.card.type, "raw")
        data_obj = response.card.data
        buttons = [item for item in data_obj["body"]["elements"] if item.get("tag") == "button"]
        self.assertEqual(len(buttons), 2)
        self.assertTrue(all(button["type"] == "primary" for button in buttons))

    def test_handle_card_frame_action_works_for_card_message_type(self) -> None:
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]
        data = SimpleNamespace(
            open_id="ou_sender",
            action=SimpleNamespace(
                value={
                    "action": "toggle_meal",
                    "target_date": "2099-01-01",
                    "target_open_id": "ou_sender",
                    "allowed_meals": ["午餐", "晚餐"],
                    "selected_meals": ["午餐"],
                    "toggle_meal": "午餐",
                },
                form_value={},
            ),
        )

        response = self.service.handle_card_frame_action(data)

        self.repo.cancel_meal_record.assert_has_calls(
            [
                call(
                    target_date=date(2099, 1, 1),
                    open_id="ou_sender",
                    meal=Meal.LUNCH,
                ),
                call(
                    target_date=date(2099, 1, 1),
                    open_id="ou_sender",
                    meal=Meal.DINNER,
                ),
            ],
            any_order=True,
        )
        self.assertEqual(response["toast"]["type"], "info")
        self.assertEqual(response["card"]["type"], "raw")

    def test_handle_card_action_rejects_operator_mismatch(self) -> None:
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_user")]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "submit_reservation",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_user",
                        "allowed_meals": ["午餐"],
                    },
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
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "toggle_meal",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_sender",
                        "allowed_meals": ["午餐"],
                        "selected_meals": [],
                        "toggle_meal": "午餐",
                    },
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
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender")]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value={
                        "action": "toggle_meal",
                        "target_date": "2099-01-01",
                        "target_open_id": "ou_sender",
                        "allowed_meals": ["午餐"],
                        "selected_meals": [],
                        "toggle_meal": "午餐",
                    },
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
