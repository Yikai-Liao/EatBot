from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.feishu_clients import FeishuApiError
from eatbot.config import RuntimeConfig
from eatbot.domain.models import Meal, MealScheduleRule, UserProfile
from eatbot.services.booking import BookingService


def build_config() -> RuntimeConfig:
    return RuntimeConfig.model_validate(
        {
            "app_id": "id",
            "app_secret": "secret",
            "app_token": "app",
            "help_doc": "帮助文档：发送“卡片”获取当日卡片，发送“帮助”查看说明。",
            "tables": {
                "user_config": "t1",
                "meal_schedule": "t2",
                "meal_record": "t3",
                "stats_receivers": "t4",
                "meal_fee_archive": "t5",
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
                "meal_fee_archive": {
                    "user": "用餐者",
                    "start_date": "开始日期",
                    "end_date": "结束日期",
                    "fee": "费用",
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


class TestBookingServiceMock:
    def setup_method(self) -> None:
        self.repo = Mock()
        self.repo.upsert_meal_record.return_value = "rec_default"
        self.repo.list_user_meal_rows.return_value = []
        self.repo.list_reserved_meal_rows.return_value = []
        self.repo.cancel_reserved_meal_rows.return_value = 0
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender"), make_user(open_id="ou_test")]
        self.repo.list_stats_receiver_open_ids.return_value = []
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
        assert status_by_meal["午餐"] == "default"
        assert status_by_meal["晚餐"] == "primary"

    def test_send_daily_cards_rule_meals_override_default_preference(self) -> None:
        target_date = date(2026, 2, 12)
        self.repo.list_schedule_rules.return_value = [
            MealScheduleRule(
                start_date=target_date,
                end_date=target_date,
                meals={Meal.LUNCH},
            )
        ]
        user = make_user()
        user.meal_preferences = {Meal.LUNCH, Meal.DINNER}
        self.repo.list_user_profiles.return_value = [user]

        self.service.send_daily_cards(target_date=target_date)

        self.repo.upsert_meal_record.assert_called_once_with(
            target_date=target_date,
            open_id="ou_test",
            meal=Meal.LUNCH,
            price=Decimal("20"),
        )
        sent_card = self.im.send_interactive.call_args.kwargs["card_json"]
        payload = json.loads(sent_card)
        meal_buttons = [
            item for item in payload["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        assert [item["text"]["content"] for item in meal_buttons] == ["午餐"]

    def test_send_daily_cards_continue_when_one_user_send_failed(self) -> None:
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [
            make_user(open_id="ou_1"),
            make_user(open_id="ou_2"),
        ]
        self.im.send_interactive.side_effect = [RuntimeError("send failed"), None]

        self.service.send_daily_cards(target_date=date(2026, 2, 12))

        assert self.im.send_interactive.call_count == 2
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

    def test_preview_daily_cards_reports_skip_on_weekend_default_rule(self) -> None:
        target_date = date(2026, 2, 14)
        self.repo.list_schedule_rules.return_value = []
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_1", enabled=True)]
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_stat_1"]

        snapshot = self.service.build_cron_preview_snapshot(target_dates={target_date})
        will_execute, detail = self.service.preview_daily_cards(target_date=target_date, snapshot=snapshot)

        assert snapshot.schedule_rules_count == 0
        assert snapshot.enabled_user_count == 1
        assert snapshot.stats_receiver_count == 1
        assert will_execute is False
        assert "规则结果=不发送" in detail

    def test_preview_daily_cards_reports_execute_when_rule_matches(self) -> None:
        target_date = date(2026, 2, 14)
        self.repo.list_schedule_rules.return_value = [
            MealScheduleRule(
                start_date=target_date,
                end_date=target_date,
                meals={Meal.DINNER},
            )
        ]
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_1", enabled=True)]
        self.repo.list_stats_receiver_open_ids.return_value = []

        snapshot = self.service.build_cron_preview_snapshot(target_dates={target_date})
        will_execute, detail = self.service.preview_daily_cards(target_date=target_date, snapshot=snapshot)

        assert will_execute is True
        assert "规则餐次=晚餐" in detail
        assert "启用用户=1" in detail

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

    def test_handle_message_event_triggers_today_card_with_today_card_text(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    message=SimpleNamespace(message_type="text", content='{"text":"当日卡片"}'),
                    sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_message_event(data)
            mocked.assert_called_once_with("ou_sender")

    def test_handle_message_event_triggers_today_card_with_card_text(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    message=SimpleNamespace(message_type="text", content='{"text":"卡片"}'),
                    sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_message_event(data)
            mocked.assert_called_once_with("ou_sender")

    def test_handle_message_event_help_command_sends_help_doc(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    message=SimpleNamespace(message_type="text", content='{"text":"帮助"}'),
                    sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_message_event(data)

            mocked.assert_not_called()
            self.im.send_text.assert_called_once_with("ou_sender", self.service._config.help_doc)

    def test_handle_message_event_unknown_text_sends_help_doc(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    message=SimpleNamespace(message_type="text", content='{"text":"随便说点什么"}'),
                    sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_message_event(data)

            mocked.assert_not_called()
            self.im.send_text.assert_called_once_with("ou_sender", self.service._config.help_doc)

    def test_handle_bot_menu_event_triggers_today_card(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    event_key="当日卡片",
                    operator=SimpleNamespace(operator_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_bot_menu_event(data)
            mocked.assert_called_once_with("ou_sender")

    def test_handle_bot_menu_event_ignores_unknown_event_key(self) -> None:
        with patch.object(self.service, "send_card_to_user_today") as mocked:
            data = SimpleNamespace(
                event=SimpleNamespace(
                    event_key="其他菜单",
                    operator=SimpleNamespace(operator_id=SimpleNamespace(open_id="ou_sender")),
                )
            )
            self.service.handle_bot_menu_event(data)
            mocked.assert_not_called()

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
        assert response.toast.type == "info"
        assert response.toast.content == "预约已更新"

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
        assert response.toast.type == "info"
        assert response.card.type == "raw"
        data_obj = response.card.data
        meal_buttons = [
            item for item in data_obj["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        assert len(meal_buttons) == 2
        status_by_meal = {item["text"]["content"]: item["type"] for item in meal_buttons}
        assert status_by_meal["午餐"] == "default"
        assert status_by_meal["晚餐"] == "primary"

    def test_handle_card_frame_action_works_for_card_message_type(self) -> None:
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=True, record_id="rec_lunch")
        ]
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
        assert response["toast"]["type"] == "info"
        assert response["card"]["type"] == "raw"

    def test_handle_card_action_revalidate_schedule_and_cancel_disallowed_meal(self) -> None:
        target_date = date(2099, 1, 1)
        self.repo.list_schedule_rules.return_value = [
            MealScheduleRule(
                start_date=target_date,
                end_date=target_date,
                meals={Meal.LUNCH},
            )
        ]
        self.repo.list_user_meal_rows.side_effect = [
            [make_meal_row(Meal.DINNER, reservation_status=True, record_id="rec_dinner_existing")],
            [make_meal_row(Meal.DINNER, reservation_status=False, record_id="rec_dinner_existing")],
        ]
        data = SimpleNamespace(
            event=SimpleNamespace(
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["晚餐"],
                        toggle_meal="晚餐",
                        meal_record_ids={"午餐": None, "晚餐": "rec_dinner_existing"},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = self.service.handle_card_action(data)

        self.repo.cancel_meal_record.assert_called_once_with(
            target_date=target_date,
            open_id="ou_sender",
            meal=Meal.DINNER,
            record_id="rec_dinner_existing",
            prefer_direct=True,
        )
        self.repo.upsert_meal_record.assert_not_called()
        assert response.toast.type == "info"
        assert "不可预约" in response.toast.content
        payload = response.card.data
        meal_buttons = [
            item for item in payload["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        assert [item["text"]["content"] for item in meal_buttons] == ["午餐"]

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
        assert response.toast.type == "info"
        assert response.toast.content == "已刷新最新预约状态"
        payload = response.card.data
        meal_buttons = [
            item for item in payload["body"]["elements"] if item.get("tag") == "button" and item["text"]["content"] in {"午餐", "晚餐"}
        ]
        status_by_meal = {item["text"]["content"]: item["type"] for item in meal_buttons}
        assert status_by_meal["午餐"] == "default"
        assert status_by_meal["晚餐"] == "primary"

    def test_handle_card_action_with_token_returns_optimistic_card_and_runs_in_background(self) -> None:
        tasks: list = []
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=True, record_id="rec_lunch"),
            make_meal_row(Meal.DINNER, reservation_status=True, record_id="rec_dinner"),
        ]
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_1",
                context=SimpleNamespace(open_message_id="om_1"),
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

        response = service.handle_card_action(data)

        assert response.toast.type == "info"
        assert response.toast.content == "处理中"
        assert response.card.type == "raw"
        optimistic_payload = response.card.data
        optimistic_refresh_button = next(
            item
            for item in optimistic_payload["body"]["elements"]
            if item.get("tag") == "button" and item["text"]["content"] == "后台处理中"
        )
        assert optimistic_refresh_button["type"] == "primary"
        assert len(tasks) == 1
        self.repo.upsert_meal_record.assert_not_called()

        tasks[0]()

        self.im.delay_update_card.assert_called_once()
        kwargs = self.im.delay_update_card.call_args.kwargs
        assert kwargs["token"] == "c_token_1"
        assert kwargs["card_payload"] is not None
        assert kwargs["toast_content"] == "处理完成"
        self.im.patch_interactive.assert_not_called()

    def test_handle_card_action_rejects_when_user_is_processing_in_background(self) -> None:
        tasks: list = []
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_1",
                context=SimpleNamespace(open_message_id="om_1"),
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

        first_response = service.handle_card_action(data)
        second_response = service.handle_card_action(data)

        assert first_response.toast.type == "info"
        assert first_response.toast.content == "处理中"
        assert second_response.toast.type == "info"
        assert second_response.toast.content == "后台处理中，请稍后"
        assert second_response.card is None
        assert len(tasks) == 1

    def test_handle_card_action_refresh_with_token_returns_syncing_card(self) -> None:
        tasks: list = []
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=False, record_id="rec_lunch"),
            make_meal_row(Meal.DINNER, reservation_status=True, record_id="rec_dinner"),
        ]
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_1",
                context=SimpleNamespace(open_message_id="om_1"),
                action=SimpleNamespace(
                    value=build_action_value(
                        action="refresh_state",
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

        response = service.handle_card_action(data)
        assert response.toast.type == "info"
        assert response.toast.content == "处理中"
        assert response.card.type == "raw"
        optimistic_payload = response.card.data
        optimistic_refresh_button = next(
            item
            for item in optimistic_payload["body"]["elements"]
            if item.get("tag") == "button" and item["text"]["content"] == "后台处理中"
        )
        assert optimistic_refresh_button["type"] == "primary"
        assert len(tasks) == 1

        tasks[0]()

        self.im.delay_update_card.assert_called_once()
        kwargs = self.im.delay_update_card.call_args.kwargs
        assert kwargs["token"] == "c_token_1"
        assert kwargs["card_payload"] is not None
        assert kwargs["toast_content"] == "处理完成"
        self.im.patch_interactive.assert_not_called()

    def test_handle_card_action_with_token_only_context_keeps_card_update_via_callback_token(self) -> None:
        tasks: list = []
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=True, record_id="rec_lunch"),
            make_meal_row(Meal.DINNER, reservation_status=False, record_id="rec_dinner"),
        ]
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_2",
                context=SimpleNamespace(open_message_id=None),
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["午餐"],
                        toggle_meal="晚餐",
                        meal_record_ids={"午餐": "rec_lunch", "晚餐": "rec_dinner"},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = service.handle_card_action(data)
        assert response.toast.type == "info"
        assert response.toast.content == "处理中"
        assert response.card.type == "raw"
        optimistic_payload = response.card.data
        optimistic_refresh_button = next(
            item
            for item in optimistic_payload["body"]["elements"]
            if item.get("tag") == "button" and item["text"]["content"] == "后台处理中"
        )
        assert optimistic_refresh_button["type"] == "primary"
        assert len(tasks) == 1

        tasks[0]()

        self.im.delay_update_card.assert_called_once()
        kwargs = self.im.delay_update_card.call_args.kwargs
        assert kwargs["token"] == "c_token_2"
        assert kwargs["card_payload"] is not None
        assert kwargs["toast_content"] == "处理完成"
        self.im.patch_interactive.assert_not_called()

    def test_handle_card_action_token_update_code_10002_falls_back_to_open_message_id_patch(self) -> None:
        tasks: list = []
        self.repo.list_user_meal_rows.return_value = [
            make_meal_row(Meal.LUNCH, reservation_status=True, record_id="rec_lunch"),
            make_meal_row(Meal.DINNER, reservation_status=False, record_id="rec_dinner"),
        ]
        self.im.delay_update_card.side_effect = FeishuApiError(
            "interactive.v1.card.update 调用失败, code=10002, msg=[UpdateMessageWithToken] msg: [params err]"
        )
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_3",
                context=SimpleNamespace(open_message_id="om_3"),
                action=SimpleNamespace(
                    value=build_action_value(
                        action="toggle_meal",
                        target_open_id="ou_sender",
                        allowed_meals=["午餐", "晚餐"],
                        default_meals=["午餐"],
                        selected_meals=["午餐"],
                        toggle_meal="晚餐",
                        meal_record_ids={"午餐": "rec_lunch", "晚餐": "rec_dinner"},
                    ),
                    form_value={},
                ),
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = service.handle_card_action(data)
        assert response.toast.type == "info"
        assert response.toast.content == "处理中"
        assert len(tasks) == 1

        tasks[0]()

        self.im.delay_update_card.assert_called_once()
        delay_kwargs = self.im.delay_update_card.call_args.kwargs
        assert delay_kwargs["token"] == "c_token_3"
        assert delay_kwargs["toast_content"] == "处理完成"
        self.im.patch_interactive.assert_called_once()
        patch_kwargs = self.im.patch_interactive.call_args.kwargs
        assert patch_kwargs["message_id"] == "om_3"

    def test_handle_card_action_with_token_blocks_after_cutoff_before_background(self) -> None:
        tasks: list = []
        service = BookingService(
            config=build_config(),
            repository=self.repo,
            im=self.im,
            now_provider=lambda: datetime(2099, 1, 1, 21, 0),
            background_runner=tasks.append,
        )
        data = SimpleNamespace(
            event=SimpleNamespace(
                token="c_token_1",
                context=SimpleNamespace(open_message_id="om_1"),
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
                operator=SimpleNamespace(open_id="ou_sender"),
            )
        )

        response = service.handle_card_action(data)

        assert response.toast.type == "error"
        assert "已过截止时间" in response.toast.content
        assert len(tasks) == 0

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

        assert response.toast.type == "error"
        assert response.toast.content == "仅允许本人提交预约"
        self.repo.upsert_meal_record.assert_not_called()
        self.repo.cancel_meal_record.assert_not_called()

    def test_send_stats_to_all_receivers(self) -> None:
        self.repo.list_reserved_meal_rows.return_value = [
            SimpleNamespace(open_id="ou_a", record_id="rec_1"),
            SimpleNamespace(open_id="ou_b", record_id="rec_2"),
            SimpleNamespace(open_id="ou_c", record_id="rec_3"),
        ]
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_1", "ou_2"]

        self.service.send_stats(target_date=date(2026, 2, 12), meal=Meal.LUNCH)

        self.im.send_text.assert_has_calls(
            [
                call("ou_1", "[管理员] 2026-02-12 周四 午餐 预约人数: 3"),
                call("ou_2", "[管理员] 2026-02-12 周四 午餐 预约人数: 3"),
            ]
        )
        assert self.im.send_text.call_count == 2
        self.repo.cancel_reserved_meal_rows.assert_not_called()

    def test_send_stats_cancel_meal_when_reserved_count_below_minimum(self) -> None:
        config = build_config()
        config.schedule.lunch_min_reserved_count = 3
        service = BookingService(config=config, repository=self.repo, im=self.im)
        self.repo.list_reserved_meal_rows.return_value = [
            SimpleNamespace(open_id="ou_booked_1", record_id="rec_1"),
            SimpleNamespace(open_id="ou_booked_2", record_id="rec_2"),
        ]
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_admin"]

        service.send_stats(target_date=date(2026, 2, 12), meal=Meal.LUNCH)

        self.repo.cancel_reserved_meal_rows.assert_called_once_with(rows=self.repo.list_reserved_meal_rows.return_value)
        self.im.send_text.assert_has_calls(
            [
                call("ou_admin", "[管理员] 2026-02-12 周四 午餐 预约人数: 2，小于最小用餐人数 3 人，本餐取消"),
                call(
                    "ou_booked_1",
                    "2026-02-12 周四 午餐 预约人数: 2，小于最小用餐人数 3 人，本餐取消。请注意，需要自行解决午餐。",
                ),
                call(
                    "ou_booked_2",
                    "2026-02-12 周四 午餐 预约人数: 2，小于最小用餐人数 3 人，本餐取消。请注意，需要自行解决午餐。",
                ),
            ]
        )
        assert self.im.send_text.call_count == 3

    def test_preview_fee_archive_returns_skip_when_not_settlement_day(self) -> None:
        should_run, detail = self.service.preview_fee_archive(target_date=date(2026, 2, 14))

        assert should_run is False
        assert "非归档日" in detail
        assert "2026-02-15" in detail

    def test_preview_fee_archive_fallbacks_to_last_day_when_month_day_not_exists(self) -> None:
        config = build_config()
        config.schedule.fee_archive_day_of_month = 31
        service = BookingService(config=config, repository=self.repo, im=self.im)

        should_run, detail = service.preview_fee_archive(target_date=date(2026, 2, 28))

        assert should_run is True
        assert "归档区间=2026-02-01~2026-02-28（闭区间）" in detail

    def test_archive_meal_fees_updates_table_and_sends_notifications(self) -> None:
        self.repo.list_meal_fee_summaries.return_value = [
            SimpleNamespace(
                open_id="ou_sender",
                total_fee=Decimal("45"),
                lunch_count=2,
                dinner_count=1,
            )
        ]
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_admin"]

        summary = self.service.archive_meal_fees(target_date=date(2026, 2, 15))

        assert summary is not None
        assert summary.start_date == date(2026, 1, 16)
        assert summary.end_date == date(2026, 2, 15)
        assert summary.user_count == 1
        assert summary.total_fee == Decimal("45")
        self.repo.list_meal_fee_summaries.assert_called_once_with(
            start_date=date(2026, 1, 16),
            end_date=date(2026, 2, 15),
        )
        self.repo.upsert_meal_fee_archive_records.assert_called_once()
        upsert_kwargs = self.repo.upsert_meal_fee_archive_records.call_args.kwargs
        assert upsert_kwargs["start_date"] == date(2026, 1, 16)
        assert upsert_kwargs["end_date"] == date(2026, 2, 15)
        archive_records = upsert_kwargs["records"]
        assert len(archive_records) == 1
        assert archive_records[0].open_id == "ou_sender"
        assert archive_records[0].fee == Decimal("45")
        assert archive_records[0].lunch_count == 2
        assert archive_records[0].dinner_count == 1
        self.im.send_text.assert_has_calls(
            [
                call("ou_sender", "餐费归档通知：2026-01-16~2026-02-15，你本月午餐 2 顿，晚餐 1 顿，共 3 顿，餐费合计 45 元。"),
                call("ou_admin", "[管理员] 餐费归档表已更新：2026-01-16~2026-02-15，午餐 2 人次，晚餐 1 人次，总计 3 人次，总收款 45 元。"),
            ],
            any_order=True,
        )
        assert self.im.send_text.call_count == 2

    def test_archive_meal_fees_sends_admin_notice_before_user_notices(self) -> None:
        self.repo.list_meal_fee_summaries.return_value = [
            SimpleNamespace(
                open_id="ou_sender",
                total_fee=Decimal("45"),
                lunch_count=2,
                dinner_count=1,
            )
        ]
        self.repo.list_stats_receiver_open_ids.return_value = ["ou_admin"]

        self.service.archive_meal_fees(target_date=date(2026, 2, 15))

        first_call = self.im.send_text.call_args_list[0]
        assert first_call == call(
            "ou_admin",
            "[管理员] 餐费归档表已更新：2026-01-16~2026-02-15，午餐 2 人次，晚餐 1 人次，总计 3 人次，总收款 45 元。",
        )

    def test_archive_meal_fees_ignores_bot_unavailable_errors_for_user_notifications(self) -> None:
        self.repo.list_meal_fee_summaries.return_value = [
            SimpleNamespace(
                open_id="ou_sender",
                total_fee=Decimal("45"),
                lunch_count=2,
                dinner_count=1,
            )
        ]
        self.repo.list_stats_receiver_open_ids.return_value = []
        self.im.send_text.side_effect = [
            FeishuApiError("im.v1.message.create 调用失败, code=230013, msg=Bot has NO availability to this user., log_id=test"),
        ]

        summary = self.service.archive_meal_fees(target_date=date(2026, 2, 15))

        assert summary is not None
        assert summary.user_count == 1
        self.im.send_text.assert_called_once_with(
            "ou_sender",
            "餐费归档通知：2026-01-16~2026-02-15，你本月午餐 2 顿，晚餐 1 顿，共 3 顿，餐费合计 45 元。",
        )
        assert self.im.send_text.call_count == 1

    def test_archive_meal_fees_skip_when_not_settlement_day(self) -> None:
        result = self.service.archive_meal_fees(target_date=date(2026, 2, 14))

        assert result is None
        self.repo.list_meal_fee_summaries.assert_not_called()
        self.repo.upsert_meal_fee_archive_records.assert_not_called()

    def test_send_card_to_user_today_when_user_missing(self) -> None:
        self.repo.list_user_profiles.return_value = []

        self.service.send_card_to_user_today("ou_missing")

        self.im.send_text.assert_called_once_with("ou_missing", "你不在后台用户列表中，请联系管理员。")

    def test_send_card_to_user_today_when_user_disabled(self) -> None:
        self.repo.list_user_profiles.return_value = [make_user(open_id="ou_sender", enabled=False)]

        self.service.send_card_to_user_today("ou_sender")

        self.im.send_text.assert_called_once_with("ou_sender", "你不在后台用户列表中，请联系管理员。")
        self.im.send_interactive.assert_not_called()

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

        assert response.toast.type == "error"
        assert "已过截止时间" in response.toast.content
        assert "联系管理员人工处理" in response.toast.content

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

        assert response.toast.type == "info"
        assert response.toast.content == "预约已更新"

    def test_schedule_rules_are_always_fetched_from_repository(self) -> None:
        service = BookingService(config=build_config(), repository=self.repo, im=self.im)
        self.repo.list_schedule_rules.side_effect = [
            [],
            [
                MealScheduleRule(
                    start_date=date(2099, 1, 2),
                    end_date=date(2099, 1, 2),
                    meals={Meal.LUNCH},
                )
            ],
            [],
            [],
        ]

        first = service._list_schedule_rules()
        second = service._list_schedule_rules()
        assert first == []
        assert len(second) == 1
        assert self.repo.list_schedule_rules.call_count == 2

        third = service._list_schedule_rules()
        assert third == []
        assert self.repo.list_schedule_rules.call_count == 3

        service._list_schedule_rules(force_refresh=True)
        assert self.repo.list_schedule_rules.call_count == 4
