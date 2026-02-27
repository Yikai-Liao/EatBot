from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import sys
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
from loguru import logger
from click.exceptions import BadParameter
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.app import _parse_cli_date, _parse_cli_datetime, build_cron_job_specs, configure_logging, cli, list_cron_trigger_events
from eatbot.config import RuntimeConfig, ScheduleConfig


def build_runtime_config() -> RuntimeConfig:
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


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_parse_cli_date_argument() -> None:
    parsed = _parse_cli_date("2026-02-14", "--date")
    assert parsed == date(2026, 2, 14)


def test_parse_cli_date_argument_invalid() -> None:
    with pytest.raises(BadParameter):
        _parse_cli_date("2026-02-31", "--date")


def test_parse_cli_datetime_accept_seconds() -> None:
    parsed = _parse_cli_datetime("2026-02-14T09:00:30", "--from")
    assert parsed == datetime(2026, 2, 14, 9, 0, 30)


def test_list_cron_trigger_events_window_boundaries() -> None:
    runtime_config = build_runtime_config()
    schedule = ScheduleConfig()
    tz = ZoneInfo(runtime_config.timezone)
    start_at = datetime(2026, 2, 14, 9, 0, tzinfo=tz)
    end_at = datetime(2026, 2, 14, 10, 30, tzinfo=tz)

    events = list_cron_trigger_events(schedule, start_at=start_at, end_at=end_at)

    assert [event.spec.job_id for event in events] == ["daily_send_cards", "daily_lunch_stats"]


def test_build_cron_job_specs_applies_stat_offset_with_seconds() -> None:
    schedule = ScheduleConfig(
        send_time="09:00",
        lunch_cutoff="10:30",
        dinner_cutoff="16:30",
        send_stat_offset="00:00:30",
    )

    specs = build_cron_job_specs(schedule)
    by_job = {item.job_id: item for item in specs}

    assert by_job["daily_lunch_stats"].hour == 10
    assert by_job["daily_lunch_stats"].minute == 30
    assert by_job["daily_lunch_stats"].second == 30
    assert by_job["daily_dinner_stats"].hour == 16
    assert by_job["daily_dinner_stats"].minute == 30
    assert by_job["daily_dinner_stats"].second == 30


def test_build_cron_job_specs_include_fee_archive_job() -> None:
    schedule = ScheduleConfig(fee_archive_time="21:05")

    specs = build_cron_job_specs(schedule)
    by_job = {item.job_id: item for item in specs}

    assert "daily_fee_archive" in by_job
    assert by_job["daily_fee_archive"].hour == 21
    assert by_job["daily_fee_archive"].minute == 5
    assert by_job["daily_fee_archive"].second == 0


def test_send_cards_command_passes_date(runner: CliRunner) -> None:
    with patch("eatbot.app._bootstrap_application") as mocked_bootstrap:
        app = Mock()
        mocked_bootstrap.return_value = app

        result = runner.invoke(cli, ["send", "cards", "--date", "2026-02-14"])

    assert result.exit_code == 0, result.output
    app.send_cards_once.assert_called_once_with(target_date=date(2026, 2, 14))


def test_root_without_subcommand_runs_service(runner: CliRunner) -> None:
    with patch("eatbot.app._bootstrap_application") as mocked_bootstrap:
        app = Mock()
        mocked_bootstrap.return_value = app

        result = runner.invoke(cli, [])

    assert result.exit_code == 0, result.output
    app.run.assert_called_once()


def test_send_stats_command_all(runner: CliRunner) -> None:
    with patch("eatbot.app._bootstrap_application") as mocked_bootstrap:
        app = Mock()
        mocked_bootstrap.return_value = app

        result = runner.invoke(cli, ["send", "stats", "--meal", "all", "--date", "2026-02-14"])

    assert result.exit_code == 0, result.output
    app.send_stats_once.assert_called_once_with(target_date=date(2026, 2, 14), meal=None)


def test_dev_cron_preview(runner: CliRunner) -> None:
    with patch("eatbot.app._load_runtime_config_or_exit") as mocked_load_config:
        with patch("eatbot.app._bootstrap_application") as mocked_bootstrap:
            app = Mock()
            snapshot = Mock(schedule_rules_count=3, enabled_user_count=5, stats_receiver_count=2)
            app.build_cron_preview_snapshot.return_value = snapshot
            app.preview_cron_action.side_effect = [
                Mock(will_execute=False, detail="date=2026-02-14(周六); 规则结果=不发送"),
                Mock(will_execute=True, detail="date=2026-02-14(周六); 餐次=午餐; 统计接收人=2"),
            ]
            mocked_load_config.return_value = build_runtime_config()
            mocked_bootstrap.return_value = app

            result = runner.invoke(
                cli,
                ["dev", "cron", "--from", "2026-02-14T09:00", "--to", "2026-02-14T10:30"],
            )

    assert result.exit_code == 0, result.output
    assert "窗口任务数: 2" in result.output
    assert "daily_send_cards [跳过]" in result.output
    assert "daily_lunch_stats [执行]" in result.output


def test_run_command_accepts_log_level_option(runner: CliRunner) -> None:
    with patch("eatbot.app._load_runtime_config_or_exit") as mocked_load_config:
        with patch("eatbot.app._bootstrap_application") as mocked_bootstrap:
            app = Mock()
            mocked_load_config.return_value = build_runtime_config()
            mocked_bootstrap.return_value = app

            result = runner.invoke(cli, ["run", "--log-level", "debug"])

    assert result.exit_code == 0, result.output
    app.run.assert_called_once()


def test_configure_logging_adds_console_and_file_sink(tmp_path: Path) -> None:
    log_file = tmp_path / "eatbot.log"
    configure_logging(level="INFO", file_path=str(log_file), file_max_size_bytes=1024)
    logger.info("hello file sink")
    logger.remove()
    configure_logging(level="INFO")
    content = log_file.read_text(encoding="utf-8")
    assert "hello file sink" in content
