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

from eatbot.app import _parse_cli_date, configure_logging, cli, list_cron_trigger_events
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


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_parse_cli_date_argument() -> None:
    parsed = _parse_cli_date("2026-02-14", "--date")
    assert parsed == date(2026, 2, 14)


def test_parse_cli_date_argument_invalid() -> None:
    with pytest.raises(BadParameter):
        _parse_cli_date("2026-02-31", "--date")


def test_list_cron_trigger_events_window_boundaries() -> None:
    schedule = ScheduleConfig()
    tz = ZoneInfo(schedule.timezone)
    start_at = datetime(2026, 2, 14, 9, 0, tzinfo=tz)
    end_at = datetime(2026, 2, 14, 10, 30, tzinfo=tz)

    events = list_cron_trigger_events(schedule, start_at=start_at, end_at=end_at)

    assert [event.spec.job_id for event in events] == ["daily_send_cards", "daily_lunch_stats"]


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
        mocked_load_config.return_value = build_runtime_config()

        result = runner.invoke(
            cli,
            ["dev", "cron", "--from", "2026-02-14T09:00", "--to", "2026-02-14T10:30"],
        )

    assert result.exit_code == 0, result.output
    assert "daily_send_cards" in result.output
    assert "daily_lunch_stats" in result.output


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
