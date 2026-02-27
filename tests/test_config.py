from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import textwrap

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.config import ConfigError, load_runtime_config


def test_load_and_merge_shared_local() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"
        wiki_token = "wiki"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        config = load_runtime_config(shared_file, local_file)
        assert config.app_id == "id"
        assert config.app_secret == "secret"
        assert config.app_token == "app"
        assert config.timezone == "Asia/Shanghai"
        assert config.logging.file_path == "logs/eatbot.log"
        assert config.logging.max_size_mb == 20


def test_duplicate_field_names_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "A"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_invalid_logging_max_size_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"

        [logging]
        file_path = "logs/eatbot.log"
        max_size_mb = 0
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_legacy_schedule_timezone_is_migrated() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"

        [schedule]
        timezone = "UTC"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        config = load_runtime_config(shared_file, local_file)
        assert config.timezone == "UTC"


def test_invalid_timezone_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"
        timezone = "Mars/Phobos"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_invalid_send_stat_offset_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"

        [schedule]
        send_stat_offset = "00:61:00"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_invalid_schedule_cache_ttl_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"

        [schedule]
        schedule_cache_ttl_minutes = 0
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_invalid_fee_archive_day_of_month_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"

        [schedule]
        fee_archive_day_of_month = 32
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)


def test_invalid_fee_archive_time_raise_error() -> None:
    shared = textwrap.dedent(
        """
        app_token = "app"

        [tables]
        user_config = "t1"
        meal_schedule = "t2"
        meal_record = "t3"
        stats_receivers = "t4"
        meal_fee_archive = "t5"

        [field_names.user_config]
        display_name = "A"
        user = "B"
        meal_preference = "C"
        lunch_price = "D"
        dinner_price = "E"
        enabled = "F"

        [field_names.meal_schedule]
        start_date = "A"
        end_date = "B"
        meals = "C"
        remark = "D"

        [field_names.meal_record]
        date = "A"
        user = "B"
        meal_type = "C"
        price = "D"
        reservation_status = "E"

        [field_names.stats_receivers]
        user = "A"

        [field_names.meal_fee_archive]
        user = "用餐者"
        start_date = "开始日期"
        end_date = "结束日期"
        fee = "费用"

        [schedule]
        fee_archive_time = "24:00"
        """
    ).strip()
    local = textwrap.dedent(
        """
        app_id = "id"
        app_secret = "secret"
        """
    ).strip()

    with tempfile.TemporaryDirectory() as tmp:
        shared_file = Path(tmp) / "config.shared.toml"
        local_file = Path(tmp) / "config.local.toml"
        shared_file.write_text(shared, encoding="utf-8")
        local_file.write_text(local, encoding="utf-8")

        with pytest.raises(ConfigError):
            load_runtime_config(shared_file, local_file)
