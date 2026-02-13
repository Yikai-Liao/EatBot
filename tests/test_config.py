from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.config import ConfigError, load_runtime_config


class ConfigTests(unittest.TestCase):
    def test_load_and_merge_shared_local(self) -> None:
        shared = textwrap.dedent(
            """
            app_token = "app"
            wiki_token = "wiki"

            [tables]
            user_config = "t1"
            meal_schedule = "t2"
            meal_record = "t3"
            stats_receivers = "t4"

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

            [field_names.stats_receivers]
            user = "A"
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
            self.assertEqual(config.app_id, "id")
            self.assertEqual(config.app_secret, "secret")
            self.assertEqual(config.app_token, "app")
            self.assertEqual(config.schedule.timezone, "Asia/Shanghai")

    def test_duplicate_field_names_raise_error(self) -> None:
        shared = textwrap.dedent(
            """
            app_token = "app"

            [tables]
            user_config = "t1"
            meal_schedule = "t2"
            meal_record = "t3"
            stats_receivers = "t4"

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

            [field_names.stats_receivers]
            user = "A"
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

            with self.assertRaises(ConfigError):
                load_runtime_config(shared_file, local_file)


if __name__ == "__main__":
    unittest.main()
