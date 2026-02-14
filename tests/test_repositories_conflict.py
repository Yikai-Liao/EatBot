from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.feishu_clients import FieldMeta, TableFieldMapping
from eatbot.config import RuntimeConfig
from eatbot.domain.models import Meal
from eatbot.services.repositories import BitableRepository


def build_config() -> RuntimeConfig:
    return RuntimeConfig.model_validate(
        {
            "app_id": "id",
            "app_secret": "secret",
            "app_token": "app",
            "tables": {
                "user_config": "tbl_user",
                "meal_schedule": "tbl_schedule",
                "meal_record": "tbl_record",
                "stats_receivers": "tbl_stats",
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


class _FakeBitable:
    def __init__(self, records_by_table: dict[str, list[SimpleNamespace]]) -> None:
        self._records_by_table = records_by_table

    def list_records(self, table_id: str) -> list[SimpleNamespace]:
        return list(self._records_by_table.get(table_id, []))


def _build_mappings() -> dict[str, TableFieldMapping]:
    def mapping(table_alias: str, table_id: str, fields: dict[str, str]) -> TableFieldMapping:
        return TableFieldMapping(
            table_alias=table_alias,
            table_id=table_id,
            by_logical_key={
                key: FieldMeta(field_id=f"f_{table_alias}_{key}", field_name=name, field_type=1)
                for key, name in fields.items()
            },
        )

    return {
        "user_config": mapping(
            "user_config",
            "tbl_user",
            {
                "display_name": "用餐人员名称",
                "user": "人员",
                "meal_preference": "餐食偏好",
                "lunch_price": "午餐单价",
                "dinner_price": "晚餐单价",
                "enabled": "启用",
            },
        ),
        "meal_schedule": mapping(
            "meal_schedule",
            "tbl_schedule",
            {
                "start_date": "开始日期",
                "end_date": "截止日期",
                "meals": "当日餐食包含",
                "remark": "备注",
            },
        ),
        "meal_record": mapping(
            "meal_record",
            "tbl_record",
            {
                "date": "日期",
                "user": "用餐者",
                "meal_type": "餐食类型",
                "price": "价格",
                "reservation_status": "预约状态",
            },
        ),
        "stats_receivers": mapping(
            "stats_receivers",
            "tbl_stats",
            {
                "user": "人员",
            },
        ),
    }


def test_list_user_profiles_conflict_uses_later_record() -> None:
    bitable = _FakeBitable(
        {
            "tbl_user": [
                SimpleNamespace(
                    record_id="u1_old",
                    fields={
                        "人员": [{"id": "ou_1", "name": "A"}],
                        "用餐人员名称": {"users": [{"name": "旧名字"}]},
                        "餐食偏好": [Meal.LUNCH.value],
                        "午餐单价": "20",
                        "晚餐单价": "25",
                        "启用": True,
                    },
                ),
                SimpleNamespace(
                    record_id="u1_new",
                    fields={
                        "人员": [{"id": "ou_1", "name": "A"}],
                        "用餐人员名称": {"users": [{"name": "新名字"}]},
                        "餐食偏好": [Meal.DINNER.value],
                        "午餐单价": "22",
                        "晚餐单价": "26",
                        "启用": False,
                    },
                ),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    users = repo.list_user_profiles()

    assert len(users) == 1
    user = users[0]
    assert user.open_id == "ou_1"
    assert user.display_name == "新名字"
    assert user.enabled is False
    assert user.lunch_price == Decimal("22")
    assert user.dinner_price == Decimal("26")
    assert user.meal_preferences == {Meal.DINNER}


def test_list_stats_receivers_conflict_uses_later_order() -> None:
    bitable = _FakeBitable(
        {
            "tbl_stats": [
                SimpleNamespace(record_id="s1", fields={"人员": [{"id": "ou_1"}]}),
                SimpleNamespace(record_id="s2", fields={"人员": [{"id": "ou_2"}]}),
                SimpleNamespace(record_id="s3", fields={"人员": [{"id": "ou_1"}]}),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    open_ids = repo.list_stats_receiver_open_ids()

    assert open_ids == ["ou_2", "ou_1"]


def test_meal_rows_conflict_use_later_record_for_user_and_stats() -> None:
    target_date = date(2026, 2, 14)
    bitable = _FakeBitable(
        {
            "tbl_record": [
                SimpleNamespace(
                    record_id="r_old",
                    fields={
                        "日期": "2026-02-14",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r_new",
                    fields={
                        "日期": "2026-02-14",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "预约状态": False,
                    },
                ),
                SimpleNamespace(
                    record_id="r_other",
                    fields={
                        "日期": "2026-02-14",
                        "用餐者": [{"id": "ou_2"}],
                        "餐食类型": Meal.LUNCH.value,
                        "预约状态": True,
                    },
                ),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    rows = repo.list_user_meal_rows(target_date=target_date, open_id="ou_1")
    count = repo.count_meal_records(target_date=target_date, meal=Meal.LUNCH)

    assert len(rows) == 1
    assert rows[0].record_id == "r_new"
    assert rows[0].reservation_status is False
    assert count == 1
