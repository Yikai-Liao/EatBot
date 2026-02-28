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
from eatbot.services.repositories import BitableRepository, MealFeeSummary


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
                "meal_fee_archive": "tbl_archive",
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
                    "lunch_count": "午餐数",
                    "dinner_count": "晚餐数",
                },
            },
        }
    )


class _FakeBitable:
    def __init__(self, records_by_table: dict[str, list[SimpleNamespace]]) -> None:
        self._records_by_table = records_by_table
        self.updated_records: list[tuple[str, str, dict]] = []
        self.created_records: list[tuple[str, dict]] = []

    def list_records(self, table_id: str) -> list[SimpleNamespace]:
        return list(self._records_by_table.get(table_id, []))

    def update_record(self, table_id: str, record_id: str, fields: dict) -> SimpleNamespace:
        self.updated_records.append((table_id, record_id, fields))
        return SimpleNamespace(record_id=record_id, fields=fields)

    def create_record(self, table_id: str, fields: dict) -> SimpleNamespace:
        self.created_records.append((table_id, fields))
        record_id = f"rec_new_{len(self.created_records)}"
        record = SimpleNamespace(record_id=record_id, fields=fields)
        self._records_by_table.setdefault(table_id, []).append(record)
        return record


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
        "meal_fee_archive": mapping(
            "meal_fee_archive",
            "tbl_archive",
            {
                "user": "用餐者",
                "start_date": "开始日期",
                "end_date": "结束日期",
                "fee": "费用",
                "lunch_count": "午餐数",
                "dinner_count": "晚餐数",
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


def test_cancel_meal_record_only_updates_reservation_status_without_overwriting_price() -> None:
    target_date = date(2026, 2, 14)
    bitable = _FakeBitable(
        {
            "tbl_record": [
                SimpleNamespace(
                    record_id="r1",
                    fields={
                        "日期": "2026-02-14",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "20",
                        "预约状态": True,
                    },
                ),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    kept_id = repo.cancel_meal_record(
        target_date=target_date,
        open_id="ou_1",
        meal=Meal.LUNCH,
        record_id="r1",
        prefer_direct=True,
    )

    assert kept_id == "r1"
    assert bitable.updated_records[-1][0] == "tbl_record"
    assert bitable.updated_records[-1][1] == "r1"
    assert bitable.updated_records[-1][2] == {"预约状态": False}


def test_list_meal_fee_summaries_use_closed_interval_and_later_record() -> None:
    bitable = _FakeBitable(
        {
            "tbl_record": [
                SimpleNamespace(
                    record_id="r1_old",
                    fields={
                        "日期": "2026-01-16",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "20",
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r1_new",
                    fields={
                        "日期": "2026-01-16",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "25",
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r2_old",
                    fields={
                        "日期": "2026-01-20",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.DINNER.value,
                        "价格": "30",
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r2_new",
                    fields={
                        "日期": "2026-01-20",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.DINNER.value,
                        "价格": "0",
                        "预约状态": False,
                    },
                ),
                SimpleNamespace(
                    record_id="r3",
                    fields={
                        "日期": "2026-02-15",
                        "用餐者": [{"id": "ou_1"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "22",
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r4",
                    fields={
                        "日期": "2026-02-01",
                        "用餐者": [{"id": "ou_2"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "18",
                        "预约状态": True,
                    },
                ),
                SimpleNamespace(
                    record_id="r5",
                    fields={
                        "日期": "2026-02-16",
                        "用餐者": [{"id": "ou_2"}],
                        "餐食类型": Meal.LUNCH.value,
                        "价格": "99",
                        "预约状态": True,
                    },
                ),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    summaries = repo.list_meal_fee_summaries(
        start_date=date(2026, 1, 16),
        end_date=date(2026, 2, 15),
    )

    assert summaries == [
        MealFeeSummary(open_id="ou_1", total_fee=Decimal("47"), lunch_count=2, dinner_count=0),
        MealFeeSummary(open_id="ou_2", total_fee=Decimal("18"), lunch_count=1, dinner_count=0),
    ]


def test_upsert_meal_fee_archive_record_update_later_conflict_row() -> None:
    bitable = _FakeBitable(
        {
            "tbl_archive": [
                SimpleNamespace(
                    record_id="a_old",
                    fields={
                        "用餐者": [{"id": "ou_1"}],
                        "开始日期": "2026-01-16",
                        "结束日期": "2026-02-15",
                        "费用": "40",
                        "午餐数": 1,
                        "晚餐数": 1,
                    },
                ),
                SimpleNamespace(
                    record_id="a_new",
                    fields={
                        "用餐者": [{"id": "ou_1"}],
                        "开始日期": "2026-01-16",
                        "结束日期": "2026-02-15",
                        "费用": "41",
                        "午餐数": 1,
                        "晚餐数": 1,
                    },
                ),
            ]
        }
    )
    repo = BitableRepository(config=build_config(), bitable=bitable, mappings=_build_mappings())

    record_id = repo.upsert_meal_fee_archive_record(
        open_id="ou_1",
        start_date=date(2026, 1, 16),
        end_date=date(2026, 2, 15),
        fee=Decimal("45"),
        lunch_count=2,
        dinner_count=3,
    )

    assert record_id == "a_new"
    assert bitable.updated_records[-1][0] == "tbl_archive"
    assert bitable.updated_records[-1][1] == "a_new"
    assert bitable.updated_records[-1][2]["费用"] == "45"
    assert bitable.updated_records[-1][2]["午餐数"] == "2"
    assert bitable.updated_records[-1][2]["晚餐数"] == "3"
