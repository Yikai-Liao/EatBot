from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.feishu_clients import FeishuApiError, FieldMappingResolver
from eatbot.config import RuntimeConfig


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
                },
            },
        }
    )


class _FakeBitable:
    def __init__(self, table_fields: dict[str, list[SimpleNamespace]]) -> None:
        self._table_fields = table_fields

    def list_fields(self, table_id: str) -> list[SimpleNamespace]:
        return self._table_fields[table_id]


def test_resolve_success_when_price_types_are_same() -> None:
    config = build_config()
    bitable = _FakeBitable(
        {
            "tbl_user": [
                SimpleNamespace(field_id="f1", field_name="用餐人员名称", type=20),
                SimpleNamespace(field_id="f2", field_name="人员", type=11),
                SimpleNamespace(field_id="f3", field_name="餐食偏好", type=4),
                SimpleNamespace(field_id="f4", field_name="午餐单价", type=2),
                SimpleNamespace(field_id="f5", field_name="晚餐单价", type=2),
                SimpleNamespace(field_id="f6", field_name="启用", type=7),
            ],
            "tbl_schedule": [
                SimpleNamespace(field_id="f7", field_name="开始日期", type=5),
                SimpleNamespace(field_id="f8", field_name="截止日期", type=5),
                SimpleNamespace(field_id="f9", field_name="当日餐食包含", type=4),
                SimpleNamespace(field_id="f10", field_name="备注", type=1),
            ],
            "tbl_record": [
                SimpleNamespace(field_id="f11", field_name="日期", type=5),
                SimpleNamespace(field_id="f12", field_name="用餐者", type=11),
                SimpleNamespace(field_id="f13", field_name="餐食类型", type=3),
                SimpleNamespace(field_id="f14", field_name="价格", type=2),
                SimpleNamespace(field_id="f15", field_name="预约状态", type=7),
            ],
            "tbl_stats": [
                SimpleNamespace(field_id="f16", field_name="人员", type=11),
            ],
            "tbl_archive": [
                SimpleNamespace(field_id="f17", field_name="用餐者", type=11),
                SimpleNamespace(field_id="f18", field_name="开始日期", type=5),
                SimpleNamespace(field_id="f19", field_name="结束日期", type=5),
                SimpleNamespace(field_id="f20", field_name="费用", type=2),
                SimpleNamespace(field_id="f21", field_name="午餐数", type=2),
                SimpleNamespace(field_id="f22", field_name="晚餐数", type=2),
            ],
        }
    )

    result = FieldMappingResolver(bitable).resolve(config)

    assert "user_config" in result
    assert "meal_record" in result
    assert "meal_fee_archive" in result


def test_resolve_raise_when_price_types_mismatch() -> None:
    config = build_config()
    bitable = _FakeBitable(
        {
            "tbl_user": [
                SimpleNamespace(field_id="f1", field_name="用餐人员名称", type=20),
                SimpleNamespace(field_id="f2", field_name="人员", type=11),
                SimpleNamespace(field_id="f3", field_name="餐食偏好", type=4),
                SimpleNamespace(field_id="f4", field_name="午餐单价", type=2),
                SimpleNamespace(field_id="f5", field_name="晚餐单价", type=1),
                SimpleNamespace(field_id="f6", field_name="启用", type=7),
            ],
            "tbl_schedule": [
                SimpleNamespace(field_id="f7", field_name="开始日期", type=5),
                SimpleNamespace(field_id="f8", field_name="截止日期", type=5),
                SimpleNamespace(field_id="f9", field_name="当日餐食包含", type=4),
                SimpleNamespace(field_id="f10", field_name="备注", type=1),
            ],
            "tbl_record": [
                SimpleNamespace(field_id="f11", field_name="日期", type=5),
                SimpleNamespace(field_id="f12", field_name="用餐者", type=11),
                SimpleNamespace(field_id="f13", field_name="餐食类型", type=3),
                SimpleNamespace(field_id="f14", field_name="价格", type=2),
                SimpleNamespace(field_id="f15", field_name="预约状态", type=7),
            ],
            "tbl_stats": [
                SimpleNamespace(field_id="f16", field_name="人员", type=11),
            ],
            "tbl_archive": [
                SimpleNamespace(field_id="f17", field_name="用餐者", type=11),
                SimpleNamespace(field_id="f18", field_name="开始日期", type=5),
                SimpleNamespace(field_id="f19", field_name="结束日期", type=5),
                SimpleNamespace(field_id="f20", field_name="费用", type=2),
                SimpleNamespace(field_id="f21", field_name="午餐数", type=2),
                SimpleNamespace(field_id="f22", field_name="晚餐数", type=2),
            ],
        }
    )

    with pytest.raises(FeishuApiError):
        FieldMappingResolver(bitable).resolve(config)
