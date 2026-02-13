from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from eatbot.adapters.feishu_clients import BitableAdapter, TableFieldMapping
from eatbot.config import RuntimeConfig
from eatbot.domain.decision import parse_meals
from eatbot.domain.models import Meal, MealScheduleRule, UserProfile


@dataclass(slots=True)
class MealRecordRow:
    record_id: str
    target_date: date | None
    open_id: str | None
    meal_type: Meal | None


class BitableRepository:
    def __init__(
        self,
        *,
        config: RuntimeConfig,
        bitable: BitableAdapter,
        mappings: dict[str, TableFieldMapping],
    ) -> None:
        self._config = config
        self._bitable = bitable
        self._mappings = mappings
        self._timezone = ZoneInfo(config.schedule.timezone)

    def list_user_profiles(self) -> list[UserProfile]:
        table_id = self._table_id("user_config")
        records = self._bitable.list_records(table_id)
        fields = self._table_fields("user_config")

        users: list[UserProfile] = []
        for record in records:
            data = record.fields or {}
            person_value = data.get(fields["user"])
            open_id = _extract_open_id(person_value)
            if not open_id:
                continue

            display_name = _extract_display_name(data.get(fields["display_name"]))
            if not display_name:
                display_name = _extract_person_name(person_value)
            if not display_name:
                display_name = open_id

            user = UserProfile(
                open_id=open_id,
                display_name=display_name,
                enabled=bool(data.get(fields["enabled"], False)),
                lunch_price=_to_decimal(data.get(fields["lunch_price"])),
                dinner_price=_to_decimal(data.get(fields["dinner_price"])),
                meal_preferences=parse_meals(data.get(fields["meal_preference"])),
            )
            users.append(user)

        return users

    def list_schedule_rules(self) -> list[MealScheduleRule]:
        table_id = self._table_id("meal_schedule")
        records = self._bitable.list_records(table_id)
        fields = self._table_fields("meal_schedule")

        rules: list[MealScheduleRule] = []
        for record in records:
            data = record.fields or {}
            start_date = _to_date(data.get(fields["start_date"]), self._timezone)
            end_date = _to_date(data.get(fields["end_date"]), self._timezone)
            if not start_date or not end_date:
                continue
            if end_date < start_date:
                continue

            meals = parse_meals(data.get(fields["meals"]))
            remark = str(data.get(fields["remark"], "") or "")

            rules.append(
                MealScheduleRule(
                    start_date=start_date,
                    end_date=end_date,
                    meals=meals,
                    remark=remark,
                )
            )

        return rules

    def list_stats_receiver_open_ids(self) -> list[str]:
        table_id = self._table_id("stats_receivers")
        records = self._bitable.list_records(table_id)
        field_name = self._table_fields("stats_receivers")["user"]

        open_ids: list[str] = []
        seen: set[str] = set()
        for record in records:
            data = record.fields or {}
            open_id = _extract_open_id(data.get(field_name))
            if not open_id or open_id in seen:
                continue
            seen.add(open_id)
            open_ids.append(open_id)
        return open_ids

    def upsert_meal_record(self, *, target_date: date, open_id: str, meal: Meal, price: Decimal) -> None:
        rows = self._list_meal_rows(target_date=target_date, open_id=open_id)
        match = next((row for row in rows if row.meal_type == meal), None)
        payload = self._meal_payload(target_date=target_date, open_id=open_id, meal=meal, price=price)

        table_id = self._table_id("meal_record")
        if match:
            self._bitable.update_record(table_id=table_id, record_id=match.record_id, fields=payload)
            return

        cancelled = next((row for row in rows if row.meal_type == Meal.CANCELLED), None)
        if cancelled:
            self._bitable.update_record(table_id=table_id, record_id=cancelled.record_id, fields=payload)
            return

        self._bitable.create_record(table_id=table_id, fields=payload)

    def cancel_meal_record(self, *, target_date: date, open_id: str, meal: Meal) -> None:
        rows = self._list_meal_rows(target_date=target_date, open_id=open_id)
        match = next((row for row in rows if row.meal_type == meal), None)
        if not match:
            return

        payload = self._meal_payload(
            target_date=target_date,
            open_id=open_id,
            meal=Meal.CANCELLED,
            price=Decimal("0"),
        )
        self._bitable.update_record(
            table_id=self._table_id("meal_record"),
            record_id=match.record_id,
            fields=payload,
        )

    def count_meal_records(self, *, target_date: date, meal: Meal) -> int:
        rows = self._list_meal_rows(target_date=target_date, open_id=None)
        return sum(1 for row in rows if row.meal_type == meal)

    def _list_meal_rows(self, *, target_date: date, open_id: str | None) -> list[MealRecordRow]:
        table_id = self._table_id("meal_record")
        records = self._bitable.list_records(table_id)
        fields = self._table_fields("meal_record")

        rows: list[MealRecordRow] = []
        for record in records:
            data = record.fields or {}
            record_date = _to_date(data.get(fields["date"]), self._timezone)
            if record_date != target_date:
                continue

            record_open_id = _extract_open_id(data.get(fields["user"]))
            if open_id and record_open_id != open_id:
                continue

            meal_type = _to_meal(data.get(fields["meal_type"]))
            rows.append(
                MealRecordRow(
                    record_id=record.record_id,
                    target_date=record_date,
                    open_id=record_open_id,
                    meal_type=meal_type,
                )
            )

        return rows

    def _meal_payload(self, *, target_date: date, open_id: str, meal: Meal, price: Decimal) -> dict[str, Any]:
        fields = self._table_fields("meal_record")
        return {
            fields["date"]: _to_date_millis(target_date, self._timezone),
            fields["user"]: [{"id": open_id}],
            fields["meal_type"]: meal.value,
            fields["price"]: _format_decimal(price),
        }

    def _table_id(self, table_alias: str) -> str:
        return self._mappings[table_alias].table_id

    def _table_fields(self, table_alias: str) -> dict[str, str]:
        mapping = self._mappings[table_alias].by_logical_key
        return {logical_key: meta.field_name for logical_key, meta in mapping.items()}


def _extract_open_id(value: object) -> str | None:
    if not isinstance(value, list) or not value:
        return None
    first = value[0]
    if not isinstance(first, dict):
        return None
    raw = first.get("id") or first.get("open_id")
    if not raw:
        return None
    return str(raw)


def _extract_person_name(value: object) -> str | None:
    if not isinstance(value, list) or not value:
        return None
    first = value[0]
    if not isinstance(first, dict):
        return None
    raw = first.get("name")
    if not raw:
        return None
    return str(raw)


def _extract_display_name(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    users = value.get("users")
    if not isinstance(users, list) or not users:
        return None
    first = users[0]
    if not isinstance(first, dict):
        return None
    raw = first.get("name")
    if not raw:
        return None
    return str(raw)


def _to_date(value: object, tz: ZoneInfo) -> date | None:
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return _to_date(int(text), tz)
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz).date()

    if isinstance(value, list) and value:
        return _to_date(value[0], tz)

    return None


def _to_date_millis(target_date: date, tz: ZoneInfo) -> int:
    dt = datetime.combine(target_date, time.min, tzinfo=tz)
    return int(dt.timestamp() * 1000)


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if not text:
        return "0"
    return text


def _to_meal(value: object) -> Meal | None:
    if value == Meal.LUNCH.value:
        return Meal.LUNCH
    if value == Meal.DINNER.value:
        return Meal.DINNER
    if value == Meal.CANCELLED.value:
        return Meal.CANCELLED
    return None
