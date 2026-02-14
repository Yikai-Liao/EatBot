from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
import time as mono_time
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger
from eatbot.adapters.feishu_clients import BitableAdapter, FeishuApiError, TableFieldMapping
from eatbot.config import RuntimeConfig
from eatbot.domain.decision import parse_meals
from eatbot.domain.models import Meal, MealScheduleRule, UserProfile

@dataclass(slots=True)
class MealRecordRow:
    record_id: str
    target_date: date | None
    open_id: str | None
    meal_type: Meal | None
    reservation_status: bool


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
        self._timezone = ZoneInfo(config.timezone)

    def list_user_profiles(self) -> list[UserProfile]:
        table_id = self._table_id("user_config")
        records = self._bitable.list_records(table_id)
        fields = self._table_fields("user_config")

        users_by_open_id: dict[str, UserProfile] = {}
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
            if open_id in users_by_open_id:
                users_by_open_id.pop(open_id)
            users_by_open_id[open_id] = user

        return list(users_by_open_id.values())

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

        open_ids: dict[str, None] = {}
        for record in records:
            data = record.fields or {}
            open_id = _extract_open_id(data.get(field_name))
            if not open_id:
                continue
            if open_id in open_ids:
                open_ids.pop(open_id)
            open_ids[open_id] = None
        return list(open_ids.keys())

    def upsert_meal_record(
        self,
        *,
        target_date: date,
        open_id: str,
        meal: Meal,
        price: Decimal,
        record_id: str | None = None,
        prefer_direct: bool = False,
    ) -> str:
        started_at = mono_time.monotonic()
        payload = self._meal_payload(
            target_date=target_date,
            open_id=open_id,
            meal=meal,
            price=price,
            reservation_status=True,
        )
        update_payload = self._meal_update_payload(meal=meal, price=price, reservation_status=True)
        table_id = self._table_id("meal_record")

        if prefer_direct:
            if record_id:
                update_started = mono_time.monotonic()
                try:
                    self._bitable.update_record(table_id=table_id, record_id=record_id, fields=update_payload)
                    logger.debug(
                        "meal_record.upsert: mode=direct_update date={} meal={} cost={}ms",
                        target_date.isoformat(),
                        meal.value,
                        int((mono_time.monotonic() - update_started) * 1000),
                    )
                    return record_id
                except FeishuApiError:
                    logger.warning(
                        "meal_record.upsert: direct_update 失败, fallback=create date={} meal={}",
                        target_date.isoformat(),
                        meal.value,
                    )
            create_started = mono_time.monotonic()
            created = self._bitable.create_record(table_id=table_id, fields=payload)
            logger.debug(
                "meal_record.upsert: mode=direct_create date={} meal={} write={}ms total={}ms",
                target_date.isoformat(),
                meal.value,
                int((mono_time.monotonic() - create_started) * 1000),
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return created.record_id

        if record_id:
            update_started = mono_time.monotonic()
            try:
                self._bitable.update_record(table_id=table_id, record_id=record_id, fields=payload)
                logger.debug(
                    "meal_record.upsert: mode=update_by_hint date={} meal={} write={}ms total={}ms",
                    target_date.isoformat(),
                    meal.value,
                    int((mono_time.monotonic() - update_started) * 1000),
                    int((mono_time.monotonic() - started_at) * 1000),
                )
                return record_id
            except FeishuApiError:
                logger.warning(
                    "meal_record.upsert: update_by_hint 失败, fallback=scan date={} meal={}",
                    target_date.isoformat(),
                    meal.value,
                )

        scan_started = mono_time.monotonic()
        rows = self._list_meal_rows(target_date=target_date, open_id=open_id)
        scan_cost = int((mono_time.monotonic() - scan_started) * 1000)
        match = next((row for row in rows if row.meal_type == meal), None)
        if match:
            write_started = mono_time.monotonic()
            self._bitable.update_record(table_id=table_id, record_id=match.record_id, fields=payload)
            logger.debug(
                "meal_record.upsert: mode=scan_update date={} meal={} scan={}ms write={}ms total={}ms",
                target_date.isoformat(),
                meal.value,
                scan_cost,
                int((mono_time.monotonic() - write_started) * 1000),
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return match.record_id

        write_started = mono_time.monotonic()
        created = self._bitable.create_record(table_id=table_id, fields=payload)
        logger.debug(
            "meal_record.upsert: mode=scan_create date={} meal={} scan={}ms write={}ms total={}ms",
            target_date.isoformat(),
            meal.value,
            scan_cost,
            int((mono_time.monotonic() - write_started) * 1000),
            int((mono_time.monotonic() - started_at) * 1000),
        )
        return created.record_id

    def cancel_meal_record(
        self,
        *,
        target_date: date,
        open_id: str,
        meal: Meal,
        record_id: str | None = None,
        prefer_direct: bool = False,
    ) -> str | None:
        started_at = mono_time.monotonic()
        if prefer_direct:
            if not record_id:
                logger.debug(
                    "meal_record.cancel: mode=direct_skip date={} meal={} total={}ms",
                    target_date.isoformat(),
                    meal.value,
                    int((mono_time.monotonic() - started_at) * 1000),
                )
                return None
            payload = self._meal_update_payload(price=Decimal("0"), reservation_status=False)
            write_started = mono_time.monotonic()
            try:
                self._bitable.update_record(
                    table_id=self._table_id("meal_record"),
                    record_id=record_id,
                    fields=payload,
                )
            except FeishuApiError:
                logger.warning(
                    "meal_record.cancel: direct_update 失败, record_id={} date={} meal={}",
                    record_id,
                    target_date.isoformat(),
                    meal.value,
                )
                return None
            logger.debug(
                "meal_record.cancel: mode=direct_update date={} meal={} write={}ms total={}ms",
                target_date.isoformat(),
                meal.value,
                int((mono_time.monotonic() - write_started) * 1000),
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return record_id

        scan_started = mono_time.monotonic()
        rows = self._list_meal_rows(target_date=target_date, open_id=open_id)
        scan_cost = int((mono_time.monotonic() - scan_started) * 1000)
        match = next((row for row in rows if row.meal_type == meal), None)
        if not match and record_id:
            match = next((row for row in rows if row.record_id == record_id), None)

        payload = self._meal_payload(
            target_date=target_date,
            open_id=open_id,
            meal=meal,
            price=Decimal("0"),
            reservation_status=False,
        )
        if match is None:
            if not record_id:
                logger.debug(
                    "meal_record.cancel: mode=scan_skip date={} meal={} scan={}ms total={}ms",
                    target_date.isoformat(),
                    meal.value,
                    scan_cost,
                    int((mono_time.monotonic() - started_at) * 1000),
                )
                return None
            write_started = mono_time.monotonic()
            try:
                self._bitable.update_record(
                    table_id=self._table_id("meal_record"),
                    record_id=record_id,
                    fields=payload,
                )
            except FeishuApiError:
                logger.warning(
                    "meal_record.cancel: scan_fallback_update 失败, record_id={} date={} meal={}",
                    record_id,
                    target_date.isoformat(),
                    meal.value,
                )
                return None
            logger.debug(
                "meal_record.cancel: mode=scan_fallback_update date={} meal={} scan={}ms write={}ms total={}ms",
                target_date.isoformat(),
                meal.value,
                scan_cost,
                int((mono_time.monotonic() - write_started) * 1000),
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return record_id

        target_record_id = record_id or match.record_id
        write_started = mono_time.monotonic()
        self._bitable.update_record(
            table_id=self._table_id("meal_record"),
            record_id=target_record_id,
            fields=payload,
        )
        logger.debug(
            "meal_record.cancel: mode=scan_update date={} meal={} scan={}ms write={}ms total={}ms",
            target_date.isoformat(),
            meal.value,
            scan_cost,
            int((mono_time.monotonic() - write_started) * 1000),
            int((mono_time.monotonic() - started_at) * 1000),
        )
        return target_record_id

    def count_meal_records(self, *, target_date: date, meal: Meal) -> int:
        rows = self._list_meal_rows(target_date=target_date, open_id=None)
        return sum(1 for row in rows if row.meal_type == meal and row.reservation_status)

    def list_user_meal_rows(self, *, target_date: date, open_id: str) -> list[MealRecordRow]:
        return self._list_meal_rows(target_date=target_date, open_id=open_id)

    def _list_meal_rows(self, *, target_date: date, open_id: str | None) -> list[MealRecordRow]:
        table_id = self._table_id("meal_record")
        records = self._bitable.list_records(table_id)
        fields = self._table_fields("meal_record")

        rows_by_key: dict[tuple[str | None, Meal | None], MealRecordRow] = {}
        for record in records:
            data = record.fields or {}
            record_date = _to_date(data.get(fields["date"]), self._timezone)
            if record_date != target_date:
                continue

            record_open_id = _extract_open_id(data.get(fields["user"]))
            if open_id and record_open_id != open_id:
                continue

            meal_type = _to_meal(data.get(fields["meal_type"]))
            reservation_status = _to_checkbox(data.get(fields["reservation_status"]), default=True)
            row = MealRecordRow(
                record_id=record.record_id,
                target_date=record_date,
                open_id=record_open_id,
                meal_type=meal_type,
                reservation_status=reservation_status,
            )
            key = (record_open_id, meal_type)
            if key in rows_by_key:
                rows_by_key.pop(key)
            rows_by_key[key] = row

        return list(rows_by_key.values())

    def _meal_payload(
        self,
        *,
        target_date: date,
        open_id: str,
        meal: Meal,
        price: Decimal,
        reservation_status: bool,
    ) -> dict[str, Any]:
        fields = self._table_fields("meal_record")
        return {
            fields["date"]: _to_date_millis(target_date, self._timezone),
            fields["user"]: [{"id": open_id}],
            fields["meal_type"]: meal.value,
            fields["price"]: self._meal_price_field_value(price),
            fields["reservation_status"]: reservation_status,
        }

    def _meal_update_payload(
        self,
        *,
        meal: Meal | None = None,
        price: Decimal | None = None,
        reservation_status: bool | None = None,
    ) -> dict[str, Any]:
        fields = self._table_fields("meal_record")
        result: dict[str, Any] = {}
        if meal is not None:
            result[fields["meal_type"]] = meal.value
        if price is not None:
            result[fields["price"]] = self._meal_price_field_value(price)
        if reservation_status is not None:
            result[fields["reservation_status"]] = reservation_status
        return result

    def _meal_price_field_value(self, price: Decimal) -> int | float | str:
        field_type = self._mappings["meal_record"].by_logical_key["price"].field_type
        if field_type == 2:
            normalized = price.normalize()
            if normalized == normalized.to_integral():
                return int(normalized)
            return float(normalized)
        return _format_decimal(price)

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
    if isinstance(value, list) and value:
        return _to_meal(value[0])
    return None


def _to_checkbox(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off", ""}:
            return False
        return default
    if isinstance(value, list) and value:
        return _to_checkbox(value[0], default=default)
    return default
