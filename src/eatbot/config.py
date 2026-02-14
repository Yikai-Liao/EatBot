from __future__ import annotations

from datetime import time, timedelta
from pathlib import Path
import tomllib
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator


class TablesConfig(BaseModel):
    user_config: str
    meal_schedule: str
    meal_record: str
    stats_receivers: str


class UserConfigFieldNames(BaseModel):
    display_name: str
    user: str
    meal_preference: str
    lunch_price: str
    dinner_price: str
    enabled: str


class MealScheduleFieldNames(BaseModel):
    start_date: str
    end_date: str
    meals: str
    remark: str


class MealRecordFieldNames(BaseModel):
    date: str
    user: str
    meal_type: str
    price: str
    reservation_status: str


class StatsReceiversFieldNames(BaseModel):
    user: str


class FieldNamesConfig(BaseModel):
    user_config: UserConfigFieldNames
    meal_schedule: MealScheduleFieldNames
    meal_record: MealRecordFieldNames
    stats_receivers: StatsReceiversFieldNames


class ScheduleConfig(BaseModel):
    send_time: str = "09:00"
    lunch_cutoff: str = "10:30"
    dinner_cutoff: str = "16:30"
    send_stat_offset: str = "00:00:00"
    schedule_cache_ttl_minutes: int = 30

    @field_validator("send_time", "lunch_cutoff", "dinner_cutoff")
    @classmethod
    def validate_hhmm(cls, value: str) -> str:
        _parse_hhmm(value)
        return value

    @field_validator("send_stat_offset")
    @classmethod
    def validate_send_stat_offset(cls, value: str) -> str:
        _parse_duration_hhmmss(value)
        return value

    @field_validator("schedule_cache_ttl_minutes")
    @classmethod
    def validate_schedule_cache_ttl_minutes(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("schedule_cache_ttl_minutes 必须大于 0")
        return value

    @property
    def send_time_obj(self) -> time:
        return _parse_hhmm(self.send_time)

    @property
    def lunch_cutoff_obj(self) -> time:
        return _parse_hhmm(self.lunch_cutoff)

    @property
    def dinner_cutoff_obj(self) -> time:
        return _parse_hhmm(self.dinner_cutoff)

    @property
    def send_stat_offset_obj(self) -> timedelta:
        return _parse_duration_hhmmss(self.send_stat_offset)

    @property
    def schedule_cache_ttl_obj(self) -> timedelta:
        return timedelta(minutes=self.schedule_cache_ttl_minutes)

    @model_validator(mode="after")
    def validate_stat_schedule_range(self) -> "ScheduleConfig":
        offset_seconds = int(self.send_stat_offset_obj.total_seconds())
        lunch_seconds = self.lunch_cutoff_obj.hour * 3600 + self.lunch_cutoff_obj.minute * 60 + self.lunch_cutoff_obj.second
        dinner_seconds = self.dinner_cutoff_obj.hour * 3600 + self.dinner_cutoff_obj.minute * 60 + self.dinner_cutoff_obj.second
        if lunch_seconds + offset_seconds >= 24 * 3600:
            raise ValueError("lunch_cutoff + send_stat_offset 超出当天范围")
        if dinner_seconds + offset_seconds >= 24 * 3600:
            raise ValueError("dinner_cutoff + send_stat_offset 超出当天范围")
        return self


class LoggingConfig(BaseModel):
    file_path: str = "logs/eatbot.log"
    max_size_mb: int = 20

    @field_validator("file_path")
    @classmethod
    def validate_file_path(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("日志文件路径不能为空")
        return value

    @field_validator("max_size_mb")
    @classmethod
    def validate_max_size_mb(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("日志文件大小上限必须大于 0")
        return value

    @property
    def max_size_bytes(self) -> int:
        return self.max_size_mb * 1024 * 1024


class RuntimeConfig(BaseModel):
    app_id: str
    app_secret: str
    app_token: str
    timezone: str = "Asia/Shanghai"
    wiki_token: str | None = None
    tables: TablesConfig
    field_names: FieldNamesConfig
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_schedule_timezone(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if data.get("timezone"):
            return data
        schedule = data.get("schedule")
        if not isinstance(schedule, dict):
            return data
        legacy_timezone = schedule.get("timezone")
        if not isinstance(legacy_timezone, str) or not legacy_timezone.strip():
            return data
        migrated = dict(data)
        migrated["timezone"] = legacy_timezone
        return migrated

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("timezone 不能为空")
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"timezone 无效: {value}") from exc
        return value

    @model_validator(mode="after")
    def validate_unique_field_names(self) -> "RuntimeConfig":
        _validate_no_duplicate_fields(self.field_names.user_config.model_dump(), "field_names.user_config")
        _validate_no_duplicate_fields(self.field_names.meal_schedule.model_dump(), "field_names.meal_schedule")
        _validate_no_duplicate_fields(self.field_names.meal_record.model_dump(), "field_names.meal_record")
        _validate_no_duplicate_fields(self.field_names.stats_receivers.model_dump(), "field_names.stats_receivers")
        return self


class ConfigError(Exception):
    pass


def load_runtime_config(
    shared_path: str | Path = "config.shared.toml",
    local_path: str | Path = "config.local.toml",
) -> RuntimeConfig:
    shared_file = Path(shared_path)
    local_file = Path(local_path)

    if not shared_file.exists():
        raise ConfigError(f"共享配置不存在: {shared_file}")
    if not local_file.exists():
        raise ConfigError(f"私密配置不存在: {local_file}")

    with shared_file.open("rb") as file:
        shared = tomllib.load(file)
    with local_file.open("rb") as file:
        local = tomllib.load(file)

    merged = _deep_merge(shared, local)

    try:
        return RuntimeConfig.model_validate(merged)
    except Exception as exc:  # pydantic validation errors
        raise ConfigError(f"配置校验失败: {exc}") from exc


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _parse_hhmm(value: str) -> time:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError(f"时间格式错误: {value}")

    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"时间范围错误: {value}")
    return time(hour=hour, minute=minute)


def _parse_duration_hhmmss(value: str) -> timedelta:
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(f"时长格式错误: {value}")

    hour = int(parts[0])
    minute = int(parts[1])
    second = int(parts[2])
    if hour < 0 or minute < 0 or second < 0:
        raise ValueError(f"时长范围错误: {value}")
    if minute > 59 or second > 59:
        raise ValueError(f"时长范围错误: {value}")

    total_seconds = hour * 3600 + minute * 60 + second
    if total_seconds >= 24 * 3600:
        raise ValueError(f"时长超出当天范围: {value}")
    return timedelta(seconds=total_seconds)


def _validate_no_duplicate_fields(mapping: dict[str, str], name: str) -> None:
    reverse: dict[str, str] = {}
    for logical_key, field_name in mapping.items():
        if not field_name:
            raise ValueError(f"{name}.{logical_key} 不能为空")
        if field_name in reverse:
            raise ValueError(f"{name} 中字段名重复: {field_name} (keys: {reverse[field_name]}, {logical_key})")
        reverse[field_name] = logical_key
