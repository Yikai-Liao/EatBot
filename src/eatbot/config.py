from __future__ import annotations

from datetime import time
from pathlib import Path
import tomllib
from typing import Any

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
    timezone: str = "Asia/Shanghai"
    send_time: str = "09:00"
    lunch_cutoff: str = "10:30"
    dinner_cutoff: str = "16:30"

    @field_validator("send_time", "lunch_cutoff", "dinner_cutoff")
    @classmethod
    def validate_hhmm(cls, value: str) -> str:
        _parse_hhmm(value)
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
    wiki_token: str | None = None
    tables: TablesConfig
    field_names: FieldNamesConfig
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

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


def _validate_no_duplicate_fields(mapping: dict[str, str], name: str) -> None:
    reverse: dict[str, str] = {}
    for logical_key, field_name in mapping.items():
        if not field_name:
            raise ValueError(f"{name}.{logical_key} 不能为空")
        if field_name in reverse:
            raise ValueError(f"{name} 中字段名重复: {field_name} (keys: {reverse[field_name]}, {logical_key})")
        reverse[field_name] = logical_key
