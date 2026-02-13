from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from typing import Any

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    AppTableRecord,
    CreateAppTableRecordRequest,
    ListAppTableFieldRequest,
    ListAppTableRecordRequest,
    UpdateAppTableRecordRequest,
)
from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

from eatbot.config import RuntimeConfig


class FeishuApiError(Exception):
    pass


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FieldMeta:
    field_id: str
    field_name: str
    field_type: int


@dataclass(slots=True)
class TableFieldMapping:
    table_alias: str
    table_id: str
    by_logical_key: dict[str, FieldMeta]


class FeishuFactory:
    @staticmethod
    def build_client(config: RuntimeConfig) -> lark.Client:
        return (
            lark.Client.builder()
            .app_id(config.app_id)
            .app_secret(config.app_secret)
            .log_level(lark.LogLevel.INFO)
            .build()
        )


class BitableAdapter:
    def __init__(self, client: lark.Client, app_token: str) -> None:
        self._client = client
        self._app_token = app_token

    def list_fields(self, table_id: str) -> list[Any]:
        items: list[Any] = []
        page_token: str | None = None

        while True:
            builder = (
                ListAppTableFieldRequest.builder()
                .app_token(self._app_token)
                .table_id(table_id)
                .page_size(500)
            )
            if page_token:
                builder = builder.page_token(page_token)

            response = self._client.bitable.v1.app_table_field.list(builder.build())
            self._ensure_success("bitable.v1.app_table_field.list", response)

            body = response.data
            if body and body.items:
                items.extend(body.items)
            if not body or not body.has_more:
                break
            page_token = body.page_token

        return items

    def list_records(self, table_id: str) -> list[AppTableRecord]:
        items: list[AppTableRecord] = []
        page_token: str | None = None

        while True:
            builder = (
                ListAppTableRecordRequest.builder()
                .app_token(self._app_token)
                .table_id(table_id)
                .page_size(500)
                .user_id_type("open_id")
            )
            if page_token:
                builder = builder.page_token(page_token)

            response = self._client.bitable.v1.app_table_record.list(builder.build())
            self._ensure_success("bitable.v1.app_table_record.list", response)

            body = response.data
            if body and body.items:
                items.extend(body.items)
            if not body or not body.has_more:
                break
            page_token = body.page_token

        return items

    def create_record(self, table_id: str, fields: dict[str, Any]) -> AppTableRecord:
        request = (
            CreateAppTableRecordRequest.builder()
            .app_token(self._app_token)
            .table_id(table_id)
            .user_id_type("open_id")
            .request_body(AppTableRecord.builder().fields(fields).build())
            .build()
        )
        response = self._client.bitable.v1.app_table_record.create(request)
        self._ensure_success("bitable.v1.app_table_record.create", response)
        if response.data is None or response.data.record is None:
            raise FeishuApiError("创建记录失败: response.data.record 为空")
        return response.data.record

    def update_record(self, table_id: str, record_id: str, fields: dict[str, Any]) -> AppTableRecord:
        request = (
            UpdateAppTableRecordRequest.builder()
            .app_token(self._app_token)
            .table_id(table_id)
            .record_id(record_id)
            .user_id_type("open_id")
            .request_body(AppTableRecord.builder().fields(fields).build())
            .build()
        )
        response = self._client.bitable.v1.app_table_record.update(request)
        self._ensure_success("bitable.v1.app_table_record.update", response)
        if response.data is None or response.data.record is None:
            raise FeishuApiError("更新记录失败: response.data.record 为空")
        return response.data.record

    @staticmethod
    def _ensure_success(api_name: str, response: Any) -> None:
        if response.success():
            return
        log_id = response.get_log_id() if hasattr(response, "get_log_id") else ""
        raise FeishuApiError(
            f"{api_name} 调用失败, code={response.code}, msg={response.msg}, log_id={log_id}"
        )


class IMAdapter:
    def __init__(self, client: lark.Client) -> None:
        self._client = client

    def send_text(self, receive_id: str, text: str, receive_id_type: str = "open_id") -> str:
        content = json.dumps({"text": text}, ensure_ascii=False)
        return self._send(receive_id=receive_id, receive_id_type=receive_id_type, msg_type="text", content=content)

    def send_interactive(self, receive_id: str, card_json: str, receive_id_type: str = "open_id") -> str:
        wrapped = json.dumps({"card": card_json}, ensure_ascii=False)
        try:
            return self._send(
                receive_id=receive_id,
                receive_id_type=receive_id_type,
                msg_type="interactive",
                content=wrapped,
            )
        except FeishuApiError as first_error:
            logger.warning("interactive 内容使用 card 包装发送失败，尝试直接发送 card JSON: %s", first_error)
            return self._send(
                receive_id=receive_id,
                receive_id_type=receive_id_type,
                msg_type="interactive",
                content=card_json,
            )

    def _send(self, *, receive_id: str, receive_id_type: str, msg_type: str, content: str) -> str:
        request = (
            CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type(msg_type)
                .content(content)
                .build()
            )
            .build()
        )
        response = self._client.im.v1.message.create(request)
        BitableAdapter._ensure_success("im.v1.message.create", response)
        if response.data is None or response.data.message_id is None:
            raise FeishuApiError("发送消息失败: message_id 为空")
        return response.data.message_id


class FieldMappingResolver:
    def __init__(self, bitable: BitableAdapter) -> None:
        self._bitable = bitable

    def resolve(self, config: RuntimeConfig) -> dict[str, TableFieldMapping]:
        result: dict[str, TableFieldMapping] = {}

        for table_alias, table_id in config.tables.model_dump().items():
            expected = getattr(config.field_names, table_alias).model_dump()
            fields = self._bitable.list_fields(table_id)
            name_to_metas: dict[str, list[FieldMeta]] = {}
            for field in fields:
                meta = FieldMeta(field_id=field.field_id, field_name=field.field_name, field_type=field.type)
                name_to_metas.setdefault(meta.field_name, []).append(meta)

            logical_mapping: dict[str, FieldMeta] = {}
            for logical_key, expected_name in expected.items():
                metas = name_to_metas.get(expected_name, [])
                if not metas:
                    raise FeishuApiError(
                        f"字段名解析失败: table={table_alias}, logical={logical_key}, name={expected_name} 未找到"
                    )
                if len(metas) > 1:
                    raise FeishuApiError(
                        f"字段名解析失败: table={table_alias}, logical={logical_key}, name={expected_name} 出现重复"
                    )
                logical_mapping[logical_key] = metas[0]

            result[table_alias] = TableFieldMapping(
                table_alias=table_alias,
                table_id=table_id,
                by_logical_key=logical_mapping,
            )

        return result
