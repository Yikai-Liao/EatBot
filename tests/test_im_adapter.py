from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.feishu_clients import FeishuApiError, IMAdapter


class _FakeResponse:
    def __init__(
        self,
        *,
        ok: bool,
        code: int = 0,
        msg: str = "ok",
        log_id: str = "log",
        data: object | None = None,
    ) -> None:
        self._ok = ok
        self.code = code
        self.msg = msg
        self._log_id = log_id
        self.data = data

    def success(self) -> bool:
        return self._ok

    def get_log_id(self) -> str:
        return self._log_id


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.last_request: SimpleNamespace | None = None

    def request(self, request: SimpleNamespace) -> _FakeResponse:
        self.last_request = request
        return self._response


def test_delay_update_card_uses_raw_card_payload_for_interactive_api() -> None:
    client = _FakeClient(_FakeResponse(ok=True))
    adapter = IMAdapter(client)  # type: ignore[arg-type]
    card_payload = {"schema": "2.0", "body": {"direction": "vertical", "elements": []}}

    adapter.delay_update_card(token="callback-token", card_payload=card_payload, toast_content="同步结束")

    assert client.last_request is not None
    assert client.last_request.uri == "/open-apis/interactive/v1/card/update"
    assert client.last_request.body == {
        "token": "callback-token",
        "card": card_payload,
        "toast": {"type": "info", "content": "同步结束"},
    }


def test_delay_update_card_raises_when_feishu_api_fails() -> None:
    client = _FakeClient(_FakeResponse(ok=False, code=400, msg="bad request"))
    adapter = IMAdapter(client)  # type: ignore[arg-type]

    with pytest.raises(FeishuApiError):
        adapter.delay_update_card(token="callback-token", toast_content="同步结束")


def test_send_image_file_uploads_image_then_sends_image_message(tmp_path: Path) -> None:
    image_file = tmp_path / "付款码.jpeg"
    image_file.write_bytes(b"fake-image")
    client = Mock()
    client.im.v1.image.create.return_value = _FakeResponse(
        ok=True,
        data=SimpleNamespace(image_key="img_v3"),
    )
    client.im.v1.message.create.return_value = _FakeResponse(
        ok=True,
        data=SimpleNamespace(message_id="om_v3"),
    )
    adapter = IMAdapter(client)

    message_id = adapter.send_image_file("ou_sender", image_file)

    assert message_id == "om_v3"
    upload_request = client.im.v1.image.create.call_args.args[0]
    assert upload_request.request_body.image_type == "message"
    send_request = client.im.v1.message.create.call_args.args[0]
    assert send_request.request_body.receive_id == "ou_sender"
    assert send_request.request_body.msg_type == "image"
    assert json.loads(send_request.request_body.content) == {"image_key": "img_v3"}
