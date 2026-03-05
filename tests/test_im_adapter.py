from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.feishu_clients import FeishuApiError, IMAdapter


class _FakeResponse:
    def __init__(self, *, ok: bool, code: int = 0, msg: str = "ok", log_id: str = "log") -> None:
        self._ok = ok
        self.code = code
        self.msg = msg
        self._log_id = log_id

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
