from __future__ import annotations

import asyncio
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.adapters.ws_client import WsClientPatched


def test_ws_client_connect_disables_proxy_auto_discovery() -> None:
    calls: list[tuple[str, object]] = []

    async def fake_connect(url: str, **kwargs: object) -> object:
        calls.append((url, kwargs.get("proxy")))
        return object()

    async def run() -> None:
        client = WsClientPatched("app_id", "app_secret")
        client._get_conn_url = lambda: "wss://example.com/ws?device_id=d1&service_id=s1"  # type: ignore[method-assign]

        def fake_create_task(coro: object) -> None:
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            return None

        with patch("eatbot.adapters.ws_client.websockets.connect", side_effect=fake_connect):
            with patch("eatbot.adapters.ws_client.loop.create_task", side_effect=fake_create_task):
                await client._connect()

        assert client._conn is not None
        assert client._conn_id == "d1"
        assert client._service_id == "s1"

    asyncio.run(run())

    assert calls == [("wss://example.com/ws?device_id=d1&service_id=s1", None)]
