from __future__ import annotations

import base64
import http
import time
from typing import Any, Callable

from lark_oapi.card.model import Card
from lark_oapi.core.const import UTF_8
from lark_oapi.core.json import JSON
from lark_oapi.core.log import logger
from lark_oapi.ws.client import Client as BaseWsClient
from lark_oapi.ws.client import _get_by_key
from lark_oapi.ws.const import (
    HEADER_BIZ_RT,
    HEADER_MESSAGE_ID,
    HEADER_SEQ,
    HEADER_SUM,
    HEADER_TRACE_ID,
    HEADER_TYPE,
)
from lark_oapi.ws.enum import MessageType
from lark_oapi.ws.model import Response


class WsClientPatched(BaseWsClient):
    def __init__(
        self,
        app_id: str,
        app_secret: str,
        *,
        card_frame_handler: Callable[[Card], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(app_id=app_id, app_secret=app_secret, **kwargs)
        self._card_frame_handler = card_frame_handler

    async def _handle_data_frame(self, frame):
        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        trace_id = _get_by_key(hs, HEADER_TRACE_ID)
        sum_ = _get_by_key(hs, HEADER_SUM)
        seq = _get_by_key(hs, HEADER_SEQ)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if int(sum_) > 1:
            pl = self._combine(msg_id, int(sum_), int(seq), pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        logger.debug(
            self._fmt_log(
                "receive message, message_type: {}, message_id: {}, trace_id: {}, payload: {}",
                message_type.value,
                msg_id,
                trace_id,
                pl.decode(UTF_8),
            )
        )

        resp = Response(code=http.HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            result = None
            if message_type == MessageType.EVENT:
                result = self._event_handler.do_without_validation(pl)
            elif message_type == MessageType.CARD:
                logger.info(
                    self._fmt_log(
                        "收到 CARD 帧并走兼容处理, message_id: {}, trace_id: {}",
                        msg_id,
                        trace_id,
                    )
                )
                if self._card_frame_handler is not None:
                    card = JSON.unmarshal(pl.decode(UTF_8), Card)
                    result = self._card_frame_handler(card)
            else:
                return

            end = int(round(time.time() * 1000))
            header = hs.add()
            header.key = HEADER_BIZ_RT
            header.value = str(end - start)

            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as exc:
            logger.error(
                self._fmt_log(
                    "handle message failed, message_type: {}, message_id: {}, trace_id: {}, err: {}",
                    message_type.value,
                    msg_id,
                    trace_id,
                    exc,
                )
            )
            resp = Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())
