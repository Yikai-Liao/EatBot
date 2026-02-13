from __future__ import annotations

import argparse
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from eatbot.adapters.feishu_clients import BitableAdapter, FeishuFactory, FieldMappingResolver, IMAdapter
from eatbot.config import ConfigError, load_runtime_config
from eatbot.domain.models import Meal
from eatbot.services.booking import BookingService
from eatbot.services.repositories import BitableRepository


logger = logging.getLogger(__name__)


class EatBotApplication:
    def __init__(self) -> None:
        self._config = None
        self._booking: BookingService | None = None
        self._scheduler: BackgroundScheduler | None = None

    def bootstrap(self) -> None:
        self._config = load_runtime_config()

        client = FeishuFactory.build_client(self._config)
        bitable = BitableAdapter(client=client, app_token=self._config.app_token)
        mappings = FieldMappingResolver(bitable).resolve(self._config)

        repository = BitableRepository(config=self._config, bitable=bitable, mappings=mappings)
        im = IMAdapter(client)
        self._booking = BookingService(config=self._config, repository=repository, im=im)

        logger.info("配置与字段映射校验通过")

    def run(self) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        self._start_scheduler()

        handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._on_message)
            .register_p2_card_action_trigger(self._on_card_action)
            .build()
        )

        ws_client = lark.ws.Client(
            self._config.app_id,
            self._config.app_secret,
            event_handler=handler,
            log_level=lark.LogLevel.INFO,
        )
        logger.info("长连接已启动")
        ws_client.start()

    def send_today_once(self) -> None:
        if self._booking is None:
            raise RuntimeError("应用未初始化")
        self._booking.send_daily_cards()

    def _start_scheduler(self) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        if self._scheduler is not None:
            return

        tz = ZoneInfo(self._config.schedule.timezone)
        scheduler = BackgroundScheduler(timezone=tz)

        send_time = self._config.schedule.send_time_obj
        scheduler.add_job(
            self._booking.send_daily_cards,
            trigger="cron",
            hour=send_time.hour,
            minute=send_time.minute,
            id="daily_send_cards",
            replace_existing=True,
        )

        lunch_time = self._config.schedule.lunch_cutoff_obj
        scheduler.add_job(
            lambda: self._booking and self._booking.send_stats(datetime.now(tz).date(), Meal.LUNCH),
            trigger="cron",
            hour=lunch_time.hour,
            minute=lunch_time.minute,
            id="daily_lunch_stats",
            replace_existing=True,
        )

        dinner_time = self._config.schedule.dinner_cutoff_obj
        scheduler.add_job(
            lambda: self._booking and self._booking.send_stats(datetime.now(tz).date(), Meal.DINNER),
            trigger="cron",
            hour=dinner_time.hour,
            minute=dinner_time.minute,
            id="daily_dinner_stats",
            replace_existing=True,
        )

        scheduler.start()
        self._scheduler = scheduler

        logger.info(
            "定时任务已启动: send=%s, lunch_cutoff=%s, dinner_cutoff=%s",
            self._config.schedule.send_time,
            self._config.schedule.lunch_cutoff,
            self._config.schedule.dinner_cutoff,
        )

    def _on_message(self, data: P2ImMessageReceiveV1) -> None:
        if self._booking is None:
            return
        try:
            self._booking.handle_message_event(data)
        except Exception as exc:
            logger.exception("处理消息事件失败: %s", exc)

    def _on_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        if self._booking is None:
            return P2CardActionTriggerResponse({"toast": {"type": "error", "content": "服务未初始化"}})
        return self._booking.handle_card_action(data)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="EatBot")
    parser.add_argument("--check", action="store_true", help="仅做配置和字段映射校验")
    parser.add_argument("--send-today", action="store_true", help="立即发送今天的预约卡片后退出")
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    app = EatBotApplication()
    try:
        app.bootstrap()
    except ConfigError as exc:
        logger.error(str(exc))
        raise SystemExit(1) from exc

    if args.check:
        logger.info("校验成功")
        return

    if args.send_today:
        app.send_today_once()
        logger.info("今日卡片发送完成")
        return

    app.run()


if __name__ == "__main__":
    main()
