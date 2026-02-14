from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path
import sys
from typing import Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)
import typer
from loguru import logger

from eatbot.adapters.feishu_clients import BitableAdapter, FeishuFactory, FieldMappingResolver, IMAdapter
from eatbot.adapters.ws_client import WsClientPatched
from eatbot.config import ConfigError, RuntimeConfig, ScheduleConfig, load_runtime_config
from eatbot.domain.models import Meal
from eatbot.services.booking import BookingService
from eatbot.services.repositories import BitableRepository


class CronAction(StrEnum):
    SEND_CARDS = "send_cards"
    LUNCH_STATS = "lunch_stats"
    DINNER_STATS = "dinner_stats"


class StatsMealOption(StrEnum):
    LUNCH = "lunch"
    DINNER = "dinner"
    ALL = "all"


class LogLevelOption(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(slots=True, frozen=True)
class CronJobSpec:
    job_id: str
    action: CronAction
    hour: int
    minute: int


@dataclass(slots=True, frozen=True)
class CronTriggerEvent:
    trigger_at: datetime
    spec: CronJobSpec


def build_cron_job_specs(schedule: ScheduleConfig) -> list[CronJobSpec]:
    send_time = schedule.send_time_obj
    lunch_time = schedule.lunch_cutoff_obj
    dinner_time = schedule.dinner_cutoff_obj

    return [
        CronJobSpec(
            job_id="daily_send_cards",
            action=CronAction.SEND_CARDS,
            hour=send_time.hour,
            minute=send_time.minute,
        ),
        CronJobSpec(
            job_id="daily_lunch_stats",
            action=CronAction.LUNCH_STATS,
            hour=lunch_time.hour,
            minute=lunch_time.minute,
        ),
        CronJobSpec(
            job_id="daily_dinner_stats",
            action=CronAction.DINNER_STATS,
            hour=dinner_time.hour,
            minute=dinner_time.minute,
        ),
    ]


def list_cron_trigger_events(
    schedule: ScheduleConfig,
    *,
    start_at: datetime,
    end_at: datetime,
) -> list[CronTriggerEvent]:
    if end_at < start_at:
        raise ValueError("end_at 必须大于等于 start_at")

    events: list[CronTriggerEvent] = []
    job_specs = build_cron_job_specs(schedule)
    current_date = start_at.date()
    end_date = end_at.date()

    while current_date <= end_date:
        for spec in job_specs:
            trigger_at = datetime.combine(
                current_date,
                time(hour=spec.hour, minute=spec.minute),
                tzinfo=start_at.tzinfo,
            )
            if start_at <= trigger_at <= end_at:
                events.append(CronTriggerEvent(trigger_at=trigger_at, spec=spec))
        current_date += timedelta(days=1)

    events.sort(key=lambda event: (event.trigger_at, event.spec.job_id))
    return events


class EatBotApplication:
    def __init__(
        self,
        *,
        now_provider: Callable[[], datetime] | None = None,
        enable_scheduler: bool = True,
    ) -> None:
        self._config: RuntimeConfig | None = None
        self._booking: BookingService | None = None
        self._scheduler: BackgroundScheduler | None = None
        self._now_provider = now_provider
        self._enable_scheduler = enable_scheduler

    def bootstrap(self, runtime_config: RuntimeConfig | None = None) -> None:
        self._config = runtime_config or load_runtime_config()

        client = FeishuFactory.build_client(self._config)
        bitable = BitableAdapter(client=client, app_token=self._config.app_token)
        mappings = FieldMappingResolver(bitable).resolve(self._config)

        repository = BitableRepository(config=self._config, bitable=bitable, mappings=mappings)
        im = IMAdapter(client)
        self._booking = BookingService(
            config=self._config,
            repository=repository,
            im=im,
            now_provider=self._now_provider,
        )

        logger.info("配置与字段映射校验通过")

    def run(self) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        if self._enable_scheduler:
            self._start_scheduler()
        else:
            logger.warning("开发联调模式: 已禁用定时任务，仅保留长连接")

        handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._on_message)
            .register_p2_card_action_trigger(self._on_card_action)
            .build()
        )

        ws_client = WsClientPatched(
            self._config.app_id,
            self._config.app_secret,
            event_handler=handler,
            card_frame_handler=self._on_card_frame_action,
            log_level=lark.LogLevel.INFO,
        )
        logger.info("长连接已启动")
        ws_client.start()

    def send_once(self, target_date: date | None = None) -> None:
        self.send_cards_once(target_date=target_date)

    def send_cards_once(self, target_date: date | None = None) -> None:
        if self._booking is None:
            raise RuntimeError("应用未初始化")
        self._booking.send_daily_cards(target_date=target_date)

    def send_stats_once(self, *, target_date: date | None = None, meal: Meal | None = None) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        today = datetime.now(ZoneInfo(self._config.timezone)).date()
        target = target_date or today
        if meal is None:
            self._booking.send_stats(target, Meal.LUNCH)
            self._booking.send_stats(target, Meal.DINNER)
            return
        self._booking.send_stats(target, meal)

    def execute_cron_action(self, action: CronAction, *, run_at: datetime) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        localized_run_at = _to_runtime_timezone(run_at, self._config.timezone)
        target_date = localized_run_at.date()
        if action == CronAction.SEND_CARDS:
            self._booking.send_daily_cards(target_date=target_date)
            return
        if action == CronAction.LUNCH_STATS:
            self._booking.send_stats(target_date, Meal.LUNCH)
            return
        self._booking.send_stats(target_date, Meal.DINNER)

    def _start_scheduler(self) -> None:
        if self._config is None or self._booking is None:
            raise RuntimeError("应用未初始化")

        if self._scheduler is not None:
            return

        tz = ZoneInfo(self._config.timezone)
        scheduler = BackgroundScheduler(timezone=tz)
        for spec in build_cron_job_specs(self._config.schedule):
            scheduler.add_job(
                self._run_scheduled_action,
                trigger="cron",
                hour=spec.hour,
                minute=spec.minute,
                args=[spec.action],
                id=spec.job_id,
                replace_existing=True,
            )

        scheduler.start()
        self._scheduler = scheduler

        logger.info(
            "定时任务已启动: send={}, lunch_cutoff={}, dinner_cutoff={}",
            self._config.schedule.send_time,
            self._config.schedule.lunch_cutoff,
            self._config.schedule.dinner_cutoff,
        )

    def _run_scheduled_action(self, action: CronAction) -> None:
        if self._config is None:
            raise RuntimeError("应用未初始化")
        now = datetime.now(ZoneInfo(self._config.timezone))
        self.execute_cron_action(action, run_at=now)

    def _on_message(self, data: P2ImMessageReceiveV1) -> None:
        if self._booking is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                self._booking.handle_message_event(data)
            except Exception:
                logger.exception("处理消息事件失败")
            return

        task = loop.create_task(self._handle_message_event_async(data))
        task.add_done_callback(self._on_message_done)

    def _on_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        if self._booking is None:
            return P2CardActionTriggerResponse({"toast": {"type": "error", "content": "服务未初始化"}})
        return self._booking.handle_card_action(data)

    def _on_card_frame_action(self, data) -> dict:
        if self._booking is None:
            return {"toast": {"type": "error", "content": "服务未初始化"}}
        return self._booking.handle_card_frame_action(data)

    async def _handle_message_event_async(self, data: P2ImMessageReceiveV1) -> None:
        if self._booking is None:
            return
        await asyncio.to_thread(self._booking.handle_message_event, data)

    @staticmethod
    def _on_message_done(task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception:
            logger.exception("异步处理消息事件失败")


def configure_logging(
    *,
    level: LogLevelOption | str = LogLevelOption.INFO,
    file_path: str | None = None,
    file_max_size_bytes: int | None = None,
) -> None:
    resolved_level = level.value.upper() if isinstance(level, LogLevelOption) else str(level).upper()
    logger.remove()
    common_options = {
        "level": resolved_level,
        "format": "{time:YYYY-MM-DD HH:mm:ss.SSS} {level} [{name}] {message}",
    }
    logger.add(
        sys.__stderr__,
        **common_options,
    )
    if file_path:
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        file_options = dict(common_options)
        file_options["encoding"] = "utf-8"
        if file_max_size_bytes is not None and file_max_size_bytes > 0:
            file_options["rotation"] = file_max_size_bytes
        logger.add(str(target), **file_options)


def _parse_cli_date(raw_value: str | None, option_name: str) -> date | None:
    if raw_value is None:
        return None
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} 格式错误，需为 YYYY-MM-DD") from exc


def _parse_cli_datetime(raw_value: str | None, option_name: str) -> datetime | None:
    if raw_value is None:
        return None
    try:
        return datetime.strptime(raw_value, "%Y-%m-%dT%H:%M")
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} 格式错误，需为 YYYY-MM-DDTHH:MM") from exc


def _to_runtime_timezone(target: datetime, timezone: str) -> datetime:
    tz = ZoneInfo(timezone)
    if target.tzinfo is None:
        return target.replace(tzinfo=tz)
    return target.astimezone(tz)


def _load_runtime_config_or_exit() -> RuntimeConfig:
    try:
        return load_runtime_config()
    except ConfigError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc


def _bootstrap_application(
    *,
    now_at: datetime | None = None,
    enable_scheduler: bool = True,
    runtime_config: RuntimeConfig | None = None,
) -> EatBotApplication:
    now_provider: Callable[[], datetime] | None = None
    if now_at is not None:
        now_provider = lambda: now_at

    app = EatBotApplication(now_provider=now_provider, enable_scheduler=enable_scheduler)
    try:
        app.bootstrap(runtime_config=runtime_config)
    except ConfigError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    return app


cli = typer.Typer(
    help="EatBot CLI（无子命令时等价于 `eatbot run`）",
    no_args_is_help=False,
    add_completion=False,
)
send_cli = typer.Typer(help="一次性发送命令（不常驻）", no_args_is_help=True)
dev_cli = typer.Typer(help="开发联调命令（测试模式）", no_args_is_help=True)
cli.add_typer(send_cli, name="send")
cli.add_typer(dev_cli, name="dev")


@cli.callback(invoke_without_command=True)
def root_callback(ctx: typer.Context) -> None:
    configure_logging(level=LogLevelOption.INFO)
    if ctx.invoked_subcommand is None:
        _run_service(log_level=LogLevelOption.INFO)


@cli.command("check", help="检查配置、字段映射与飞书表结构是否可用，不启动长连接和定时任务。")
def check_command() -> None:
    _bootstrap_application()
    logger.info("校验成功")


@cli.command("run", help="生产运行模式：启动长连接与定时任务并常驻。")
def run_command(
    log_level: LogLevelOption = typer.Option(
        LogLevelOption.INFO,
        "--log-level",
        case_sensitive=False,
        help="日志级别（同时作用于终端与文件日志），默认 info。",
    ),
) -> None:
    _run_service(log_level=log_level)


def _run_service(*, log_level: LogLevelOption) -> None:
    runtime_config = _load_runtime_config_or_exit()
    configure_logging(
        level=log_level,
        file_path=runtime_config.logging.file_path,
        file_max_size_bytes=runtime_config.logging.max_size_bytes,
    )
    app = _bootstrap_application(runtime_config=runtime_config)
    app.run()


@send_cli.command("cards", help="一次性发送预约卡片，不启动常驻服务。")
def send_cards_command(
    target_date: str | None = typer.Option(None, "--date", help="业务日期，格式 YYYY-MM-DD，默认当天。"),
) -> None:
    parsed_date = _parse_cli_date(target_date, "--date")
    app = _bootstrap_application()
    app.send_cards_once(target_date=parsed_date)
    if parsed_date is None:
        logger.info("今日卡片发送完成")
    else:
        logger.info("指定日期卡片发送完成: {}", parsed_date.isoformat())


@send_cli.command("stats", help="一次性发送统计消息，不启动常驻服务。")
def send_stats_command(
    meal: StatsMealOption = typer.Option(
        ...,
        "--meal",
        help="统计餐次：lunch|dinner|all（all 表示午晚餐都发送）。",
        case_sensitive=False,
    ),
    target_date: str | None = typer.Option(None, "--date", help="业务日期，格式 YYYY-MM-DD，默认当天。"),
) -> None:
    parsed_date = _parse_cli_date(target_date, "--date")
    app = _bootstrap_application()

    if meal == StatsMealOption.ALL:
        app.send_stats_once(target_date=parsed_date, meal=None)
    elif meal == StatsMealOption.LUNCH:
        app.send_stats_once(target_date=parsed_date, meal=Meal.LUNCH)
    else:
        app.send_stats_once(target_date=parsed_date, meal=Meal.DINNER)

    if parsed_date is None:
        logger.info("统计发送完成: meal={} date=today", meal.value)
    else:
        logger.info("统计发送完成: meal={} date={}", meal.value, parsed_date.isoformat())


@dev_cli.command("listen", help="开发联调模式：仅启动长连接，不启动定时任务。")
def dev_listen_command(
    at: str | None = typer.Option(None, "--at", help="虚拟当前时间，格式 YYYY-MM-DDTHH:MM。"),
) -> None:
    fake_now = _parse_cli_datetime(at, "--at")
    if fake_now is not None:
        logger.warning("开发联调虚拟时间: {}", fake_now.isoformat())
    app = _bootstrap_application(now_at=fake_now, enable_scheduler=False)
    app.run()


@dev_cli.command("cron", help="在时间窗口内预览或执行应触发的 cron 任务（用于联调定时器）。")
def dev_cron_command(
    from_: str = typer.Option(..., "--from", help="窗口开始时间，格式 YYYY-MM-DDTHH:MM。"),
    to: str = typer.Option(..., "--to", help="窗口结束时间，格式 YYYY-MM-DDTHH:MM。"),
    execute: bool = typer.Option(False, "--execute", help="执行窗口内命中的任务；默认仅预览不执行。"),
) -> None:
    runtime_config = _load_runtime_config_or_exit()
    parsed_from = _parse_cli_datetime(from_, "--from")
    parsed_to = _parse_cli_datetime(to, "--to")
    if parsed_from is None or parsed_to is None:
        raise typer.BadParameter("时间参数不能为空")

    from_at = _to_runtime_timezone(parsed_from, runtime_config.timezone)
    to_at = _to_runtime_timezone(parsed_to, runtime_config.timezone)
    if to_at < from_at:
        raise typer.BadParameter("--to 必须大于等于 --from")

    events = list_cron_trigger_events(runtime_config.schedule, start_at=from_at, end_at=to_at)
    if not events:
        typer.echo("窗口内无可触发任务")
        return

    typer.echo(f"窗口任务数: {len(events)}")
    for event in events:
        typer.echo(f"{event.trigger_at.isoformat()} {event.spec.job_id}")

    if not execute:
        return

    app = _bootstrap_application()
    for event in events:
        app.execute_cron_action(event.spec.action, run_at=event.trigger_at)
        logger.info("dev cron 任务执行完成: at={}, job={}", event.trigger_at.isoformat(), event.spec.job_id)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
