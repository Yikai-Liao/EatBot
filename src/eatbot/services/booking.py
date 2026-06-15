from __future__ import annotations

from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import threading
import time as mono_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from loguru import logger
from lark_oapi.api.application.v6 import P2ApplicationBotMenuV6
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from lark_oapi.card.model import Card
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from eatbot.config import RuntimeConfig
from eatbot.domain.cards import LeaveCardBuilder, ReservationCardBuilder
from eatbot.domain.decision import MealPlanDecider, parse_meals
from eatbot.domain.models import DailyMealPlan, Meal, MealScheduleRule, UserProfile
from eatbot.services.repositories import BitableRepository, CancelMealResult, MealFeeArchiveRecord
from eatbot.adapters.feishu_clients import FeishuApiError, IMAdapter


@dataclass(slots=True, frozen=True)
class ApplySelectionResult:
    record_ids: dict[Meal, str | None]
    cancel_failed: bool = False


@dataclass(slots=True, frozen=True)
class CronPreviewSnapshot:
    schedule_rules_count: int
    enabled_user_count: int
    stats_receiver_count: int
    plans_by_date: dict[date, DailyMealPlan]
    rules_by_date: dict[date, set[Meal]]
    matched_rule_count_by_date: dict[date, int]


@dataclass(slots=True, frozen=True)
class MealFeeArchiveWindow:
    run_date: date
    start_date: date
    end_date: date


@dataclass(slots=True, frozen=True)
class MealFeeArchiveSummary:
    run_date: date
    start_date: date
    end_date: date
    user_count: int
    total_fee: Decimal


@dataclass(slots=True, frozen=True)
class MealFeeArchiveExecutionPlan:
    archive_records: list[MealFeeArchiveRecord]
    user_notice_messages: list[tuple[str, str]]
    admin_receivers: list[str]
    admin_text: str | None
    total_fee: Decimal
    total_lunch_count: int
    total_dinner_count: int


@dataclass(slots=True, frozen=True)
class CardCallbackUpdateContext:
    token: str | None
    open_message_id: str | None


class BookingService:
    _ALL_MEALS = {Meal.LUNCH, Meal.DINNER}
    _USER_NOT_FOUND_TEXT = "你不在后台用户列表中，请联系管理员。"
    _CANCEL_FAILED_TEXT = "取消失败，请联系管理员。"
    _FEISHU_BOT_UNAVAILABLE_CODE = "230013"
    _RESERVATION_DISABLED_TEXT = "预约卡片功能已停用。"
    _LEAVE_DISABLED_TEXT = "请假功能已停用。"
    _RESERVATION_ACTIONS = {"toggle_meal", "refresh_state", "submit_reservation"}
    _LEAVE_ACTIONS = {"select_leave_start", "select_leave_end", "submit_leave_range"}

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        repository: BitableRepository,
        im: IMAdapter,
        now_provider: Callable[[], datetime] | None = None,
        background_runner: Callable[[Callable[[], None]], None] | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._im = im
        self._card_builder = ReservationCardBuilder()
        self._leave_card_builder = LeaveCardBuilder()
        self._decider = MealPlanDecider()
        self._timezone = ZoneInfo(config.timezone)
        self._now_provider = now_provider
        self._today_card_text_commands = frozenset(config.commands.today_card_texts)
        self._leave_text_commands = frozenset(config.commands.leave_texts)
        self._help_text_commands = frozenset(config.commands.help_texts)
        self._payment_qr_text_commands = frozenset(config.commands.payment_qr_texts)
        self._today_card_menu_event_keys = frozenset(config.commands.today_card_menu_event_keys)
        self._leave_menu_event_keys = frozenset(config.commands.leave_menu_event_keys)
        self._payment_qr_image_path = self._resolve_project_path(config.commands.payment_qr_image_path)
        self._card_action_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="eatbot-card-action")
        self._background_runner = background_runner or self._default_background_runner
        self._processing_users: set[str] = set()
        self._processing_users_lock = threading.Lock()

    def send_daily_cards(self, target_date: date | None = None) -> None:
        if not self._reservation_interactions_enabled:
            logger.info("预约卡片功能已停用，跳过每日发卡")
            return

        target = target_date or self._now().date()
        plan = self._plan_for_date(target, force_refresh=True)
        if not plan.meals:
            logger.info("今天不发送订餐卡片: date={} reason={}", target.isoformat(), plan.reason)
            return

        users = [user for user in self._repository.list_user_profiles() if user.enabled]
        for user in users:
            try:
                self._send_card_to_user(user=user, target_date=target, allowed_meals=plan.meals)
            except Exception:
                logger.exception("给用户发卡失败, user={}, open_id={}", user.display_name, user.open_id)

    def send_card_to_user_today(self, open_id: str) -> None:
        if not self._reservation_interactions_enabled:
            self._im.send_text(open_id, self._RESERVATION_DISABLED_TEXT)
            return

        target_date = self._card_request_target_date(self._now())
        user = self._load_user(open_id)
        if user is None:
            self._im.send_text(open_id, self._USER_NOT_FOUND_TEXT)
            return

        plan = self._plan_for_date(target_date)
        if not plan.meals:
            self._im.send_text(open_id, f"{target_date.isoformat()} 不在订餐发送范围（{plan.reason}）。")
            return

        self._send_card_to_user(user=user, target_date=target_date, allowed_meals=plan.meals)

    def send_leave_card_to_user(self, open_id: str) -> None:
        if not self._reservation_interactions_enabled:
            self._im.send_text(open_id, self._LEAVE_DISABLED_TEXT)
            return

        user = self._load_user(open_id)
        if user is None:
            self._im.send_text(open_id, self._USER_NOT_FOUND_TEXT)
            return

        card_json = self._leave_card_builder.build(user_open_id=user.open_id, target_date=self._now().date())
        self._im.send_interactive(receive_id=user.open_id, card_json=card_json)

    def send_stats(self, target_date: date, meal: Meal) -> None:
        started_at = mono_time.monotonic()
        logger.info("统计发送开始: date={} meal={}", target_date.isoformat(), meal.value)
        plan = self._plan_for_date(target_date)
        if meal not in plan.meals:
            logger.info(
                "当前餐次不在执行范围，跳过统计发送: date={} meal={} source={} reason={} allowed={}",
                target_date.isoformat(),
                meal.value,
                plan.source,
                plan.reason,
                _format_meals(plan.meals),
            )
            return

        reserved_rows = self._repository.list_reserved_meal_rows(target_date=target_date, meal=meal)
        count = len(reserved_rows)
        min_reserved_count = self._min_reserved_count(meal)
        receivers = self._repository.list_stats_receiver_open_ids()

        if count < min_reserved_count:
            self._repository.cancel_reserved_meal_rows(rows=reserved_rows)
            cancel_detail = (
                f"{_format_date_with_weekday(target_date)} {meal.value} 预约人数: {count}，"
                f"小于最小用餐人数 {min_reserved_count} 人，本餐取消"
            )
            admin_text = f"[管理员] {cancel_detail}"
            user_text = f"{cancel_detail}。请注意，需要自行解决{self._meal_fallback_text(meal)}。"
            for open_id in receivers:
                self._send_text_notice(
                    open_id=open_id,
                    text=admin_text,
                    log_name="订餐统计管理员通知",
                )
            for row in reserved_rows:
                if not row.open_id:
                    continue
                self._send_text_notice(
                    open_id=row.open_id,
                    text=user_text,
                    log_name="订餐取消用户通知",
                )
            logger.info(
                "订餐人数不足已取消: date={} meal={} count={} min_required={} receivers={} users={} cost={}ms",
                target_date.isoformat(),
                meal.value,
                count,
                min_reserved_count,
                len(receivers),
                len([row for row in reserved_rows if row.open_id]),
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return

        if not receivers:
            logger.info(
                "无统计接收人配置，跳过统计发送: date={} meal={} count={} cost={}ms",
                target_date.isoformat(),
                meal.value,
                count,
                int((mono_time.monotonic() - started_at) * 1000),
            )
            return

        text = f"[管理员] {_format_date_with_weekday(target_date)} {meal.value} 预约人数: {count}"
        for open_id in receivers:
            self._send_text_notice(
                open_id=open_id,
                text=text,
                log_name="订餐统计管理员通知",
            )
        logger.info(
            "统计发送完成: date={} meal={} count={} receivers={} cost={}ms",
            target_date.isoformat(),
            meal.value,
            count,
            len(receivers),
            int((mono_time.monotonic() - started_at) * 1000),
        )

    def send_text_to_enabled_users(self, text: str) -> None:
        started_at = mono_time.monotonic()
        users = [user for user in self._repository.list_user_profiles() if user.enabled]
        if not users:
            logger.info("无启用用户配置，跳过文本发送")
            return

        sent = 0
        skipped = 0
        failed = 0
        for user in users:
            result = self._send_text_notice(
                open_id=user.open_id,
                text=text,
                log_name="批量文本通知",
            )
            if result == "sent":
                sent += 1
            elif result == "skipped":
                skipped += 1
            else:
                failed += 1

        logger.info(
            "批量文本发送完成: users={} sent={} skipped={} failed={} cost={}ms",
            len(users),
            sent,
            skipped,
            failed,
            int((mono_time.monotonic() - started_at) * 1000),
        )

    def build_cron_preview_snapshot(self, *, target_dates: set[date]) -> CronPreviewSnapshot:
        rules = self._list_schedule_rules()
        enabled_user_count = sum(1 for user in self._repository.list_user_profiles() if user.enabled)
        stats_receiver_count = len(self._repository.list_stats_receiver_open_ids())

        plans_by_date: dict[date, DailyMealPlan] = {}
        rules_by_date: dict[date, set[Meal]] = {}
        matched_rule_count_by_date: dict[date, int] = {}
        for target_date in target_dates:
            plan = self._decider.decide(target_date, rules)
            plans_by_date[target_date] = plan
            rules_by_date[target_date] = set(plan.meals)
            matched_rule_count_by_date[target_date] = plan.matched_rule_count

        return CronPreviewSnapshot(
            schedule_rules_count=len(rules),
            enabled_user_count=enabled_user_count,
            stats_receiver_count=stats_receiver_count,
            plans_by_date=plans_by_date,
            rules_by_date=rules_by_date,
            matched_rule_count_by_date=matched_rule_count_by_date,
        )

    def preview_daily_cards(
        self,
        *,
        target_date: date,
        snapshot: CronPreviewSnapshot | None = None,
    ) -> tuple[bool, str]:
        if not self._reservation_interactions_enabled:
            return False, "预约卡片功能已停用"

        if snapshot is None:
            snapshot = self.build_cron_preview_snapshot(target_dates={target_date})
        plan = snapshot.plans_by_date.get(target_date) or self._plan_for_date(target_date)
        meals = set(plan.meals)
        if not meals:
            if plan.source == "schedule_rule":
                return False, "规则结果=不发送; 命中规则但餐次为空"
            return (
                False,
                f"规则结果=不发送; 默认规则={plan.reason}",
            )

        meals_text = _format_meals(meals)
        source_text = "规则来源=用餐定时配置" if plan.source == "schedule_rule" else f"默认规则={plan.reason}"
        if snapshot.enabled_user_count <= 0:
            return (
                False,
                f"规则餐次={meals_text}; {source_text}; 启用用户=0",
            )
        return (
            True,
            f"规则餐次={meals_text}; {source_text}; 启用用户={snapshot.enabled_user_count}",
        )

    def preview_stats(
        self,
        *,
        target_date: date,
        meal: Meal,
        snapshot: CronPreviewSnapshot | None = None,
    ) -> tuple[bool, str]:
        if snapshot is None:
            snapshot = self.build_cron_preview_snapshot(target_dates={target_date})
        plan = snapshot.plans_by_date.get(target_date) or self._plan_for_date(target_date)
        if meal not in plan.meals:
            if plan.source == "schedule_rule":
                if not plan.meals:
                    return False, f"餐次={meal.value}; 命中用餐定时配置但当日不供餐"
                return False, f"餐次={meal.value}; 当日可订餐次={_format_meals(plan.meals)}"
            return False, f"餐次={meal.value}; 默认规则={plan.reason}"
        if snapshot.stats_receiver_count <= 0:
            return False, f"餐次={meal.value}; 统计接收人=0"
        return True, f"餐次={meal.value}; 统计接收人={snapshot.stats_receiver_count}"

    def preview_fee_archive(self, *, target_date: date) -> tuple[bool, str]:
        window = self._build_meal_fee_archive_window(target_date)
        if target_date != window.run_date:
            return (
                False,
                f"非归档日; 本月归档日={window.run_date.isoformat()}",
            )
        plan = self._prepare_meal_fee_archive_execution(window=window)
        display_name_by_open_id = {
            user.open_id: user.display_name
            for user in self._repository.list_user_profiles()
        }
        lines = [
            (
                f"归档区间={window.start_date.isoformat()}~{window.end_date.isoformat()}（闭区间）; "
                f"写归档表={len(plan.archive_records)} 条; "
                f"用户通知={len(plan.user_notice_messages)}; "
                f"管理员通知={len(plan.admin_receivers)}; "
                f"总收款={_format_decimal(plan.total_fee)} 元"
            )
        ]
        if plan.archive_records:
            lines.append("归档写表:")
            for record in plan.archive_records:
                lines.append(
                    (
                        f"  - {_format_notice_target(record.open_id, display_name_by_open_id)}: "
                        f"午餐={record.lunch_count} 晚餐={record.dinner_count} "
                        f"费用={_format_decimal(record.fee)} 元"
                    )
                )
        else:
            lines.append("归档写表: 无记录")
        if plan.admin_text is None:
            lines.append("管理员通知: 无统计接收人配置")
        else:
            lines.append("管理员通知:")
            for open_id in plan.admin_receivers:
                lines.append(
                    f"  - {_format_notice_target(open_id, display_name_by_open_id)} <- {plan.admin_text}"
                )
        if plan.user_notice_messages:
            lines.append("用户通知:")
            for open_id, user_text in plan.user_notice_messages:
                lines.append(
                    f"  - {_format_notice_target(open_id, display_name_by_open_id)} <- {user_text} [附带: 付款码]"
                )
        else:
            lines.append("用户通知: 无需发送")
        return (
            True,
            "\n".join(lines),
        )

    def archive_meal_fees(self, *, target_date: date | None = None) -> MealFeeArchiveSummary | None:
        started_at = mono_time.monotonic()
        target = target_date or self._now().date()
        window = self._build_meal_fee_archive_window(target)
        if target != window.run_date:
            logger.debug(
                "今日非餐费归档日，跳过执行: target={} expected={}",
                target.isoformat(),
                window.run_date.isoformat(),
            )
            return None

        logger.info(
            "餐费归档开始: run_date={} start={} end={}",
            window.run_date.isoformat(),
            window.start_date.isoformat(),
            window.end_date.isoformat(),
        )
        plan = self._prepare_meal_fee_archive_execution(window=window)
        self._repository.upsert_meal_fee_archive_records(
            start_date=window.start_date,
            end_date=window.end_date,
            records=plan.archive_records,
        )
        for record in plan.archive_records:
            logger.debug(
                "餐费归档写表明细: open_id={} lunch_count={} dinner_count={} fee={}",
                record.open_id,
                record.lunch_count,
                record.dinner_count,
                _format_decimal(record.fee),
            )

        user_notice_sent = 0
        user_notice_skipped = 0
        user_notice_failed = 0
        admin_notice_sent = 0
        admin_notice_skipped = 0
        admin_notice_failed = 0
        if plan.admin_text is not None:
            for open_id in plan.admin_receivers:
                logger.debug(
                    "餐费归档管理员通知明细: open_id={} text={}",
                    open_id,
                    plan.admin_text,
                )
                result = self._send_text_notice(
                    open_id=open_id,
                    text=plan.admin_text,
                    log_name="餐费归档管理员通知",
                )
                if result == "sent":
                    admin_notice_sent += 1
                elif result == "skipped":
                    admin_notice_skipped += 1
                else:
                    admin_notice_failed += 1
        else:
            logger.info("无统计接收人配置，跳过餐费归档管理员通知")

        for open_id, user_text in plan.user_notice_messages:
            logger.debug(
                "餐费归档用户通知明细: open_id={} text={} attachment=付款码",
                open_id,
                user_text,
            )
            result = self._send_text_notice(
                open_id=open_id,
                text=user_text,
                log_name="餐费归档通知",
            )
            if result == "sent":
                user_notice_sent += 1
                self._send_payment_qr_notice(open_id=open_id, log_name="付款码")
            elif result == "skipped":
                user_notice_skipped += 1
            else:
                user_notice_failed += 1

        logger.info(
            "餐费归档完成: run_date={} start={} end={} users={} total_fee={} cost={}ms",
            window.run_date.isoformat(),
            window.start_date.isoformat(),
            window.end_date.isoformat(),
            len(plan.archive_records),
            _format_decimal(plan.total_fee),
            int((mono_time.monotonic() - started_at) * 1000),
        )
        logger.info(
            (
                "餐费归档通知结果: user_sent={} user_skipped={} user_failed={} "
                "admin_sent={} admin_skipped={} admin_failed={}"
            ),
            user_notice_sent,
            user_notice_skipped,
            user_notice_failed,
            admin_notice_sent,
            admin_notice_skipped,
            admin_notice_failed,
        )
        return MealFeeArchiveSummary(
            run_date=window.run_date,
            start_date=window.start_date,
            end_date=window.end_date,
            user_count=len(plan.archive_records),
            total_fee=plan.total_fee,
        )

    def _prepare_meal_fee_archive_execution(self, *, window: MealFeeArchiveWindow) -> MealFeeArchiveExecutionPlan:
        summaries = self._repository.list_meal_fee_summaries(start_date=window.start_date, end_date=window.end_date)
        summary_by_open_id = {item.open_id: item for item in summaries}
        target_open_ids = sorted(summary_by_open_id.keys())

        total_fee = Decimal("0")
        total_lunch_count = 0
        total_dinner_count = 0
        archive_records: list[MealFeeArchiveRecord] = []
        user_notice_messages: list[tuple[str, str]] = []
        for open_id in target_open_ids:
            summary = summary_by_open_id.get(open_id)
            fee = summary.total_fee if summary else Decimal("0")
            lunch_count = summary.lunch_count if summary else 0
            dinner_count = summary.dinner_count if summary else 0
            total_fee += fee
            total_lunch_count += lunch_count
            total_dinner_count += dinner_count
            archive_records.append(
                MealFeeArchiveRecord(
                    open_id=open_id,
                    fee=fee,
                    lunch_count=lunch_count,
                    dinner_count=dinner_count,
                )
            )
            user_notice_messages.append(
                (
                    open_id,
                    self._build_meal_fee_archive_user_notice_text(
                        window=window,
                        lunch_count=lunch_count,
                        dinner_count=dinner_count,
                        fee=fee,
                    ),
                )
            )

        admin_receivers = self._repository.list_stats_receiver_open_ids()
        admin_text = None
        if admin_receivers:
            admin_text = self._build_meal_fee_archive_admin_notice_text(
                window=window,
                total_lunch_count=total_lunch_count,
                total_dinner_count=total_dinner_count,
                total_fee=total_fee,
            )

        return MealFeeArchiveExecutionPlan(
            archive_records=archive_records,
            user_notice_messages=user_notice_messages,
            admin_receivers=admin_receivers,
            admin_text=admin_text,
            total_fee=total_fee,
            total_lunch_count=total_lunch_count,
            total_dinner_count=total_dinner_count,
        )

    def _build_meal_fee_archive_user_notice_text(
        self,
        *,
        window: MealFeeArchiveWindow,
        lunch_count: int,
        dinner_count: int,
        fee: Decimal,
    ) -> str:
        meal_count = lunch_count + dinner_count
        return (
            f"餐费归档通知：{window.start_date.isoformat()}~{window.end_date.isoformat()}，"
            f"你本月午餐 {lunch_count} 顿，晚餐 {dinner_count} 顿，共 {meal_count} 顿，"
            f"餐费合计 {_format_decimal(fee)} 元。"
        )

    def _build_meal_fee_archive_admin_notice_text(
        self,
        *,
        window: MealFeeArchiveWindow,
        total_lunch_count: int,
        total_dinner_count: int,
        total_fee: Decimal,
    ) -> str:
        total_meal_count = total_lunch_count + total_dinner_count
        return (
            f"[管理员] 餐费归档表已更新：{window.start_date.isoformat()}~"
            f"{window.end_date.isoformat()}，"
            f"午餐 {total_lunch_count} 人次，晚餐 {total_dinner_count} 人次，"
            f"总计 {total_meal_count} 人次，总收款 {_format_decimal(total_fee)} 元。"
        )

    def handle_message_event(self, data: P2ImMessageReceiveV1) -> None:
        message = data.event.message if data and data.event else None
        sender = data.event.sender if data and data.event else None
        sender_id = sender.sender_id if sender else None
        sender_open_id = sender_id.open_id if sender_id else None
        if not message or not sender_open_id:
            return

        if message.message_type != "text":
            return

        text = _extract_text_from_message_content(message.content)
        if text in self._today_card_text_commands:
            self.send_card_to_user_today(sender_open_id)
            return
        if text in self._leave_text_commands:
            self.send_leave_card_to_user(sender_open_id)
            return
        if text in self._payment_qr_text_commands:
            self._send_payment_qr_notice(open_id=sender_open_id, log_name="付款码")
            return
        if text in self._help_text_commands:
            self._im.send_text(sender_open_id, self._config.help_doc)
            return

        self._im.send_text(sender_open_id, self._config.help_doc)

    def handle_bot_menu_event(self, data: P2ApplicationBotMenuV6) -> None:
        event = data.event if data else None
        operator = event.operator if event else None
        operator_id = operator.operator_id if operator else None
        operator_open_id = operator_id.open_id if operator_id else None
        event_key = str(event.event_key).strip() if event and event.event_key is not None else ""

        if not operator_open_id:
            logger.warning("机器人菜单事件缺少操作者 open_id: event_key={}", event_key)
            return
        logger.info("收到机器人菜单事件: operator={} event_key={}", operator_open_id, event_key)
        if event_key in self._today_card_menu_event_keys:
            logger.info("命中当日卡片菜单事件: operator={} event_key={}", operator_open_id, event_key)
            self.send_card_to_user_today(operator_open_id)
            return
        if event_key in self._leave_menu_event_keys:
            logger.info("命中请假菜单事件: operator={} event_key={}", operator_open_id, event_key)
            self.send_leave_card_to_user(operator_open_id)
            return
        logger.warning(
            "忽略未知机器人菜单事件: operator={} event_key={} today_keys={} leave_keys={}",
            operator_open_id,
            event_key,
            sorted(self._today_card_menu_event_keys),
            sorted(self._leave_menu_event_keys),
        )

    def handle_card_action(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        started_at = mono_time.monotonic()
        try:
            event = data.event
            if not event or not event.action:
                return self._toast("error", "卡片参数缺失")

            callback_context = self._extract_callback_update_context(
                token=getattr(event, "token", None),
                context=getattr(event, "context", None),
            )
            level, content, card_payload = self._process_action_entry(
                operator_open_id=event.operator.open_id if event.operator else None,
                action_value=event.action.value or {},
                form_value=event.action.form_value or {},
                action_tag=getattr(event.action, "tag", None),
                action_option=getattr(event.action, "option", None),
                action_input_value=getattr(event.action, "input_value", None),
                source="event",
                callback_context=callback_context,
            )
            return self._toast(level, content, card_payload)
        except ValueError as exc:
            return self._toast("error", str(exc))
        except Exception:
            logger.exception("处理卡片回调失败")
            return self._toast("error", "预约更新失败")
        finally:
            logger.debug("卡片回调处理耗时: {}ms source=event", int((mono_time.monotonic() - started_at) * 1000))

    def handle_card_frame_action(self, data: Card) -> dict[str, Any]:
        started_at = mono_time.monotonic()
        try:
            action = getattr(data, "action", None)
            if action is None:
                return self._toast_dict("error", "卡片参数缺失")

            callback_context = self._extract_callback_update_context(
                token=getattr(data, "token", None),
                context=getattr(data, "context", None),
            )
            level, content, card_payload = self._process_action_entry(
                operator_open_id=getattr(data, "open_id", None),
                action_value=action.value or {},
                form_value=action.form_value or {},
                action_tag=getattr(action, "tag", None),
                action_option=getattr(action, "option", None),
                action_input_value=getattr(action, "input_value", None),
                source="card",
                callback_context=callback_context,
            )
            return self._toast_dict(level, content, card_payload)
        except ValueError as exc:
            return self._toast_dict("error", str(exc))
        except Exception:
            logger.exception("处理卡片回调失败")
            return self._toast_dict("error", "预约更新失败")
        finally:
            logger.debug("卡片回调处理耗时: {}ms source=card", int((mono_time.monotonic() - started_at) * 1000))

    def _send_card_to_user(self, *, user: UserProfile, target_date: date, allowed_meals: set[Meal]) -> None:
        defaults = user.meal_preferences & allowed_meals
        meal_prices: dict[Meal, Decimal] = {}

        if Meal.LUNCH in allowed_meals:
            meal_prices[Meal.LUNCH] = user.lunch_price
        if Meal.DINNER in allowed_meals:
            meal_prices[Meal.DINNER] = user.dinner_price

        selected, meal_record_ids = self._resolve_selected_from_records(
            target_date=target_date,
            open_id=user.open_id,
            allowed_meals=allowed_meals,
        )
        for meal in defaults:
            if meal_record_ids.get(meal) is None:
                selected.add(meal)

        for meal in selected:
            if meal_record_ids.get(meal) is not None:
                continue
            price = meal_prices.get(meal, Decimal("0"))
            record_id = self._repository.upsert_meal_record(
                target_date=target_date,
                open_id=user.open_id,
                meal=meal,
                price=price,
            )
            meal_record_ids[meal] = record_id

        card_json = self._card_builder.build(
            target_date=target_date,
            lunch_cutoff=self._config.schedule.lunch_cutoff,
            dinner_cutoff=self._config.schedule.dinner_cutoff,
            user_open_id=user.open_id,
            allowed_meals=allowed_meals,
            default_meals=defaults,
            selected_meals=selected,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
        )
        self._im.send_interactive(receive_id=user.open_id, card_json=card_json)

    def _process_action_entry(
        self,
        *,
        operator_open_id: str | None,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        action_tag: str | None,
        action_option: str | None,
        action_input_value: str | None,
        source: str,
        callback_context: CardCallbackUpdateContext | None,
    ) -> tuple[str | None, str | None, dict[str, Any] | None]:
        action_name = str(action_value.get("action") or "")
        if not self._reservation_interactions_enabled:
            disabled_text = self._disabled_action_text(action_name)
            if disabled_text is not None:
                return ("error", disabled_text, None)

        if callback_context is None or action_name not in {"toggle_meal", "refresh_state", "submit_leave_range"}:
            return self._process_action(
                operator_open_id=operator_open_id,
                action_value=action_value,
                form_value=form_value,
                action_tag=action_tag,
                action_option=action_option,
                action_input_value=action_input_value,
                source=source,
            )
        if not operator_open_id:
            return ("error", "仅允许本人提交预约", None)

        target_open_id = str(action_value.get("target_open_id") or "")
        if operator_open_id != target_open_id:
            return ("error", "仅允许本人提交预约", None)

        target_date = _parse_iso_date(str(action_value.get("target_date") or ""))
        if target_date is None:
            return ("error", "日期参数无效", None)

        if not self._mark_user_processing(operator_open_id):
            return ("info", "后台处理中，请稍后", None)

        submitted_to_background = False
        try:
            if action_name == "toggle_meal":
                toggle = _parse_meal(action_value.get("toggle_meal"))
                if toggle is None:
                    return ("error", "不支持的餐次操作", None)
                allowed_meals = parse_meals(action_value.get("allowed_meals"))
                if toggle in allowed_meals and not self._is_editable(target_date=target_date, meal=toggle):
                    return ("error", f"{toggle.value} 已过截止时间，如有特殊情况请联系管理员人工处理", None)

            optimistic_card_payload = self._build_optimistic_card_payload(
                target_date=target_date,
                target_open_id=target_open_id,
                action_value=action_value,
                refresh_syncing=True,
            )
            if optimistic_card_payload is None:
                return self._process_action(
                    operator_open_id=operator_open_id,
                    action_value=action_value,
                    form_value=form_value,
                    action_tag=action_tag,
                    action_option=action_option,
                    action_input_value=action_input_value,
                    source=source,
                )

            self._background_runner(
                lambda: self._run_action_in_background(
                    operator_open_id=operator_open_id,
                    target_date=target_date,
                    action_value=action_value,
                    form_value=form_value,
                    action_tag=action_tag,
                    action_option=action_option,
                    action_input_value=action_input_value,
                    source=source,
                    callback_context=callback_context,
                    optimistic_card_payload=optimistic_card_payload,
                )
            )
            submitted_to_background = True
            return ("info", "处理中", optimistic_card_payload)
        finally:
            if not submitted_to_background:
                self._unmark_user_processing(operator_open_id)

    def _run_action_in_background(
        self,
        *,
        operator_open_id: str,
        target_date: date,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        action_tag: str | None,
        action_option: str | None,
        action_input_value: str | None,
        source: str,
        callback_context: CardCallbackUpdateContext,
        optimistic_card_payload: dict[str, Any],
    ) -> None:
        action_name = str(action_value.get("action") or "")
        try:
            try:
                level, content, latest_payload = self._process_action(
                    operator_open_id=operator_open_id,
                    action_value=action_value,
                    form_value=form_value,
                    action_tag=action_tag,
                    action_option=action_option,
                    action_input_value=action_input_value,
                    source=f"{source}_async",
                )
            except Exception:
                logger.exception(
                    "后台处理卡片回调失败: operator={} action={}",
                    operator_open_id,
                    action_name,
                )
                level = "error"
                content = "预约更新失败，请稍后刷新重试"
                latest_payload = None

            fallback_payload = self._build_optimistic_card_payload(
                target_date=target_date,
                target_open_id=operator_open_id,
                action_value=action_value,
                refresh_syncing=False,
            )
            final_payload = latest_payload or fallback_payload or optimistic_card_payload
            self._push_async_card_update(
                callback_context=callback_context,
                card_payload=final_payload,
                operator_open_id=operator_open_id,
                target_date=target_date,
                toast_type=level,
                toast_content=content,
            )
        finally:
            self._unmark_user_processing(operator_open_id)

    def _push_async_card_update(
        self,
        *,
        callback_context: CardCallbackUpdateContext,
        card_payload: dict[str, Any],
        operator_open_id: str,
        target_date: date,
        toast_type: str = "info",
        toast_content: str | None = None,
    ) -> None:
        if callback_context.token:
            try:
                self._im.delay_update_card(
                    token=callback_context.token,
                    card_payload=card_payload,
                    toast_type=toast_type,
                    toast_content=toast_content,
                )
                logger.info(
                    "异步卡片刷新成功: mode=callback_token operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )
                return
            except FeishuApiError as exc:
                if "code=10002" in str(exc):
                    logger.warning(
                        "异步卡片刷新回退: mode=callback_token operator={} date={} reason=code_10002",
                        operator_open_id,
                        target_date.isoformat(),
                    )
                else:
                    logger.exception(
                        "异步卡片刷新失败: mode=callback_token operator={} date={}",
                        operator_open_id,
                        target_date.isoformat(),
                    )
            except Exception:
                logger.exception(
                    "异步卡片刷新失败: mode=callback_token operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )

        if callback_context.open_message_id:
            try:
                self._im.patch_interactive(message_id=callback_context.open_message_id, card_payload=card_payload)
                logger.info(
                    "异步卡片刷新成功: mode=open_message_id operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )
                return
            except Exception:
                logger.exception(
                    "异步卡片刷新失败: mode=open_message_id operator={} date={}",
                    operator_open_id,
                    target_date.isoformat(),
                )

        logger.warning(
            "异步卡片刷新跳过: operator={} date={} reason=no_available_context",
            operator_open_id,
            target_date.isoformat(),
        )

    def _build_optimistic_card_payload(
        self,
        *,
        target_date: date,
        target_open_id: str,
        action_value: dict[str, Any],
        refresh_syncing: bool = False,
    ) -> dict[str, Any] | None:
        action_name = str(action_value.get("action") or "")
        if action_name == "submit_leave_range":
            return self._leave_card_builder.build_payload(
                user_open_id=target_open_id,
                target_date=target_date,
                selected_start_date=_parse_iso_date(str(action_value.get("selected_start_date") or "")) or target_date,
                selected_end_date=_parse_iso_date(str(action_value.get("selected_end_date") or "")) or target_date,
                readonly=refresh_syncing,
                submit_syncing=refresh_syncing,
            )
        if action_name not in {"toggle_meal", "refresh_state"}:
            return None

        allowed = parse_meals(action_value.get("allowed_meals"))
        if not allowed:
            return None
        defaults = parse_meals(action_value.get("default_meals")) & allowed
        selected = parse_meals(action_value.get("selected_meals")) & allowed
        if action_name == "toggle_meal":
            toggle_meal = _parse_meal(action_value.get("toggle_meal"))
            if toggle_meal and toggle_meal in allowed:
                if toggle_meal in selected:
                    selected.remove(toggle_meal)
                else:
                    selected.add(toggle_meal)

        meal_prices = self._parse_meal_prices(action_value=action_value, allowed_meals=allowed)
        meal_record_ids = self._parse_meal_record_ids(action_value=action_value, allowed_meals=allowed)
        return self._card_builder.build_payload(
            target_date=target_date,
            lunch_cutoff=self._config.schedule.lunch_cutoff,
            dinner_cutoff=self._config.schedule.dinner_cutoff,
            user_open_id=target_open_id,
            allowed_meals=allowed,
            default_meals=defaults,
            selected_meals=selected,
            meal_prices=meal_prices,
            meal_record_ids=meal_record_ids,
            refresh_syncing=refresh_syncing,
        )

    @staticmethod
    def _parse_meal_prices(*, action_value: dict[str, Any], allowed_meals: set[Meal]) -> dict[Meal, Decimal]:
        raw_prices = action_value.get("meal_prices")
        if not isinstance(raw_prices, dict):
            return {meal: Decimal("0") for meal in allowed_meals}
        result: dict[Meal, Decimal] = {}
        for meal in allowed_meals:
            raw = raw_prices.get(meal.value)
            try:
                result[meal] = Decimal(str(raw))
            except Exception:
                result[meal] = Decimal("0")
        return result

    @staticmethod
    def _parse_meal_record_ids(*, action_value: dict[str, Any], allowed_meals: set[Meal]) -> dict[Meal, str | None]:
        raw_ids = action_value.get("meal_record_ids")
        if not isinstance(raw_ids, dict):
            return {meal: None for meal in allowed_meals}
        result: dict[Meal, str | None] = {}
        for meal in allowed_meals:
            raw = raw_ids.get(meal.value)
            if raw is None or raw == "":
                result[meal] = None
                continue
            result[meal] = str(raw)
        return result

    def _default_background_runner(self, task: Callable[[], None]) -> None:
        self._card_action_executor.submit(task)

    def _mark_user_processing(self, open_id: str) -> bool:
        with self._processing_users_lock:
            if open_id in self._processing_users:
                return False
            self._processing_users.add(open_id)
            return True

    def _unmark_user_processing(self, open_id: str) -> None:
        with self._processing_users_lock:
            self._processing_users.discard(open_id)

    @staticmethod
    def _extract_callback_update_context(*, token: Any, context: Any) -> CardCallbackUpdateContext | None:
        token_value = str(token or "").strip() or None
        open_message_id_value = str(getattr(context, "open_message_id", "") or "").strip() or None
        if token_value is None and open_message_id_value is None:
            return None
        return CardCallbackUpdateContext(
            token=token_value,
            open_message_id=open_message_id_value,
        )

    def _process_action(
        self,
        *,
        operator_open_id: str | None,
        action_value: dict[str, Any],
        form_value: dict[str, Any],
        action_tag: str | None,
        action_option: str | None,
        action_input_value: str | None,
        source: str,
        enforce_cutoff: bool = True,
    ) -> tuple[str, str, dict[str, Any] | None]:
        action_name = str(action_value.get("action") or "")
        perf_total_started = mono_time.monotonic()
        perf_last_started = perf_total_started
        phase_cost: dict[str, int] = {}

        def _mark(phase: str) -> None:
            nonlocal perf_last_started
            now = mono_time.monotonic()
            phase_cost[phase] = int((now - perf_last_started) * 1000)
            perf_last_started = now

        try:
            logger.info(
                "收到卡片回调: source={} operator={} action={}",
                source,
                operator_open_id or "",
                action_name,
            )

            if not self._reservation_interactions_enabled:
                disabled_text = self._disabled_action_text(action_name)
                if disabled_text is not None:
                    return ("error", disabled_text, None)

            if not operator_open_id:
                return ("error", "仅允许本人提交预约", None)

            target_open_id = str(action_value.get("target_open_id") or "")
            if operator_open_id != target_open_id:
                return ("error", "仅允许本人提交预约", None)

            target_date = _parse_iso_date(str(action_value.get("target_date") or ""))
            if target_date is None:
                return ("error", "日期参数无效", None)

            user = self._load_user(operator_open_id)
            if user is None:
                return ("error", self._USER_NOT_FOUND_TEXT, None)

            if action_name in {"select_leave_start", "select_leave_end"}:
                selected_date = _parse_card_picker_date(str(action_option or action_input_value or ""))
                if selected_date is None:
                    logger.warning(
                        "请假日期选择回调缺少日期: source={} action={} tag={} option={} input={} payload={}",
                        source,
                        action_name,
                        action_tag or "",
                        action_option or "",
                        action_input_value or "",
                        action_value,
                    )
                    return ("error", "日期选择失败，请重试", None)

                current_start = _parse_iso_date(str(action_value.get("selected_start_date") or ""))
                current_end = _parse_iso_date(str(action_value.get("selected_end_date") or ""))
                if action_name == "select_leave_start":
                    current_start = selected_date
                else:
                    current_end = selected_date

                fallback_date = selected_date
                card_payload = self._leave_card_builder.build_payload(
                    user_open_id=operator_open_id,
                    target_date=target_date,
                    selected_start_date=current_start or fallback_date,
                    selected_end_date=current_end or fallback_date,
                    submit_syncing=False,
                )
                _mark("parse_and_validate")
                _mark("apply_selection")
                _mark("build_card")
                return ("info", "日期已更新", card_payload)

            if action_name == "submit_leave_range":
                start_date = _parse_iso_date(str(action_value.get("selected_start_date") or ""))
                end_date = _parse_iso_date(str(action_value.get("selected_end_date") or ""))
                logger.info(
                    "处理请假提交: source={} start={} end={} tag={} option={} form_keys={} payload={}",
                    source,
                    start_date.isoformat() if start_date else "",
                    end_date.isoformat() if end_date else "",
                    action_tag or "",
                    action_option or "",
                    sorted(form_value.keys()),
                    action_value,
                )
                if start_date is None or end_date is None:
                    return ("error", "请先选择开始日期和结束日期", None)
                if end_date < start_date:
                    return ("error", "结束日期不能早于开始日期", None)

                self._apply_leave_range(user=user, start_date=start_date, end_date=end_date)
                self._send_leave_success_notice(
                    open_id=operator_open_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                _mark("parse_and_validate")
                _mark("apply_selection")
                card_payload = self._leave_card_builder.build_payload(
                    user_open_id=operator_open_id,
                    target_date=start_date,
                    selected_start_date=start_date,
                    selected_end_date=end_date,
                    readonly=True,
                    submitted=True,
                    submit_syncing=False,
                )
                _mark("build_card")
                return ("info", "请假已设置", card_payload)

            allowed = self._allowed_meals_for_date(target_date)
            defaults = user.meal_preferences & allowed
            meal_prices = self._build_meal_prices(user=user, allowed_meals=allowed)

            rows = self._repository.list_user_meal_rows(target_date=target_date, open_id=operator_open_id)
            rows = self._sync_disallowed_meal_rows(
                target_date=target_date,
                open_id=operator_open_id,
                allowed_meals=allowed,
                rows=rows,
            )
            selected_before, meal_record_ids = self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed)
            selected = set(selected_before)

            if action_name == "toggle_meal":
                toggle = _parse_meal(action_value.get("toggle_meal"))
                if toggle is None:
                    return ("error", "不支持的餐次操作", None)
                if toggle not in allowed:
                    _mark("parse_and_validate")
                    _mark("apply_selection")
                    card_payload = self._card_builder.build_payload(
                        target_date=target_date,
                        lunch_cutoff=self._config.schedule.lunch_cutoff,
                        dinner_cutoff=self._config.schedule.dinner_cutoff,
                        user_open_id=operator_open_id,
                        allowed_meals=allowed,
                        default_meals=defaults,
                        selected_meals=selected,
                        meal_prices=meal_prices,
                        meal_record_ids=meal_record_ids,
                    )
                    _mark("build_card")
                    return ("info", f"{toggle.value} 当前不可预约，已同步最新状态", card_payload)
                if toggle in selected:
                    selected.remove(toggle)
                else:
                    selected.add(toggle)
            elif action_name == "submit_reservation":
                form_selected = parse_meals(form_value.get("meals"))
                if form_selected:
                    selected = form_selected & allowed
            elif action_name == "refresh_state":
                _mark("parse_and_validate")
                _mark("apply_selection")

                card_payload = self._card_builder.build_payload(
                    target_date=target_date,
                    lunch_cutoff=self._config.schedule.lunch_cutoff,
                    dinner_cutoff=self._config.schedule.dinner_cutoff,
                    user_open_id=operator_open_id,
                    allowed_meals=allowed,
                    default_meals=defaults,
                    selected_meals=selected,
                    meal_prices=meal_prices,
                    meal_record_ids=meal_record_ids,
                )
                _mark("build_card")
                return ("info", "已刷新最新预约状态", card_payload)
            else:
                return ("error", "不支持的卡片操作", None)

            selected &= allowed
            changed_meals = {meal for meal in allowed if (meal in selected_before) != (meal in selected)}
            if enforce_cutoff:
                blocked_meal = next(
                    (meal for meal in changed_meals if not self._is_editable(target_date=target_date, meal=meal)),
                    None,
                )
                if blocked_meal is not None:
                    return ("error", f"{blocked_meal.value} 已过截止时间，如有特殊情况请联系管理员人工处理", None)
            _mark("parse_and_validate")

            apply_result = self._apply_selection(
                target_date=target_date,
                operator_open_id=operator_open_id,
                changed_meals=changed_meals,
                selected=selected,
                meal_prices=meal_prices,
                meal_record_ids=meal_record_ids,
            )
            _mark("apply_selection")

            if apply_result.cancel_failed:
                self._send_cancel_failed_notice(open_id=operator_open_id)
                card_payload = self._card_builder.build_payload(
                    target_date=target_date,
                    lunch_cutoff=self._config.schedule.lunch_cutoff,
                    dinner_cutoff=self._config.schedule.dinner_cutoff,
                    user_open_id=operator_open_id,
                    allowed_meals=allowed,
                    default_meals=defaults,
                    selected_meals=selected_before,
                    meal_prices=meal_prices,
                    meal_record_ids=meal_record_ids,
                )
                _mark("build_card")
                return ("error", self._CANCEL_FAILED_TEXT, card_payload)

            card_payload = self._card_builder.build_payload(
                target_date=target_date,
                lunch_cutoff=self._config.schedule.lunch_cutoff,
                dinner_cutoff=self._config.schedule.dinner_cutoff,
                user_open_id=operator_open_id,
                allowed_meals=allowed,
                default_meals=defaults,
                selected_meals=selected,
                meal_prices=meal_prices,
                meal_record_ids=apply_result.record_ids,
            )
            _mark("build_card")
            return ("info", "预约已更新", card_payload)
        finally:
            total_cost = int((mono_time.monotonic() - perf_total_started) * 1000)
            logger.debug(
                "卡片回调分段耗时: source={} action={} parse={}ms apply={}ms build={}ms total={}ms",
                source,
                action_name,
                phase_cost.get("parse_and_validate", 0),
                phase_cost.get("apply_selection", 0),
                phase_cost.get("build_card", 0),
                total_cost,
            )

    def _apply_selection(
        self,
        *,
        target_date: date,
        operator_open_id: str,
        changed_meals: set[Meal],
        selected: set[Meal],
        meal_prices: dict[Meal, Decimal],
        meal_record_ids: dict[Meal, str | None],
    ) -> ApplySelectionResult:
        started_at = mono_time.monotonic()
        updated_record_ids = dict(meal_record_ids)

        write_started = mono_time.monotonic()
        upsert_count = 0
        cancel_count = 0
        for meal in self._sorted_changed_meals(changed_meals):
            record_id = updated_record_ids.get(meal)
            if meal in selected:
                continue
            op_started = mono_time.monotonic()
            result = self._repository.cancel_meal_record(
                target_date=target_date,
                open_id=operator_open_id,
                meal=meal,
                record_id=record_id,
                prefer_direct=True,
            )
            cancel_count += 1
            if result.status == "failed":
                logger.error(
                    "预约取消失败: open_id={} date={} meal={} record_id={} error={}",
                    operator_open_id,
                    target_date.isoformat(),
                    meal.value,
                    record_id or "",
                    result.error_message or "",
                )
                return ApplySelectionResult(record_ids=updated_record_ids, cancel_failed=True)
            if result.record_id is not None:
                updated_record_ids[meal] = result.record_id
            logger.debug(
                "预约写入耗时: op=cancel meal={} date={} has_record={} status={} cost={}ms",
                meal.value,
                target_date.isoformat(),
                bool(record_id),
                result.status,
                int((mono_time.monotonic() - op_started) * 1000),
            )

        for meal in self._sorted_changed_meals(changed_meals):
            record_id = updated_record_ids.get(meal)
            if meal not in selected:
                continue
            price = meal_prices.get(meal)
            if price is None:
                raise ValueError(f"{meal.value} 单价缺失")
            has_record_id = bool(record_id)
            op_started = mono_time.monotonic()
            record_id = self._repository.upsert_meal_record(
                target_date=target_date,
                open_id=operator_open_id,
                meal=meal,
                price=price,
                record_id=record_id,
                prefer_direct=True,
            )
            upsert_count += 1
            updated_record_ids[meal] = record_id
            logger.debug(
                "预约写入耗时: op=upsert meal={} date={} direct={} cost={}ms",
                meal.value,
                target_date.isoformat(),
                has_record_id,
                int((mono_time.monotonic() - op_started) * 1000),
            )

        write_cost = int((mono_time.monotonic() - write_started) * 1000)
        total_cost = int((mono_time.monotonic() - started_at) * 1000)
        logger.debug(
            "预约写入分段耗时: date={} open_id={} changed={} cutoff={}ms write={}ms upsert={} cancel={} total={}ms",
            target_date.isoformat(),
            operator_open_id,
            len(changed_meals),
            0,
            write_cost,
            upsert_count,
            cancel_count,
            total_cost,
        )
        return ApplySelectionResult(record_ids=updated_record_ids)

    @staticmethod
    def _sorted_changed_meals(meals: set[Meal]) -> list[Meal]:
        return sorted(meals, key=lambda item: (0 if item == Meal.LUNCH else 1, item.value))

    def _apply_leave_range(
        self,
        *,
        user: UserProfile,
        start_date: date,
        end_date: date,
    ) -> None:
        # start_date/end_date are treated as an inclusive range.
        target_date = start_date
        while target_date <= end_date:
            allowed_meals = self._allowed_meals_for_date(target_date)
            if not allowed_meals:
                target_date += timedelta(days=1)
                continue

            meal_prices = self._build_meal_prices(user=user, allowed_meals=allowed_meals)
            rows = self._repository.list_user_meal_rows(target_date=target_date, open_id=user.open_id)
            rows = self._sync_disallowed_meal_rows(
                target_date=target_date,
                open_id=user.open_id,
                allowed_meals=allowed_meals,
                rows=rows,
            )
            _, meal_record_ids = self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed_meals)

            for meal in allowed_meals:
                price = meal_prices.get(meal)
                if price is None:
                    raise ValueError(f"{meal.value} 单价缺失")
                record_id = self._repository.mark_meal_record_unreserved(
                    target_date=target_date,
                    open_id=user.open_id,
                    meal=meal,
                    price=price,
                    record_id=meal_record_ids.get(meal),
                    prefer_direct=True,
                )
                meal_record_ids[meal] = record_id

            target_date += timedelta(days=1)

    def _send_leave_success_notice(self, *, open_id: str, start_date: date, end_date: date) -> None:
        text = (
            f"请假成功，已自动暂停 {start_date.isoformat()} 到 {end_date.isoformat()} 的食堂自动预约，"
            "但仍可通过点击当日卡片重新预约食堂。"
        )
        self._send_text_notice(open_id=open_id, text=text, log_name="请假成功通知")

    def _resolve_selected_from_records(
        self,
        *,
        target_date: date,
        open_id: str,
        allowed_meals: set[Meal],
    ) -> tuple[set[Meal], dict[Meal, str | None]]:
        rows = self._repository.list_user_meal_rows(target_date=target_date, open_id=open_id)
        return self._resolve_selected_from_rows(rows=rows, allowed_meals=allowed_meals)

    @staticmethod
    def _resolve_selected_from_rows(
        *,
        rows: list[Any],
        allowed_meals: set[Meal],
    ) -> tuple[set[Meal], dict[Meal, str | None]]:
        row_by_meal = _pick_rows_by_meal(rows=rows, allowed_meals=allowed_meals)
        selected: set[Meal] = set()
        meal_record_ids: dict[Meal, str | None] = {meal: None for meal in allowed_meals}
        for meal in allowed_meals:
            row = row_by_meal.get(meal)
            if row is None:
                continue
            meal_record_ids[meal] = row.record_id
            if row.reservation_status:
                selected.add(meal)
        return selected, meal_record_ids

    def _list_schedule_rules(self, *, force_refresh: bool = False) -> list[MealScheduleRule]:
        rules = self._repository.list_schedule_rules()
        logger.debug(
            "用餐定时配置已实时拉取: force={} rules={}",
            force_refresh,
            len(rules),
        )
        return list(rules)

    def _plan_for_date(self, target_date: date, *, force_refresh: bool = False) -> DailyMealPlan:
        rules = self._list_schedule_rules(force_refresh=force_refresh)
        return self._decider.decide(target_date, rules)

    def _allowed_meals_for_date(self, target_date: date) -> set[Meal]:
        return set(self._plan_for_date(target_date).meals)

    @staticmethod
    def _build_meal_prices(*, user: UserProfile, allowed_meals: set[Meal]) -> dict[Meal, Decimal]:
        prices: dict[Meal, Decimal] = {}
        if Meal.LUNCH in allowed_meals:
            prices[Meal.LUNCH] = user.lunch_price
        if Meal.DINNER in allowed_meals:
            prices[Meal.DINNER] = user.dinner_price
        return prices

    def _sync_disallowed_meal_rows(
        self,
        *,
        target_date: date,
        open_id: str,
        allowed_meals: set[Meal],
        rows: list[Any],
    ) -> list[Any]:
        disallowed_meals = self._ALL_MEALS - allowed_meals
        if not disallowed_meals:
            return rows

        disallowed_rows = _pick_rows_by_meal(rows=rows, allowed_meals=disallowed_meals)
        changed_meals: set[Meal] = set()
        attempted = False
        for meal in disallowed_meals:
            row = disallowed_rows.get(meal)
            if row is None or not bool(getattr(row, "reservation_status", False)):
                continue
            attempted = True
            result = self._repository.cancel_meal_record(
                target_date=target_date,
                open_id=open_id,
                meal=meal,
                record_id=row.record_id,
                prefer_direct=True,
            )
            if result.status == "failed":
                logger.error(
                    "自动取消不可预约餐次失败: date={} open_id={} meal={} record_id={} error={}",
                    target_date.isoformat(),
                    open_id,
                    meal.value,
                    row.record_id,
                    result.error_message or "",
                )
                continue
            changed_meals.add(meal)

        if not attempted:
            return rows

        if changed_meals:
            logger.info(
                "根据用餐定时配置自动取消不可预约餐次: date={} open_id={} meals={}",
                target_date.isoformat(),
                open_id,
                _format_meals(changed_meals),
            )
        return self._repository.list_user_meal_rows(target_date=target_date, open_id=open_id)

    def _load_user(self, open_id: str) -> UserProfile | None:
        users = self._repository.list_user_profiles()
        return next((user for user in users if user.open_id == open_id and user.enabled), None)

    def _is_editable(self, *, target_date: date, meal: Meal) -> bool:
        now = self._now()
        today = now.date()

        if target_date > today:
            return True
        if target_date < today:
            return False

        if meal == Meal.LUNCH:
            return now.time() < self._config.schedule.lunch_cutoff_obj
        if meal == Meal.DINNER:
            return now.time() < self._config.schedule.dinner_cutoff_obj
        return False

    def _min_reserved_count(self, meal: Meal) -> int:
        if meal == Meal.LUNCH:
            return self._config.schedule.lunch_min_reserved_count
        if meal == Meal.DINNER:
            return self._config.schedule.dinner_min_reserved_count
        return 0

    @staticmethod
    def _meal_fallback_text(meal: Meal) -> str:
        if meal == Meal.LUNCH:
            return "午餐"
        if meal == Meal.DINNER:
            return "晚餐"
        return "用餐"

    def _build_meal_fee_archive_window(self, target_date: date) -> MealFeeArchiveWindow:
        day_of_month = self._config.schedule.fee_archive_day_of_month
        run_date = _resolve_monthly_day(
            year=target_date.year,
            month=target_date.month,
            day_of_month=day_of_month,
        )
        if target_date.month == 1:
            prev_year = target_date.year - 1
            prev_month = 12
        else:
            prev_year = target_date.year
            prev_month = target_date.month - 1
        prev_run_date = _resolve_monthly_day(
            year=prev_year,
            month=prev_month,
            day_of_month=day_of_month,
        )
        return MealFeeArchiveWindow(
            run_date=run_date,
            start_date=prev_run_date + timedelta(days=1),
            end_date=run_date,
        )

    def _now(self) -> datetime:
        if self._now_provider is None:
            return datetime.now(self._timezone)

        now = self._now_provider()
        if now.tzinfo is None:
            return now.replace(tzinfo=self._timezone)
        return now.astimezone(self._timezone)

    def _card_request_target_date(self, now: datetime) -> date:
        target_date = now.date()
        if now.time() >= self._config.schedule.card_request_cutover_obj:
            return target_date + timedelta(days=1)
        return target_date

    @property
    def _reservation_interactions_enabled(self) -> bool:
        return self._config.features.reservation_interactions_enabled

    @classmethod
    def _disabled_action_text(cls, action_name: str) -> str | None:
        if action_name in cls._RESERVATION_ACTIONS:
            return cls._RESERVATION_DISABLED_TEXT
        if action_name in cls._LEAVE_ACTIONS:
            return cls._LEAVE_DISABLED_TEXT
        return None

    @classmethod
    def _is_bot_unavailable_error(cls, exc: FeishuApiError) -> bool:
        return f"code={cls._FEISHU_BOT_UNAVAILABLE_CODE}" in str(exc)

    def _send_text_notice(self, *, open_id: str, text: str, log_name: str) -> str:
        try:
            self._im.send_text(open_id, text)
            return "sent"
        except FeishuApiError as exc:
            if self._is_bot_unavailable_error(exc):
                logger.warning(
                    "发送{}跳过: open_id={} reason=bot_no_availability code={}",
                    log_name,
                    open_id,
                    self._FEISHU_BOT_UNAVAILABLE_CODE,
                )
                return "skipped"
            logger.warning("发送{}失败: open_id={} error={}", log_name, open_id, str(exc))
            return "failed"
        except Exception as exc:
            logger.warning(
                "发送{}失败: open_id={} error_type={} error={}",
                log_name,
                open_id,
                exc.__class__.__name__,
                str(exc),
            )
            return "failed"

    def _send_cancel_failed_notice(self, *, open_id: str) -> None:
        try:
            self._im.send_text(open_id, self._CANCEL_FAILED_TEXT)
        except Exception as exc:
            logger.error(
                "发送取消失败通知失败: open_id={} error_type={} error={}",
                open_id,
                exc.__class__.__name__,
                str(exc),
            )

    def _send_payment_qr_notice(self, *, open_id: str, log_name: str) -> str:
        try:
            self._im.send_image_file(open_id, self._payment_qr_image_path)
            return "sent"
        except FileNotFoundError as exc:
            logger.warning(
                "发送{}失败: open_id={} image_path={} error_type={} error={}",
                log_name,
                open_id,
                str(self._payment_qr_image_path),
                exc.__class__.__name__,
                str(exc),
            )
            return "failed"
        except FeishuApiError as exc:
            if self._is_bot_unavailable_error(exc):
                logger.warning(
                    "发送{}跳过: open_id={} reason=bot_no_availability code={}",
                    log_name,
                    open_id,
                    self._FEISHU_BOT_UNAVAILABLE_CODE,
                )
                return "skipped"
            logger.warning("发送{}失败: open_id={} error={}", log_name, open_id, str(exc))
            return "failed"
        except Exception as exc:
            logger.warning(
                "发送{}失败: open_id={} error_type={} error={}",
                log_name,
                open_id,
                exc.__class__.__name__,
                str(exc),
            )
            return "failed"

    @staticmethod
    def _resolve_project_path(path_value: str) -> Path:
        path = Path(path_value).expanduser()
        if path.is_absolute():
            return path
        return Path(__file__).resolve().parents[3] / path

    @staticmethod
    def _toast(
        level: str | None,
        content: str | None,
        card_payload: dict[str, Any] | None = None,
    ) -> P2CardActionTriggerResponse:
        result: dict[str, Any] = {}
        if level and content:
            result["toast"] = {"type": level, "content": content}
        if card_payload is not None:
            result["card"] = {"type": "raw", "data": card_payload}
        return P2CardActionTriggerResponse(result)

    @staticmethod
    def _toast_dict(
        level: str | None,
        content: str | None,
        card_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if level and content:
            result["toast"] = {"type": level, "content": content}
        if card_payload is not None:
            result["card"] = {"type": "raw", "data": card_payload}
        return result


def _extract_text_from_message_content(content: str | None) -> str:
    if not content:
        return ""
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return ""
    raw = payload.get("text")
    if raw is None:
        return ""
    return str(raw).strip()


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_card_picker_date(value: str) -> date | None:
    if not value:
        return None

    normalized = value.strip()
    direct = _parse_iso_date(normalized)
    if direct is not None:
        return direct

    head = normalized.split(" ", 1)[0].strip()
    direct = _parse_iso_date(head)
    if direct is not None:
        return direct

    for pattern in ("%Y-%m-%d %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S %z"):
        try:
            return datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    return None


def _parse_meal(value: object) -> Meal | None:
    if value == Meal.LUNCH.value:
        return Meal.LUNCH
    if value == Meal.DINNER.value:
        return Meal.DINNER
    return None


def _pick_rows_by_meal(rows: list[Any], allowed_meals: set[Meal]) -> dict[Meal, Any]:
    selected: dict[Meal, Any] = {}
    for row in rows:
        meal = getattr(row, "meal_type", None)
        if meal not in allowed_meals:
            continue
        selected[meal] = row
    return selected


def _weekday_text(target_date: date) -> str:
    weekdays = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
    return weekdays[target_date.weekday()]


def _format_date_with_weekday(target_date: date) -> str:
    return f"{target_date.isoformat()} {_weekday_text(target_date)}"


def _format_meals(meals: set[Meal]) -> str:
    ordered: list[Meal] = []
    if Meal.LUNCH in meals:
        ordered.append(Meal.LUNCH)
    if Meal.DINNER in meals:
        ordered.append(Meal.DINNER)
    if not ordered:
        return "-"
    return "、".join(meal.value for meal in ordered)


def _resolve_monthly_day(*, year: int, month: int, day_of_month: int) -> date:
    last_day = monthrange(year, month)[1]
    return date(year, month, min(day_of_month, last_day))


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if not text:
        return "0"
    return text


def _format_notice_target(open_id: str, display_name_by_open_id: dict[str, str]) -> str:
    display_name = display_name_by_open_id.get(open_id)
    if display_name:
        return f"{display_name}({open_id})"
    return open_id
