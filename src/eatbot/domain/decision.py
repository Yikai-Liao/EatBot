from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from chinese_calendar import get_holiday_detail, is_in_lieu, is_workday

from .models import DailyMealPlan, Meal, MealScheduleRule


def parse_meals(raw_values: object) -> set[Meal]:
    if not isinstance(raw_values, list):
        return set()

    meals: set[Meal] = set()
    for value in raw_values:
        if value == Meal.LUNCH.value:
            meals.add(Meal.LUNCH)
        elif value == Meal.DINNER.value:
            meals.add(Meal.DINNER)
    return meals


@dataclass(slots=True, frozen=True)
class CalendarDayPolicy:
    is_workday: bool
    reason: str


class OfficialWorkdayCalendar:
    def classify(self, target_date: date) -> CalendarDayPolicy:
        try:
            workday = is_workday(target_date)
            _, holiday_name = get_holiday_detail(target_date)
            in_lieu = is_in_lieu(target_date)
        except NotImplementedError as exc:
            raise ValueError(
                f"中国节假日日历暂不支持 {target_date.year} 年，请先升级 chinesecalendar 后再运行"
            ) from exc

        if workday:
            if target_date.weekday() >= 5:
                return CalendarDayPolicy(is_workday=True, reason="调休工作日")
            return CalendarDayPolicy(is_workday=True, reason="工作日")

        if holiday_name is not None:
            return CalendarDayPolicy(is_workday=False, reason="法定节假日")
        if in_lieu:
            return CalendarDayPolicy(is_workday=False, reason="节假日调休放假")
        if target_date.weekday() >= 5:
            return CalendarDayPolicy(is_workday=False, reason="周末休班")
        return CalendarDayPolicy(is_workday=False, reason="官方非工作日")


class MealPlanDecider:
    def __init__(self, *, calendar: OfficialWorkdayCalendar | None = None) -> None:
        self._calendar = calendar or OfficialWorkdayCalendar()

    def decide(self, target_date: date, rules: list[MealScheduleRule]) -> DailyMealPlan:
        matched_rules = [rule for rule in rules if rule.start_date <= target_date <= rule.end_date]

        if matched_rules:
            effective_meals: set[Meal] = set()
            for rule in matched_rules:
                effective_meals = {meal for meal in rule.meals if meal in {Meal.LUNCH, Meal.DINNER}}
            return DailyMealPlan(
                date=target_date,
                meals=effective_meals,
                source="schedule_rule",
                reason="命中用餐定时配置",
                matched_rule_count=len(matched_rules),
            )

        day_policy = self._calendar.classify(target_date)
        if not day_policy.is_workday:
            return DailyMealPlan(
                date=target_date,
                meals=set(),
                source="default_offday",
                reason=day_policy.reason,
            )

        return DailyMealPlan(
            date=target_date,
            meals={Meal.LUNCH, Meal.DINNER},
            source="default_workday",
            reason=day_policy.reason,
        )
