from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum


class Meal(StrEnum):
    LUNCH = "午餐"
    DINNER = "晚餐"
    CANCELLED = "取消预约"


@dataclass(slots=True)
class UserProfile:
    open_id: str
    display_name: str
    enabled: bool
    lunch_price: Decimal
    dinner_price: Decimal
    meal_preferences: set[Meal] = field(default_factory=set)


@dataclass(slots=True)
class MealScheduleRule:
    start_date: date
    end_date: date
    meals: set[Meal]
    remark: str = ""


@dataclass(slots=True)
class DailyMealPlan:
    date: date
    meals: set[Meal]

    @property
    def send_lunch(self) -> bool:
        return Meal.LUNCH in self.meals

    @property
    def send_dinner(self) -> bool:
        return Meal.DINNER in self.meals
