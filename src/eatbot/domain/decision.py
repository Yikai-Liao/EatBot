from __future__ import annotations

from datetime import date

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


class MealPlanDecider:
    def decide(self, target_date: date, rules: list[MealScheduleRule]) -> DailyMealPlan:
        matched_rules = [rule for rule in rules if rule.start_date <= target_date <= rule.end_date]

        if matched_rules:
            effective_meals: set[Meal] = set()
            for rule in matched_rules:
                effective_meals = {meal for meal in rule.meals if meal in {Meal.LUNCH, Meal.DINNER}}
            return DailyMealPlan(date=target_date, meals=effective_meals)

        if target_date.weekday() >= 5:
            return DailyMealPlan(date=target_date, meals=set())

        return DailyMealPlan(date=target_date, meals={Meal.LUNCH, Meal.DINNER})
