from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.domain.decision import MealPlanDecider
from eatbot.domain.models import Meal, MealScheduleRule


def test_weekday_default_send_both_meals() -> None:
    decider = MealPlanDecider()
    plan = decider.decide(date(2026, 2, 12), rules=[])
    assert plan.meals == {Meal.LUNCH, Meal.DINNER}


def test_weekend_default_send_none() -> None:
    decider = MealPlanDecider()
    plan = decider.decide(date(2026, 2, 14), rules=[])
    assert plan.meals == set()


def test_matched_rule_overrides_default() -> None:
    decider = MealPlanDecider()
    rules = [
        MealScheduleRule(
            start_date=date(2026, 2, 14),
            end_date=date(2026, 2, 14),
            meals={Meal.DINNER},
        )
    ]
    plan = decider.decide(date(2026, 2, 14), rules=rules)
    assert plan.meals == {Meal.DINNER}


def test_multiple_matched_rules_union_meals() -> None:
    decider = MealPlanDecider()
    rules = [
        MealScheduleRule(
            start_date=date(2026, 2, 12),
            end_date=date(2026, 2, 12),
            meals={Meal.LUNCH},
        ),
        MealScheduleRule(
            start_date=date(2026, 2, 10),
            end_date=date(2026, 2, 15),
            meals={Meal.DINNER},
        ),
    ]
    plan = decider.decide(date(2026, 2, 12), rules=rules)
    assert plan.meals == {Meal.LUNCH, Meal.DINNER}
