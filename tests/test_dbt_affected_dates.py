from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _sql(path: str) -> str:
    text = (ROOT / path).read_text(encoding="utf-8").lower()
    return re.sub(r"\s+", " ", text)


def _apply_and_refresh(
    fact: dict[int, dict[str, Any]],
    aggregate: dict[date, dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> set[date]:
    """Cloud-free reference for the two-date recomputation contract."""

    affected_dates: set[date] = set()
    for row in incoming:
        existing = fact.get(row["order_item_id"])
        previous_date = existing["order_date"] if existing else None
        if previous_date == row["order_date"]:
            previous_date = existing.get("previous_order_date") if existing else None
        fact[row["order_item_id"]] = {**row, "previous_order_date": previous_date}
        affected_dates.add(row["order_date"])
        if previous_date is not None:
            affected_dates.add(previous_date)

    for affected_date in affected_dates:
        current = [
            row
            for row in fact.values()
            if row["order_date"] == affected_date and row["status"] == "completed"
        ]
        aggregate[affected_date] = {
            "order_count": len({row["order_id"] for row in current}),
            "items_sold": sum(row["quantity"] for row in current),
            "revenue": sum((row["line_revenue"] for row in current), Decimal("0.00")),
        }
    return affected_dates


def test_dbt_models_capture_old_and_new_dates_and_rebuild_complete_totals() -> None:
    fact_sql = _sql("models/marts/fct_order_items.sql")
    aggregate_sql = _sql("models/marts/agg_daily_sales.sql")

    assert "unique_key='order_item_id'" in fact_sql
    assert "on_schema_change='append_new_columns'" in fact_sql
    assert "left join previous_fact_state previous" in fact_sql
    assert "when previous.order_date <> oi.order_date then previous.order_date" in fact_sql
    assert "else previous.previous_order_date" in fact_sql
    assert "end as previous_order_date" in fact_sql

    assert "unique_key='order_date'" in aggregate_sql
    assert "explode(array(order_date, previous_order_date))" in aggregate_sql
    assert "from {{ ref('fct_order_items') }}" in aggregate_sql
    assert "left join current_daily current" in aggregate_sql
    assert "coalesce(current.order_count, 0)" in aggregate_sql
    assert "coalesce(current.items_sold, 0)" in aggregate_sql
    assert "coalesce(current.revenue, 0)" in aggregate_sql


def test_date_moving_correction_clears_old_date_without_double_counting() -> None:
    first_date = date(2026, 9, 6)
    second_date = date(2026, 9, 7)
    fact: dict[int, dict[str, Any]] = {}
    aggregate: dict[date, dict[str, Any]] = {}
    initial = {
        "order_item_id": 101,
        "order_id": 10,
        "order_date": first_date,
        "status": "completed",
        "quantity": 2,
        "line_revenue": Decimal("25.00"),
    }

    assert _apply_and_refresh(fact, aggregate, [initial]) == {first_date}
    assert aggregate[first_date] == {
        "order_count": 1,
        "items_sold": 2,
        "revenue": Decimal("25.00"),
    }

    correction = {**initial, "order_date": second_date}
    assert _apply_and_refresh(fact, aggregate, [correction]) == {first_date, second_date}
    assert aggregate[first_date] == {
        "order_count": 0,
        "items_sold": 0,
        "revenue": Decimal("0.00"),
    }
    assert aggregate[second_date] == {
        "order_count": 1,
        "items_sold": 2,
        "revenue": Decimal("25.00"),
    }

    assert _apply_and_refresh(fact, aggregate, [correction]) == {first_date, second_date}
    assert aggregate[first_date]["revenue"] == Decimal("0.00")
    assert aggregate[second_date]["revenue"] == Decimal("25.00")
