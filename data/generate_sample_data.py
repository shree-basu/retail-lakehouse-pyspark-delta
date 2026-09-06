"""Generate one deterministic, immutable retail source batch locally."""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import tempfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from retail_lakehouse.contracts import ENTITY_CONTRACTS, build_manifest, validate_manifest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "input"
SCENARIOS = ("normal", "duplicate-order", "orphan-item", "invalid-amount", "schema-evolution")


def _write_csv(path: Path, entity: str, rows: list[dict[str, Any]]) -> None:
    columns = ENTITY_CONTRACTS[entity].columns
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def _iso_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_batch(
    output_root: Path,
    *,
    business_date: date,
    batch_id: str,
    seed: int,
    customer_count: int,
    product_count: int,
    order_count: int,
    scenario: str = "normal",
) -> Path:
    """Write a complete batch once; an existing identity is never overwritten."""

    if scenario not in SCENARIOS:
        raise ValueError(f"unsupported scenario: {scenario}")
    if min(customer_count, product_count, order_count) < 1:
        raise ValueError("entity counts must be positive")
    target = output_root / business_date.isoformat() / batch_id
    if target.exists():
        raise FileExistsError(f"immutable batch already exists: {target}")

    rng = random.Random(seed)
    base_ts = datetime.combine(business_date, datetime.min.time(), tzinfo=UTC)
    customers: list[dict[str, Any]] = []
    for customer_id in range(1, customer_count + 1):
        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"Customer {customer_id}",
                "email": f"customer{customer_id}@example.com",
                "signup_date": (business_date - timedelta(days=rng.randint(30, 730))).isoformat(),
                "country": rng.choice(("US", "UK", "DE", "IN", "CA")),
                "segment": rng.choices(("standard", "premium", "enterprise"), (70, 25, 5))[0],
                "updated_at": _iso_timestamp(base_ts + timedelta(minutes=customer_id)),
            }
        )

    categories = ("electronics", "home", "fashion", "beauty", "sports")
    products: list[dict[str, Any]] = []
    prices: dict[int, Decimal] = {}
    for product_id in range(1, product_count + 1):
        price = Decimal(str(round(rng.uniform(10, 1000), 2)))
        prices[product_id] = price
        products.append(
            {
                "product_id": product_id,
                "product_name": f"Product {product_id}",
                "category": rng.choice(categories),
                "unit_price": f"{price:.2f}",
                "updated_at": _iso_timestamp(base_ts + timedelta(minutes=product_id)),
            }
        )

    orders: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    item_id = 1
    for order_id in range(1, order_count + 1):
        order_ts = base_ts - timedelta(days=rng.randint(0, 30), seconds=rng.randint(0, 86399))
        orders.append(
            {
                "order_id": order_id,
                "customer_id": rng.randint(1, customer_count),
                "order_ts": _iso_timestamp(order_ts),
                "status": rng.choices(("completed", "cancelled", "refunded"), (82, 12, 6))[0],
                "payment_method": rng.choice(("card", "paypal", "bank_transfer")),
                "currency": "USD",
                "updated_at": _iso_timestamp(base_ts + timedelta(minutes=order_id)),
            }
        )
        for _ in range(rng.randint(1, 4)):
            product_id = rng.randint(1, product_count)
            items.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": rng.randint(1, 4),
                    "unit_price": f"{prices[product_id]:.2f}",
                    "currency": "USD",
                    "updated_at": _iso_timestamp(base_ts + timedelta(minutes=item_id)),
                }
            )
            item_id += 1

    events: list[dict[str, Any]] = []
    for event_id in range(1, max(order_count, customer_count) + 1):
        events.append(
            {
                "event_id": f"evt-{business_date:%Y%m%d}-{event_id:07d}",
                "customer_id": rng.randint(1, customer_count),
                "event_ts": _iso_timestamp(base_ts - timedelta(seconds=rng.randint(0, 86399))),
                "event_type": rng.choice(("page_view", "search", "add_to_cart", "checkout")),
                "session_id": f"session-{rng.randint(1, max(1, customer_count // 2))}",
                "device": {"type": rng.choice(("mobile", "desktop", "tablet")), "os": "synthetic"},
                "attributes": {"campaign": rng.choice(("direct", "email", "paid"))},
            }
        )

    if scenario == "duplicate-order":
        duplicate = dict(orders[0])
        duplicate["updated_at"] = _iso_timestamp(base_ts + timedelta(hours=2))
        orders.append(duplicate)
    elif scenario == "orphan-item":
        items[0]["order_id"] = order_count + 999
    elif scenario == "invalid-amount":
        items[0]["unit_price"] = "-0.01"
    elif scenario == "schema-evolution":
        events[0]["attributes"]["experiment_id"] = "checkout-v2"

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{batch_id}-", dir=target.parent))
    try:
        _write_csv(staging / "customers.csv", "customers", customers)
        _write_csv(staging / "products.csv", "products", products)
        _write_csv(staging / "orders.csv", "orders", orders)
        _write_csv(staging / "order_items.csv", "order_items", items)
        _write_jsonl(staging / "customer_events.jsonl", events)
        manifest = build_manifest(
            staging,
            business_date=business_date,
            batch_id=batch_id,
            scenario=scenario,
        )
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        staging.rename(target)
        validate_manifest(target, manifest)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--business-date", type=date.fromisoformat, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--orders", type=int, default=5_000)
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    target = generate_batch(
        args.output_root,
        business_date=args.business_date,
        batch_id=args.batch_id,
        seed=args.seed,
        customer_count=args.customers,
        product_count=args.products,
        order_count=args.orders,
        scenario=args.scenario,
    )
    print(target)


if __name__ == "__main__":
    main()
