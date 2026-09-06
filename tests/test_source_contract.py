from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from data.generate_sample_data import generate_batch
from retail_lakehouse.contracts import ContractViolation, load_and_validate_manifest


def _generate(root: Path, batch_id: str = "retail-20260906") -> Path:
    return generate_batch(
        root,
        business_date=date(2026, 9, 6),
        batch_id=batch_id,
        seed=17,
        customer_count=12,
        product_count=5,
        order_count=20,
    )


def test_batch_is_deterministic_and_manifest_is_complete(tmp_path: Path) -> None:
    first = _generate(tmp_path / "one")
    second = _generate(tmp_path / "two")
    first_manifest = load_and_validate_manifest(first)
    second_manifest = load_and_validate_manifest(second)

    assert first_manifest == second_manifest
    assert set(first_manifest["entities"]) == {
        "customers",
        "products",
        "orders",
        "order_items",
        "customer_events",
    }
    assert (first / "customer_events.jsonl").read_bytes() == (
        second / "customer_events.jsonl"
    ).read_bytes()


def test_checksum_tampering_fails_closed(tmp_path: Path) -> None:
    batch = _generate(tmp_path)
    with (batch / "orders.csv").open("a", encoding="utf-8") as handle:
        handle.write("999,1,2026-09-06T00:00:00Z,completed,card,USD,2026-09-06T00:00:00Z\n")

    with pytest.raises(ContractViolation, match="row count mismatch|checksum mismatch"):
        load_and_validate_manifest(batch)


def test_manifest_cannot_redirect_an_entity_path(tmp_path: Path) -> None:
    batch = _generate(tmp_path)
    manifest_path = batch / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entities"]["orders"]["path"] = "../orders.csv"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ContractViolation, match="exact path"):
        load_and_validate_manifest(batch)


def test_existing_batch_identity_is_never_overwritten(tmp_path: Path) -> None:
    _generate(tmp_path)
    with pytest.raises(FileExistsError, match="immutable batch"):
        _generate(tmp_path)


def test_bad_data_scenario_is_explicit_in_manifest(tmp_path: Path) -> None:
    batch = generate_batch(
        tmp_path,
        business_date=date(2026, 9, 6),
        batch_id="bad-amount-20260906",
        seed=4,
        customer_count=3,
        product_count=2,
        order_count=3,
        scenario="invalid-amount",
    )
    assert load_and_validate_manifest(batch)["scenario"] == "invalid-amount"
