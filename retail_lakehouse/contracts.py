"""Immutable retail source-batch contract and integrity validation."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from retail_lakehouse import SCHEMA_VERSION

BATCH_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")


@dataclass(frozen=True)
class EntityContract:
    filename: str
    file_format: str
    columns: tuple[str, ...]
    natural_key: tuple[str, ...]


ENTITY_CONTRACTS: dict[str, EntityContract] = {
    "customers": EntityContract(
        "customers.csv",
        "csv",
        (
            "customer_id",
            "customer_name",
            "email",
            "signup_date",
            "country",
            "segment",
            "updated_at",
        ),
        ("customer_id",),
    ),
    "products": EntityContract(
        "products.csv",
        "csv",
        ("product_id", "product_name", "category", "unit_price", "updated_at"),
        ("product_id",),
    ),
    "orders": EntityContract(
        "orders.csv",
        "csv",
        (
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "payment_method",
            "currency",
            "updated_at",
        ),
        ("order_id",),
    ),
    "order_items": EntityContract(
        "order_items.csv",
        "csv",
        (
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "currency",
            "updated_at",
        ),
        ("order_item_id",),
    ),
    "customer_events": EntityContract(
        "customer_events.jsonl",
        "jsonl",
        (
            "event_id",
            "customer_id",
            "event_ts",
            "event_type",
            "session_id",
            "device",
            "attributes",
        ),
        ("event_id",),
    ),
}


class ContractViolation(ValueError):
    """A permanent source-contract failure that must not be retried."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_count(path: Path, file_format: str) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        if file_format == "csv":
            return sum(1 for _ in csv.reader(handle)) - 1
        return sum(1 for line in handle if line.strip())


def _validate_csv_header(path: Path, expected: tuple[str, ...]) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle), None)
    if header != list(expected):
        raise ContractViolation(
            f"{path.name} header mismatch: expected {list(expected)}, found {header}"
        )


def validate_manifest(batch_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate identity, exact paths, schemas, counts, and hashes for one batch."""

    required = {"schema_version", "business_date", "batch_id", "scenario", "entities"}
    missing = required.difference(manifest)
    if missing:
        raise ContractViolation(f"manifest is missing fields: {sorted(missing)}")
    if manifest["schema_version"] != SCHEMA_VERSION:
        raise ContractViolation(f"unsupported schema_version: {manifest['schema_version']!r}")
    try:
        date.fromisoformat(str(manifest["business_date"]))
    except ValueError as exc:
        raise ContractViolation("business_date must be ISO YYYY-MM-DD") from exc
    if not BATCH_ID_PATTERN.fullmatch(str(manifest["batch_id"])):
        raise ContractViolation("batch_id must be a lowercase, path-safe identifier")
    if batch_dir.name != manifest["batch_id"] or batch_dir.parent.name != manifest["business_date"]:
        raise ContractViolation("manifest identity does not match its immutable directory")

    entities = manifest["entities"]
    if not isinstance(entities, dict) or set(entities) != set(ENTITY_CONTRACTS):
        raise ContractViolation("manifest must contain exactly the contracted entities")

    for entity, contract in ENTITY_CONTRACTS.items():
        entry = entities[entity]
        expected_path = contract.filename
        if entry.get("path") != expected_path or entry.get("format") != contract.file_format:
            raise ContractViolation(f"{entity} must use exact path {expected_path}")
        source = batch_dir / expected_path
        if not source.is_file() or source.resolve().parent != batch_dir.resolve():
            raise ContractViolation(f"missing or redirected source object: {expected_path}")
        if contract.file_format == "csv":
            _validate_csv_header(source, contract.columns)
        actual_count = _row_count(source, contract.file_format)
        expected_count = entry.get("row_count")
        if not isinstance(expected_count, int) or expected_count < 0:
            raise ContractViolation(f"{entity} row_count must be a non-negative integer")
        if actual_count != expected_count:
            raise ContractViolation(
                f"{entity} row count mismatch: expected {expected_count}, found {actual_count}"
            )
        actual_hash = sha256_file(source)
        if entry.get("sha256") != actual_hash:
            raise ContractViolation(f"{entity} checksum mismatch")
    return manifest


def load_and_validate_manifest(batch_dir: Path) -> dict[str, Any]:
    manifest_path = batch_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ContractViolation("manifest.json is required")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ContractViolation("manifest.json is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise ContractViolation("manifest root must be an object")
    return validate_manifest(batch_dir, manifest)


def build_manifest(
    batch_dir: Path, *, business_date: date, batch_id: str, scenario: str
) -> dict[str, Any]:
    entities: dict[str, dict[str, Any]] = {}
    for entity, contract in ENTITY_CONTRACTS.items():
        path = batch_dir / contract.filename
        entities[entity] = {
            "path": contract.filename,
            "format": contract.file_format,
            "row_count": _row_count(path, contract.file_format),
            "sha256": sha256_file(path),
            "natural_key": list(contract.natural_key),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "business_date": business_date.isoformat(),
        "batch_id": batch_id,
        "scenario": scenario,
        "entities": entities,
    }
