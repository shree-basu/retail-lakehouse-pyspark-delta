from __future__ import annotations

from datetime import datetime

from retail_lakehouse.pipeline import _run_status_frame


def test_run_status_schema_supports_running_and_failed_attempts(spark) -> None:
    manifest = {
        "business_date": "2026-09-06",
        "batch_id": "retail-20260906",
    }
    started = datetime(2026, 9, 6, 1, 0, 0)
    running = _run_status_frame(
        spark,
        attempt_id="attempt-1",
        run_identifier="run-1",
        manifest=manifest,
        status="RUNNING",
        started_at=started,
    ).first()
    failed = _run_status_frame(
        spark,
        attempt_id="attempt-1",
        run_identifier="run-1",
        manifest=manifest,
        status="FAILED",
        started_at=started,
        finished_at=datetime(2026, 9, 6, 1, 1, 0),
        error=RuntimeError("source count mismatch"),
    ).first()

    assert running.finished_at is None
    assert failed.error_type == "RuntimeError"
    assert failed.error_message == "source count mismatch"
