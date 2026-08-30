"""Unit tests for the pure classification/analysis logic of tools/query_history (1.4).

These exercise ``_classify`` and ``_analyze`` only - no Databricks, no Spark.
The live ``_fetch_history`` path is remote-only (covered by integration tests).
"""

from __future__ import annotations

import os
import sys

# Ensure the repo root (where ``tools/`` lives) is importable.
_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from tools.query_history import _analyze, _classify  # noqa: E402, I001


def _row(text: str, total_ms: int = 100, exec_ms: int = 80, client: str = "kimball"):
    return {
        "statement_id": "x",
        "statement_text": text,
        "statement_type": "",
        "execution_status": "SUCCEEDED",
        "client_application": client,
        "execution_duration_ms": exec_ms,
        "total_duration_ms": total_ms,
        "read_rows": 0,
        "produced_rows": 0,
        "read_files": 0,
        "written_bytes": 0,
        "start_time": "",
        "error_message": "",
    }


def test_classify_merge_is_mutation():
    assert _classify("MERGE INTO gold.t USING src ON ...") == ("table mutation", "")


def test_classify_describe_history_probe_is_waste():
    assert _classify("DESCRIBE HISTORY gold.t LIMIT 1") == (
        "waste",
        "describe_history_probe",
    )


def test_classify_show_columns_is_waste():
    assert _classify("SHOW COLUMNS IN silver.s") == ("waste", "show_columns")


def test_classify_select_is_read():
    assert _classify("SELECT * FROM x") == ("data read", "")


def test_classify_python_fragment_skipped():
    assert _classify("self.foo = 1")[0] == "python_code"


def test_classify_create_is_ddl():
    assert _classify("CREATE TABLE foo (a INT)")[0] == "DDL"


def test_classify_unknown_is_other():
    assert _classify("BOGUS STATEMENT") == ("other", "")


def test_analyze_empty_report():
    report = _analyze([], days=7)
    assert report["total_queries"] == 0
    assert report["window_days"] == 7
    assert report["by_category"] == {}
    assert report["waste"]["total_waste_seconds"] == 0
    assert report["waste"]["waste_pct"] == 0
    assert report["by_client_application"] == {}
    assert report["avg_duration_seconds"] == 0


def test_analyze_aggregates_categories_waste_and_clients():
    rows = [
        _row("MERGE INTO gold.t USING src", total_ms=1000, client="kimball"),
        _row("DESCRIBE HISTORY gold.t LIMIT 1", total_ms=500, client="dbx"),
        _row("SELECT * FROM x", total_ms=200, client="bi"),
    ]
    report = _analyze(rows, days=30)
    assert report["window_days"] == 30
    assert report["total_queries"] == 3
    assert report["total_duration_seconds"] == 1.7
    assert report["by_category"]["table mutation"]["count"] == 1
    assert report["waste"]["by_type"]["describe_history_probe"]["count"] == 1
    assert report["waste"]["total_waste_seconds"] == 0.5
    assert report["by_client_application"]["kimball"]["pct"] == 33.3


def test_analyze_worst_queries_sorted_by_duration():
    rows = [_row("SELECT 1", total_ms=100), _row("SELECT 2", total_ms=9000)]
    report = _analyze(rows, days=1)
    assert report["worst_queries"][0]["duration_seconds"] == 9.0
