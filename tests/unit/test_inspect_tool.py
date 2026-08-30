"""Unit tests for tools/inspect_etl_control.py.

Mocks Spark so tests run without a Databricks connection.
"""

from __future__ import annotations

from datetime import datetime, timezone

from tools.inspect_etl_control import (
    _color,
    _fmt_dt,
    _fmt_elapsed,
    _pad,
)


class TestFmtDt:
    def test_none_returns_dash(self) -> None:
        assert _fmt_dt(None) == "-"

    def test_datetime_formatted(self) -> None:
        dt = datetime(2026, 7, 19, 14, 30, 0, tzinfo=timezone.utc)
        assert _fmt_dt(dt) == "2026-07-19 14:30"

    def test_str_truncated(self) -> None:
        assert _fmt_dt("2026-07-19T14:30:00")[:16] == "2026-07-19T14:30"


class TestFmtElapsed:
    def test_no_started_returns_dash(self) -> None:
        assert _fmt_elapsed({"started_at": None}) == "-"

    def test_started_only(self) -> None:
        old = datetime(2026, 1, 1, tzinfo=timezone.utc)
        r = _fmt_elapsed({"started_at": old})
        assert "d" in r or "h" in r or "m" in r or "s" in r

    def test_sub_minute(self) -> None:
        start = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 7, 19, 10, 0, 30, tzinfo=timezone.utc)
        assert _fmt_elapsed({"started_at": start, "completed_at": end}) == "30s"

    def test_minutes(self) -> None:
        start = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 7, 19, 10, 2, 0, tzinfo=timezone.utc)
        assert _fmt_elapsed({"started_at": start, "completed_at": end}) == "2.0m"


class TestPad:
    def test_shorter_than_width(self) -> None:
        assert _pad("hi", 5) == "hi   "

    def test_exact_width(self) -> None:
        assert _pad("hello", 5) == "hello"

    def test_longer_than_width(self) -> None:
        assert _pad("hello world", 5) == "hello world"


class TestColor:
    def test_running(self) -> None:
        c, r = _color("RUNNING", True)
        assert "\033[" in c  # ANSI escape
        assert r == "\033[0m"

    def test_failed(self) -> None:
        c, r = _color("FAILED", True)
        assert "\033[" in c

    def test_success_not_colored_when_not_tty(self) -> None:
        c, r = _color("SUCCESS", False)
        assert c == "" and r == ""
