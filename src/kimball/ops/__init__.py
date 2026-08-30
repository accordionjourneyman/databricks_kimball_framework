"""Operational tooling harness (ROADMAP Phase 1).

The diagnostic/operational layer that `kimball inspect`, `kimball recover`
and `kimball explain` compose. Pure logic lives here and is unit-testable
with fake providers; Spark-backed adapters live in ``spark_adapters`` and
are exercised by integration tests only.
"""
