"""Normalized, durable audit records for data repair operations.

The ledger is deliberately separate from ``etl_control``: control rows are
mutable processing cursors, while repair intent, inputs, attempts, commits,
and lifecycle events are historical evidence.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from kimball.common.utils import quote_table_name

_TABLES = {
    "revision": "etl_repair_revision",
    "correction": "etl_repair_input_correction",
    "plan": "etl_repair_plan",
    "step": "etl_repair_step",
    "write": "etl_repair_write",
    "dependency": "etl_repair_step_dependency",
    "read": "etl_repair_read",
    "attempt_read": "etl_repair_attempt_read",
    "attempt": "etl_repair_attempt",
    "step_attempt": "etl_repair_step_attempt",
    "commit": "etl_repair_commit",
    "event": "etl_repair_event",
}

_DDL = {
    "revision": """repair_id STRING NOT NULL, revision INT NOT NULL,
        repair_key STRING NOT NULL, reason STRING NOT NULL, owner STRING,
        artifact_uri STRING, artifact_digest STRING,
        supersedes_repair_id STRING, recorded_at TIMESTAMP NOT NULL""",
    "correction": """repair_id STRING NOT NULL, source_table STRING NOT NULL,
        bad_version BIGINT NOT NULL, replacement_version BIGINT NOT NULL,
        source_generation STRING NOT NULL, recorded_at TIMESTAMP NOT NULL""",
    "plan": """plan_id STRING NOT NULL, repair_id STRING NOT NULL,
        manifest_digest STRING, config_digest STRING, runtime_profile STRING,
        strategy STRING NOT NULL, publication_policy STRING NOT NULL,
        created_at TIMESTAMP NOT NULL""",
    "step": """step_id STRING NOT NULL, plan_id STRING NOT NULL,
        ordinal INT NOT NULL, target_table STRING NOT NULL,
        target_generation STRING, strategy STRING NOT NULL,
        scope_ref STRING, scope_digest STRING,
        expected_current_version BIGINT, rollback_version BIGINT,
        operation_token STRING NOT NULL""",
    "write": """write_id STRING NOT NULL, step_id STRING NOT NULL,
        target_table STRING NOT NULL, write_role STRING NOT NULL,
        target_existed_before BOOLEAN NOT NULL, target_generation STRING,
        target_version_before BIGINT, rollback_version BIGINT""",
    "dependency": """step_id STRING NOT NULL,
        depends_on_step_id STRING NOT NULL""",
    "read": """read_id STRING NOT NULL, step_id STRING NOT NULL,
        dataset_table STRING NOT NULL, source_role STRING NOT NULL,
        read_mode STRING NOT NULL, snapshot_version BIGINT,
        cdf_start_version BIGINT, cdf_end_version BIGINT,
        schema_digest STRING, dataset_generation STRING,
        source_step_id STRING, prior_watermark BIGINT,
        reproducible BOOLEAN NOT NULL""",
    "attempt_read": """attempt_read_id STRING NOT NULL,
        attempt_id STRING NOT NULL, step_id STRING NOT NULL,
        dataset_table STRING NOT NULL, source_role STRING NOT NULL,
        read_mode STRING NOT NULL, snapshot_version BIGINT,
        cdf_start_version BIGINT, cdf_end_version BIGINT,
        schema_digest STRING, dataset_generation STRING,
        reproducible BOOLEAN NOT NULL, observed_at TIMESTAMP NOT NULL""",
    "attempt": """attempt_id STRING NOT NULL, plan_id STRING NOT NULL,
        parent_attempt_id STRING, actor STRING, environment STRING,
        started_at TIMESTAMP NOT NULL""",
    "step_attempt": """step_attempt_id STRING NOT NULL,
        attempt_id STRING NOT NULL, step_id STRING NOT NULL,
        target_existed_before BOOLEAN NOT NULL,
        target_version_before BIGINT, started_at TIMESTAMP NOT NULL""",
    "commit": """commit_id STRING NOT NULL,
        step_attempt_id STRING NOT NULL, target_table STRING NOT NULL,
        target_generation STRING, target_version_before BIGINT,
        target_version BIGINT NOT NULL,
        operation STRING, operation_token STRING,
        repair_operation_token STRING NOT NULL, attributed BOOLEAN NOT NULL,
        rollback_version BIGINT, committed_at TIMESTAMP NOT NULL""",
    "event": """event_id STRING NOT NULL, attempt_id STRING NOT NULL,
        step_attempt_id STRING, event_type STRING NOT NULL,
        status STRING NOT NULL, occurred_at TIMESTAMP NOT NULL,
        elapsed_ms BIGINT, error_message STRING, evidence_ref STRING""",
}


@dataclass(frozen=True)
class RepairRead:
    """One pinned input used by a repair step.

    Exactly one of ``snapshot_version`` or the inclusive CDF range must be
    supplied. Full snapshots with no known version should be recorded with
    ``reproducible=False`` by a caller-specific extension, not version zero.
    """

    dataset_table: str
    source_role: str = "source"
    read_mode: str = "snapshot"
    snapshot_version: int | None = None
    cdf_start_version: int | None = None
    cdf_end_version: int | None = None
    schema_digest: str | None = None
    dataset_generation: str | None = None
    source_step_id: str | None = None
    prior_watermark: int | None = None
    reproducible: bool = True

    def validate(self) -> None:
        if self.read_mode == "snapshot":
            if self.snapshot_version is None or self.snapshot_version < 0:
                raise ValueError(
                    "snapshot reads require a non-negative exact snapshot_version"
                )
            if self.cdf_start_version is not None or self.cdf_end_version is not None:
                raise ValueError("snapshot reads cannot also specify CDF bounds")
        elif self.read_mode == "cdf":
            if self.snapshot_version is not None:
                raise ValueError("CDF reads cannot specify snapshot_version")
            if self.cdf_start_version is None or self.cdf_end_version is None:
                raise ValueError("CDF reads require inclusive start and end versions")
            if self.cdf_start_version < 0 or self.cdf_end_version < 0:
                raise ValueError("CDF versions must be non-negative")
            if self.cdf_start_version > self.cdf_end_version:
                raise ValueError("CDF start version must be <= end version")
        elif self.read_mode == "dependency":
            if self.snapshot_version is not None:
                raise ValueError("dependency reads resolve their version during apply")
            if not self.source_step_id:
                raise ValueError("dependency reads require source_step_id")
        else:
            raise ValueError("read_mode must be 'snapshot', 'cdf', or 'dependency'")


@dataclass(frozen=True)
class RepairInputCorrection:
    source_table: str
    bad_version: int
    replacement_version: int
    source_generation: str


@dataclass(frozen=True)
class RepairPlanStep:
    target_table: str
    strategy: str
    target_generation: str | None = None
    scope_ref: str | None = None
    scope_digest: str | None = None
    expected_current_version: int | None = None
    rollback_version: int | None = None
    reads: tuple[RepairRead, ...] = ()
    writes: tuple[RepairWriteTarget, ...] = ()
    depends_on: tuple[int, ...] = ()


@dataclass(frozen=True)
class RepairWriteTarget:
    target_table: str
    write_role: str = "target"
    target_existed_before: bool = False
    target_generation: str | None = None
    target_version_before: int | None = None
    rollback_version: int | None = None


class DeltaRepairLedger:
    """Delta-backed normalized repair ledger.

    Delta does not enforce uniqueness for these logical keys on every
    deployment. IDs are generated once per intent, and callers must serialize
    writers for a repair ID. Lifecycle rows are append-only; status and elapsed
    time are represented by terminal events rather than overwritten records.
    """

    def __init__(self, spark: Any, etl_schema: str) -> None:
        self.spark = spark
        self.schema = etl_schema
        self.tables = {
            key: (name if "." in name else f"{etl_schema}.{name}")
            for key, name in _TABLES.items()
        }
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        for key, columns in _DDL.items():
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS "
                f"{quote_table_name(self.tables[key])} ({columns}) USING DELTA"
            )

    def _append(self, key: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from pyspark.sql.types import (
            BooleanType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        type_map = {
            "STRING": StringType,
            "INT": IntegerType,
            "BIGINT": LongType,
            "BOOLEAN": BooleanType,
            "TIMESTAMP": TimestampType,
        }
        fields = []
        for declaration in _DDL[key].replace("\n", " ").split(","):
            parts = declaration.strip().split()
            fields.append(
                StructField(
                    parts[0], type_map[parts[1]](), "NOT NULL" not in declaration
                )
            )
        frame = self.spark.createDataFrame(rows, schema=StructType(fields))
        frame.write.format("delta").mode("append").saveAsTable(self.tables[key])

    def create_plan(
        self,
        *,
        repair_id: str,
        reason: str,
        steps: tuple[RepairPlanStep, ...],
        owner: str | None = None,
        repair_key: str | None = None,
        revision: int = 1,
        supersedes_repair_id: str | None = None,
        artifact_uri: str | None = None,
        artifact_digest: str | None = None,
        manifest_digest: str | None = None,
        config_digest: str | None = None,
        runtime_profile: str | None = None,
        strategy: str = "operator_defined",
        publication_policy: str = "in_place_single_writer",
        corrections: tuple[RepairInputCorrection, ...] = (),
    ) -> str:
        if not repair_id.strip() or not reason.strip() or not steps:
            raise ValueError("repair_id, reason, and at least one step are required")
        if revision < 1:
            raise ValueError("revision must be positive")
        for step in steps:
            for read in step.reads:
                read.validate()
        for index, step in enumerate(steps):
            if any(dep < 0 or dep >= index for dep in step.depends_on):
                raise ValueError(
                    "dependencies must refer to earlier steps in topological order"
                )
        for correction in corrections:
            if correction.bad_version < 0 or correction.replacement_version < 0:
                raise ValueError("correction versions must be non-negative")
            if correction.bad_version == correction.replacement_version:
                raise ValueError("bad and replacement versions must differ")

        now = _now()
        plan_id = _stable_id(repair_id, "plan")
        step_ids = [
            _stable_id(repair_id, f"step:{index}") for index in range(len(steps))
        ]
        step_rows = []
        write_rows = []
        read_rows = []
        dependency_rows = []
        for index, step in enumerate(steps):
            step_id = step_ids[index]
            step_rows.append(
                {
                    "step_id": step_id,
                    "plan_id": plan_id,
                    "ordinal": index,
                    "target_table": step.target_table,
                    "target_generation": step.target_generation,
                    "strategy": step.strategy,
                    "scope_ref": step.scope_ref,
                    "scope_digest": step.scope_digest,
                    "expected_current_version": step.expected_current_version,
                    "rollback_version": step.rollback_version,
                    "operation_token": _stable_id(repair_id, f"operation:{index}"),
                }
            )
            for write_index, write in enumerate(step.writes):
                write_rows.append(
                    {
                        "write_id": _stable_id(
                            repair_id,
                            f"write:{index}:{write_index}:{write.target_table}",
                        ),
                        "step_id": step_id,
                        "target_table": write.target_table,
                        "write_role": write.write_role,
                        "target_existed_before": write.target_existed_before,
                        "target_generation": write.target_generation,
                        "target_version_before": write.target_version_before,
                        "rollback_version": write.rollback_version,
                    }
                )
            for read_index, read in enumerate(step.reads):
                read_rows.append(
                    {
                        "read_id": _stable_id(repair_id, f"read:{index}:{read_index}"),
                        "step_id": step_id,
                        "dataset_table": read.dataset_table,
                        "source_role": read.source_role,
                        "read_mode": read.read_mode,
                        "snapshot_version": read.snapshot_version,
                        "cdf_start_version": read.cdf_start_version,
                        "cdf_end_version": read.cdf_end_version,
                        "schema_digest": read.schema_digest,
                        "dataset_generation": read.dataset_generation,
                        "source_step_id": read.source_step_id,
                        "prior_watermark": read.prior_watermark,
                        "reproducible": read.reproducible,
                    }
                )
            for dependency_index in step.depends_on:
                dependency_rows.append(
                    {
                        "step_id": step_id,
                        "depends_on_step_id": step_ids[dependency_index],
                    }
                )

        revision_rows = self._rows("revision", repair_id)
        if revision_rows:
            plan_rows = self._rows("plan", repair_id)
            if len(revision_rows) != 1 or len(plan_rows) != 1:
                raise RuntimeError(
                    f"repair {repair_id} has duplicate or incomplete intent; inspect ledger"
                )
            expected_revision = {
                "revision": revision,
                "repair_key": repair_key or repair_id,
                "reason": reason,
                "owner": owner,
                "artifact_uri": artifact_uri,
                "artifact_digest": artifact_digest,
                "supersedes_repair_id": supersedes_repair_id,
            }
            expected_plan = {
                "manifest_digest": manifest_digest,
                "config_digest": config_digest,
                "runtime_profile": runtime_profile,
                "strategy": strategy,
                "publication_policy": publication_policy,
            }
            if any(
                revision_rows[0].get(key) != value
                for key, value in expected_revision.items()
            ) or any(
                plan_rows[0].get(key) != value for key, value in expected_plan.items()
            ):
                raise RuntimeError(
                    f"repair {repair_id} already has a different frozen revision"
                )

            actual_steps = self._rows("step", plan_id)
            actual_writes = self._rows_for_plan_steps(repair_id, "write")
            actual_reads = self._rows_for_plan_steps(repair_id, "read")
            actual_dependencies = self._rows_for_plan_steps(repair_id, "dependency")
            actual_corrections = self._rows("correction", repair_id)
            checks = (
                (
                    actual_steps,
                    step_rows,
                    (
                        "ordinal",
                        "target_table",
                        "target_generation",
                        "strategy",
                        "scope_ref",
                        "scope_digest",
                        "expected_current_version",
                        "rollback_version",
                        "operation_token",
                    ),
                ),
                (
                    actual_writes,
                    write_rows,
                    (
                        "write_id",
                        "step_id",
                        "target_table",
                        "write_role",
                        "target_existed_before",
                        "target_generation",
                        "target_version_before",
                        "rollback_version",
                    ),
                ),
                (
                    actual_reads,
                    read_rows,
                    (
                        "read_id",
                        "step_id",
                        "dataset_table",
                        "source_role",
                        "read_mode",
                        "snapshot_version",
                        "cdf_start_version",
                        "cdf_end_version",
                        "schema_digest",
                        "dataset_generation",
                        "source_step_id",
                        "prior_watermark",
                        "reproducible",
                    ),
                ),
                (
                    actual_dependencies,
                    dependency_rows,
                    ("step_id", "depends_on_step_id"),
                ),
                (
                    actual_corrections,
                    [
                        {
                            "source_table": row.source_table,
                            "bad_version": row.bad_version,
                            "replacement_version": row.replacement_version,
                            "source_generation": row.source_generation,
                        }
                        for row in corrections
                    ],
                    (
                        "source_table",
                        "bad_version",
                        "replacement_version",
                        "source_generation",
                    ),
                ),
            )
            for actual, expected, fields in checks:
                actual_values = sorted(
                    (tuple(row.get(field) for field in fields) for row in actual),
                    key=repr,
                )
                expected_values = sorted(
                    (tuple(row.get(field) for field in fields) for row in expected),
                    key=repr,
                )
                if actual_values != expected_values:
                    raise RuntimeError(
                        f"repair {repair_id} has an incomplete or different frozen plan"
                    )
            return plan_id

        self._append(
            "revision",
            [
                {
                    "repair_id": repair_id,
                    "revision": revision,
                    "repair_key": repair_key or repair_id,
                    "reason": reason,
                    "owner": owner,
                    "artifact_uri": artifact_uri,
                    "artifact_digest": artifact_digest,
                    "supersedes_repair_id": supersedes_repair_id,
                    "recorded_at": now,
                }
            ],
        )
        self._append(
            "correction",
            [
                {
                    "repair_id": repair_id,
                    "source_table": correction.source_table,
                    "bad_version": correction.bad_version,
                    "replacement_version": correction.replacement_version,
                    "source_generation": correction.source_generation,
                    "recorded_at": now,
                }
                for correction in corrections
            ],
        )
        self._append(
            "plan",
            [
                {
                    "plan_id": plan_id,
                    "repair_id": repair_id,
                    "manifest_digest": manifest_digest,
                    "config_digest": config_digest,
                    "runtime_profile": runtime_profile,
                    "strategy": strategy,
                    "publication_policy": publication_policy,
                    "created_at": now,
                }
            ],
        )
        self._append("step", step_rows)
        self._append("write", write_rows)
        self._append("dependency", dependency_rows)
        self._append("read", read_rows)
        return plan_id

    def start_repair_attempt(
        self,
        *,
        repair_id: str,
        actor: str | None = None,
        environment: str | None = None,
        parent_attempt_id: str | None = None,
    ) -> str:
        plan_id = _stable_id(repair_id, "plan")
        if not self._rows("plan", repair_id):
            raise ValueError(f"repair plan {repair_id} does not exist")
        attempt_id = str(uuid.uuid4())
        self._append(
            "attempt",
            [
                {
                    "attempt_id": attempt_id,
                    "plan_id": plan_id,
                    "parent_attempt_id": parent_attempt_id,
                    "actor": actor,
                    "environment": environment,
                    "started_at": _now(),
                }
            ],
        )
        self.append_event(
            attempt_id=attempt_id,
            step_attempt_id=None,
            event_type="STARTED",
            status="RUNNING",
        )
        return attempt_id

    def start_step_attempt(
        self,
        *,
        repair_id: str,
        attempt_id: str,
        step_index: int,
        target_existed_before: bool,
        target_version_before: int | None,
    ) -> str:
        steps = self._rows("step", _stable_id(repair_id, "plan"))
        step = next((row for row in steps if int(row["ordinal"]) == step_index), None)
        if step is None:
            raise ValueError(f"repair plan {repair_id} has no step {step_index}")
        step_attempt_id = str(uuid.uuid4())
        now = _now()
        self._append(
            "step_attempt",
            [
                {
                    "step_attempt_id": step_attempt_id,
                    "attempt_id": attempt_id,
                    "step_id": step["step_id"],
                    "target_existed_before": target_existed_before,
                    "target_version_before": target_version_before,
                    "started_at": now,
                }
            ],
        )
        self.append_event(
            attempt_id=attempt_id,
            step_attempt_id=step_attempt_id,
            event_type="STARTED",
            status="RUNNING",
            occurred_at=now,
        )
        return step_attempt_id

    def start_attempt(
        self,
        *,
        repair_id: str,
        step_index: int = 0,
        target_existed_before: bool,
        target_version_before: int | None,
        actor: str | None = None,
        environment: str | None = None,
        parent_attempt_id: str | None = None,
    ) -> tuple[str, str]:
        """Compatibility helper for one-step operations such as RESTORE."""
        attempt_id = self.start_repair_attempt(
            repair_id=repair_id,
            actor=actor,
            environment=environment,
            parent_attempt_id=parent_attempt_id,
        )
        step_attempt_id = self.start_step_attempt(
            repair_id=repair_id,
            attempt_id=attempt_id,
            step_index=step_index,
            target_existed_before=target_existed_before,
            target_version_before=target_version_before,
        )
        return attempt_id, step_attempt_id

    def record_commit(
        self,
        *,
        repair_id: str,
        step_attempt_id: str,
        target_table: str,
        target_generation: str | None,
        target_version_before: int | None,
        target_version: int,
        operation: str | None,
        operation_token: str | None,
        repair_operation_token: str,
        attributed: bool,
        rollback_version: int | None,
        committed_at: datetime | None = None,
    ) -> None:
        commit_id = _stable_id(repair_id, f"commit:{target_table}:{target_version}")
        if self._rows("commit", commit_id):
            return
        self._append(
            "commit",
            [
                {
                    "commit_id": commit_id,
                    "step_attempt_id": step_attempt_id,
                    "target_table": target_table,
                    "target_generation": target_generation,
                    "target_version_before": target_version_before,
                    "target_version": target_version,
                    "operation": operation,
                    "operation_token": operation_token,
                    "repair_operation_token": repair_operation_token,
                    "attributed": attributed,
                    "rollback_version": rollback_version,
                    "committed_at": committed_at or _now(),
                }
            ],
        )

    def record_attempt_reads(
        self,
        *,
        repair_id: str,
        attempt_id: str,
        step_index: int,
        reads: tuple[RepairRead, ...],
    ) -> None:
        steps = self._rows("step", _stable_id(repair_id, "plan"))
        step = next((row for row in steps if int(row["ordinal"]) == step_index), None)
        if step is None:
            raise ValueError(f"repair plan {repair_id} has no step {step_index}")
        now = _now()
        rows = []
        for index, read in enumerate(reads):
            read.validate()
            if read.read_mode == "dependency":
                raise ValueError("attempt reads must contain resolved versions")
            rows.append(
                {
                    "attempt_read_id": _stable_id(
                        attempt_id, f"read:{step_index}:{index}:{read.dataset_table}"
                    ),
                    "attempt_id": attempt_id,
                    "step_id": step["step_id"],
                    "dataset_table": read.dataset_table,
                    "source_role": read.source_role,
                    "read_mode": read.read_mode,
                    "snapshot_version": read.snapshot_version,
                    "cdf_start_version": read.cdf_start_version,
                    "cdf_end_version": read.cdf_end_version,
                    "schema_digest": read.schema_digest,
                    "dataset_generation": read.dataset_generation,
                    "reproducible": read.reproducible,
                    "observed_at": now,
                }
            )
        self._append("attempt_read", rows)

    def append_event(
        self,
        *,
        attempt_id: str,
        step_attempt_id: str | None,
        event_type: str,
        status: str,
        occurred_at: datetime | None = None,
        elapsed_ms: int | None = None,
        error_message: str | None = None,
        evidence_ref: str | None = None,
    ) -> None:
        occurred = occurred_at or _now()
        event_key = ":".join(
            (
                attempt_id,
                step_attempt_id or "",
                event_type,
                status,
                occurred.isoformat(),
            )
        )
        self._append(
            "event",
            [
                {
                    "event_id": hashlib.sha256(event_key.encode()).hexdigest(),
                    "attempt_id": attempt_id,
                    "step_attempt_id": step_attempt_id,
                    "event_type": event_type,
                    "status": status,
                    "occurred_at": occurred,
                    "elapsed_ms": elapsed_ms,
                    "error_message": error_message,
                    "evidence_ref": evidence_ref,
                }
            ],
        )

    def status(self, repair_id: str) -> dict[str, Any]:
        return {
            "repair_id": repair_id,
            "revision": self._rows("revision", repair_id),
            "corrections": self._rows("correction", repair_id),
            "plan": self._rows("plan", repair_id),
            "steps": self._rows("step", _stable_id(repair_id, "plan")),
            "writes": self._rows_for_plan_steps(repair_id, "write"),
            "reads": self._rows_for_plan_steps(repair_id, "read"),
            "dependencies": self._rows_for_plan_steps(repair_id, "dependency"),
            "attempts": self._rows_for_plan_attempts(repair_id, "attempt"),
            "attempt_reads": self._rows_for_plan_attempts(repair_id, "attempt_read"),
            "step_attempts": self._rows_for_plan_attempts(repair_id, "step_attempt"),
            "commits": self._rows_for_plan_attempts(repair_id, "commit"),
            "events": self._rows_for_plan_attempts(repair_id, "event"),
        }

    def _rows(self, key: str, value: str) -> list[dict[str, Any]]:
        escaped = value.replace("'", "''")
        return [
            _as_dict(row)
            for row in self.spark.sql(
                f"SELECT * FROM {quote_table_name(self.tables[key])} "
                f"WHERE {self._key_column(key)} = '{escaped}'"
            ).collect()
        ]

    def _rows_for_plan_steps(self, repair_id: str, key: str) -> list[dict[str, Any]]:
        steps = self._rows("step", _stable_id(repair_id, "plan"))
        if not steps:
            return []
        step_ids = ", ".join(f"'{r['step_id']}'" for r in steps)
        return [
            _as_dict(row)
            for row in self.spark.sql(
                f"SELECT * FROM {quote_table_name(self.tables[key])} "
                f"WHERE step_id IN ({step_ids})"
            ).collect()
        ]

    def _rows_for_plan_attempts(self, repair_id: str, key: str) -> list[dict[str, Any]]:
        attempts = self._rows("attempt", _stable_id(repair_id, "plan"))
        if not attempts:
            return []
        attempt_ids = ", ".join(f"'{r['attempt_id']}'" for r in attempts)
        if key == "attempt":
            return attempts
        if key in {"step_attempt", "attempt_read", "event"}:
            predicate = f"attempt_id IN ({attempt_ids})"
        else:
            step_attempts = self._rows_for_plan_attempts(repair_id, "step_attempt")
            if not step_attempts:
                return []
            ids = ", ".join(f"'{r['step_attempt_id']}'" for r in step_attempts)
            predicate = f"step_attempt_id IN ({ids})"
        return [
            _as_dict(row)
            for row in self.spark.sql(
                f"SELECT * FROM {quote_table_name(self.tables[key])} WHERE {predicate}"
            ).collect()
        ]

    @staticmethod
    def _key_column(key: str) -> str:
        return {
            "revision": "repair_id",
            "plan": "repair_id",
            "step": "plan_id",
            "write": "write_id",
            "read": "read_id",
            "correction": "repair_id",
            "commit": "commit_id",
            "attempt": "plan_id",
        }[key]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_id(namespace: str, name: str) -> str:
    return stable_repair_id(namespace, name)


def stable_repair_id(namespace: str, name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"kimball-repair:{namespace}:{name}"))


def _as_dict(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)


def status_json(ledger: DeltaRepairLedger, repair_id: str) -> str:
    return json.dumps(ledger.status(repair_id), indent=2, sort_keys=True, default=str)
