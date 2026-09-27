"""Check whether target commits can be attributed to known pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from kimball.ops.providers import DeltaCommit, TargetDeltaState


class WriterVerdict(str, Enum):
    CLEAN = "clean"
    SUSPECTED_VIOLATION = "suspected_violation"
    UNKNOWN = "unknown"  # serverless / no tagging


@dataclass(frozen=True)
class WriterContractReport:
    target_table: str
    verdict: WriterVerdict
    suspicious_commits: tuple[DeltaCommit, ...]
    known_batch_ids: tuple[str, ...]


def check_writer_contract(
    delta_state: TargetDeltaState,
    known_batch_ids: tuple[str, ...],
    supports_tagging: bool,
) -> WriterContractReport:
    if not supports_tagging:
        return WriterContractReport(
            delta_state.target_table, WriterVerdict.UNKNOWN, (), known_batch_ids
        )
    known = set(known_batch_ids)
    suspicious = tuple(
        c
        for c in delta_state.commits
        if c.batch_id is not None and c.batch_id not in known
    )
    verdict = WriterVerdict.SUSPECTED_VIOLATION if suspicious else WriterVerdict.CLEAN
    return WriterContractReport(
        delta_state.target_table, verdict, suspicious, known_batch_ids
    )
