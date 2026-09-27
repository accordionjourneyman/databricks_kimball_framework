from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from kimball.streaming.services.microbatch import StreamingMicroBatchProcessor


@pytest.mark.parametrize(
    ("provided_version", "expected_version_reads"),
    [(7, 0), (None, 1)],
    ids=["known-version", "resolve-once-when-not-supplied"],
)
def test_microbatch_reuses_version_and_commits_it_after_merge(
    provided_version, expected_version_reads
) -> None:
    processor = object.__new__(StreamingMicroBatchProcessor)
    processor.config = SimpleNamespace(
        table_type="fact",
        merge_keys=["order_id"],
        natural_keys=[],
        foreign_keys=[SimpleNamespace(lookup=object())],
    )
    batch_df = MagicMock()
    source_df = MagicMock()
    events = []

    processor._prepare_source_df = MagicMock(return_value=source_df)
    processor.ensure_target_table = MagicMock()
    processor._batch_run_contract_gates = MagicMock(return_value=None)
    processor._batch_reattach_cdf_metadata = MagicMock(return_value=source_df)
    processor._get_max_version = MagicMock(
        side_effect=lambda _df: events.append("read_version") or 7
    )
    processor._batch_resolve_keys_or_nulls = MagicMock(return_value=source_df)
    processor._validate_fks = MagicMock()
    processor._validate_grain = MagicMock()
    processor._batch_merge = MagicMock(
        side_effect=lambda *_args: events.append("merge") or 4
    )
    processor._save_fingerprints = MagicMock()
    processor._batch_commit_watermark = MagicMock(
        side_effect=lambda _name, version, _batch, _rows: events.append(
            ("watermark", version)
        )
    )
    processor._batch_commit_temporal_state = MagicMock()

    processor.process_microbatch(
        batch_df,
        SimpleNamespace(name="silver.orders"),
        42,
        source_version=provided_version,
    )

    assert processor._get_max_version.call_count == expected_version_reads
    assert processor._batch_resolve_keys_or_nulls.call_args.args[-1] == 7
    assert events[-2:] == ["merge", ("watermark", 7)]
