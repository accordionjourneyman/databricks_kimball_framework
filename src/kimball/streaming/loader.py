"""
Streaming CDF read support for Delta tables.

Wraps ``spark.readStream.format("delta").option("readChangeFeed", "true")``
and provides a thin facade that mirrors the batch ``DataLoader.load_cdf``
contract (drop ``update_preimage`` rows, return a CDF DataFrame with
``_change_type`` / ``_commit_version`` / ``_commit_timestamp`` columns).

The dedup window that the batch path applies in-memory is intentionally
**not** moved here: in streaming, dedup is best done inside the
``foreachBatch`` closure so the engine can amortise state. Callers that
need load-time dedup should pass ``primary_keys`` to the merger via the
``source.primary_keys`` field on the existing config; the streaming
loader does not dedup on its own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from kimball.common.config import StreamingSourceConfig


class StreamCdfLoader:
    """Build streaming CDF readers.

    Usage::

        loader = StreamCdfLoader(spark)
        df = loader.stream_cdf(
            "silver.customers",
            config=source.streaming,          # StreamingSourceConfig
        )
        # df is a streaming DataFrame; pass it through foreachBatch
    """

    def __init__(self, spark_session: SparkSession):
        self._spark = spark_session

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def stream_cdf(
        self,
        table_name: str,
        config: StreamingSourceConfig,
        ignore_deletes: bool | None = None,
        ignore_changes: bool | None = None,
    ) -> DataFrame:
        """Open a streaming CDF source for ``table_name``.

        ``config.enabled`` must be True; this method does not check the
        flag and is the low-level primitive the orchestrator calls after
        deciding to enter streaming mode.
        """
        reader = (
            self.spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .option(
                "ignoreDeletes",
                "true" if (ignore_deletes or config.ignore_deletes) else "false",
            )
            .option(
                "ignoreChanges",
                "true" if (ignore_changes or config.ignore_changes) else "false",
            )
        )
        if config.starting_version is not None:
            reader = reader.option("startingVersion", config.starting_version)
        elif config.starting_timestamp is not None:
            reader = reader.option("startingTimestamp", config.starting_timestamp)

        return cast(DataFrame, reader.table(table_name))

    def get_latest_version(self, table_name: str) -> int:
        """Return the latest Delta version of ``table_name``."""
        row = (
            DeltaTable.forName(self.spark, table_name)
            .history(1)
            .select("version")
            .first()
        )
        return int(row["version"]) if row else 0

    def default_checkpoint_location(
        self,
        table_name: str,
        etl_schema: str | None,
    ) -> str:
        """Compute a default ``checkpointLocation`` if the user did not set one.

        The default lives inside the etl_schema (or, falling back to the
        table's own directory, a sibling path) so checkpoints travel with
        the data they describe.
        """
        from kimball.streaming.checkpoint import default_checkpoint_path

        return default_checkpoint_path(
            source_table=table_name,
            etl_schema=etl_schema,
        )
