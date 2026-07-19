from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


def optimize_table(table_name: str, cluster_by: list[str] | None = None) -> None:
    logger.info(f"Optimizing table {table_name}...")
    get_spark().sql(f"OPTIMIZE {quote_table_name(table_name)}")
    logger.info(
        f"Optimized {table_name}"
        + (f" using Liquid Clustering on {cluster_by}" if cluster_by else "")
    )


def get_last_merge_metrics(
    table_name: str,
    batch_id: str | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Fetch merge commit metrics from Delta history.

    Avoids the FAILED ``DeltaTable.forName`` probe by checking table existence
    first.  Accepts an optional *spark* session so callers that already hold a
    ``PipelineContext`` can reuse it (and its table-existence cache).
    """
    _spark = spark or get_spark()

    # Guard: skip the expensive DeltaTable.forName + history(1) probe when the
    # table does not exist yet (first-run scenario).
    try:
        if not _spark.catalog.tableExists(table_name):
            return {}
    except Exception:
        return {}

    try:
        delta_table = DeltaTable.forName(_spark, table_name)
        if batch_id:
            # Try to find the specific batch commit (reads up to 10 history entries).
            matching = (
                delta_table.history(10).filter(col("userMetadata") == batch_id).first()
            )
            if matching and matching.operationMetrics:
                return dict(matching.operationMetrics)
            logger.debug(
                "Could not find commit with batch_id=%s for %s, falling back to latest",
                batch_id,
                table_name,
            )
        # Fallback: read only the single most recent commit.
        history = delta_table.history(1).select("operationMetrics").first()
        if history and history.operationMetrics:
            return dict(history.operationMetrics)
    except Exception as e:
        logger.debug(f"Could not fetch merge metrics for {table_name}: {e}")
    return {}
