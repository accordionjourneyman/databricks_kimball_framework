from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql.functions import col

from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


def optimize_table(table_name: str, cluster_by: list[str] | None = None) -> None:
    logger.info(f"Optimizing table {table_name}...")
    get_spark().sql(f"OPTIMIZE {quote_table_name(table_name)}")
    logger.info(f"Optimized {table_name}" + (f" using Liquid Clustering on {cluster_by}" if cluster_by else ""))


def get_last_merge_metrics(table_name: str, batch_id: str | None = None) -> dict[str, Any]:
    try:
        delta_table = DeltaTable.forName(get_spark(), table_name)
        if batch_id:
            matching = delta_table.history(10).filter(col("userMetadata") == batch_id).first()
            if matching and matching.operationMetrics:
                return dict(matching.operationMetrics)
            logger.info(f"Warning: Could not find commit with batch_id={batch_id}. Using latest commit metrics.")
        history = delta_table.history(1).select("operationMetrics").first()
        if history and history.operationMetrics:
            return dict(history.operationMetrics)
    except Exception as e:
        logger.debug(f"Could not fetch merge metrics for {table_name}: {e}")
    return {}