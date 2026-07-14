from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimePolicy:
    is_databricks: bool

    def cluster_clause(
        self, cluster_by: list[str] | None, partition_by: list[str] | None
    ) -> str:
        if self.is_databricks and cluster_by:
            return f"\nCLUSTER BY ({', '.join(cluster_by)})"
        if partition_by:
            return f"\nPARTITIONED BY ({', '.join(partition_by)})"
        return ""


def get_runtime_policy() -> RuntimePolicy:
    return RuntimePolicy(
        is_databricks=bool(
            os.environ.get("DATABRICKS_RUNTIME_VERSION")
            or os.environ.get("DATABRICKS_HOST")
            or os.environ.get("SPARK_REMOTE")
        )
    )
