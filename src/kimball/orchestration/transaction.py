from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from delta.tables import DeltaTable

from kimball.common.spark_session import get_spark

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TransactionManager:
    """
    Manages ensuring ACID-like properties for ETL pipelines using Delta Time Travel.

    Provides:
    - Context manager for atomic table operations (RESTORE on failure).
    - Commit tagging with batch_ID for traceability.
    - Zombie recovery for rolling back crashed driver commits.
    """

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        self.spark = spark_session or get_spark()

    def _get_table_version(self, table_name: str) -> int:
        """Get the current committed version of a Delta table."""
        try:
            # Optimize: Get version from history(1) instead of full history
            history = DeltaTable.forName(self.spark, table_name).history(1).collect()
            if not history:
                return -1  # Empty table, arguably version 0 or -1
            return int(history[0]["version"])
        except Exception:
            # If table doesn't exist or not a delta table
            return -1

    def _rollback(self, table_name: str, version: int) -> None:
        """Rollback table to a specific version using RESTORE."""
        logger.info(
            f"TRANSACTION ROLLBACK: Restoring {table_name} to version {version}..."
        )
        try:
            self.spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
            logger.info(f"ROLLBACK COMPLETE: {table_name} restored to {version}.")
        except Exception as e:
            logger.info(f"CRITICAL: Failed to rollback {table_name}: {e}")
            raise e

    def recover_zombies(self, table_name: str, batch_id: str) -> bool:
        """
        Check if a previous batch_id has committed data and rollback if necessary.

        Args:
            table_name: Target table to check.
            batch_id: The batch_id of the zombie (crashed) run.

        Returns:
            True if rollback occurred, False otherwise.
        """
        try:
            # Check if table exists first
            if not self.spark.catalog.tableExists(table_name):
                logger.info(
                    f"ZOMBIE RECOVERY SKIPPED: Table {table_name} does not exist."
                )
                return False

            # Check history for commits tagged with this batch_id
            history = DeltaTable.forName(self.spark, table_name).history(10).collect()

            # Find commits made by this batch
            # Row objects don't support .get(), so we use dictionary access
            zombie_commits = [
                h
                for h in history
                if (h["userMetadata"] or "") == batch_id
                or (h["userMetadata"] or "").endswith(f"batch_id={batch_id}")
            ]

            if not zombie_commits:
                return False

            # We found partial commits. We must rollback to the version BEFORE the first zombie commit.
            # Sort by version ascending to find the first commit
            zombie_commits.sort(key=lambda x: x["version"])
            first_zombie_version = zombie_commits[0]["version"]
            restore_version = first_zombie_version - 1

            if restore_version < 0:
                logger.info(
                    f"WARNING: Cannot rollback {table_name} below version 0. Manual intervention required."
                )
                return False

            logger.info(
                f"ZOMBIE RECOVERY: Found {len(zombie_commits)} commits for crashed batch {batch_id}."
            )
            self._rollback(table_name, restore_version)
            return True

        except Exception as e:
            logger.info(f"ZOMBIE RECOVERY FAILED: {e}")
            return False

    @contextmanager
    def table_transaction(
        self, table_name: str, batch_id: str
    ) -> Generator[None, None, None]:
        """
        Context manager that wraps execution in a robust transaction.

        1. Captures start version.
        2. Sets Spark commit metadata (batch_id).
        3. Yields control.
        4. On Exception: Restores table to start version.
        5. Finally: Clears commit metadata.
        """
        start_version = self._get_table_version(table_name)
        # Note: If start_version is -1 (table doesn't exist), we can't rollback to it easily.
        # But usually table creation is separate. If table exists, version >= 0.

        # Set commit tagging - lenient on errors (e.g. Serverless limitations)
        try:
            self.spark.conf.set(
                "spark.databricks.delta.commitInfo.userMetadata", str(batch_id)
            )
        except Exception:
            logger.info(
                "WARNING: Could not set commit info metadata (likely Serverless restriction). Proceeding without commit tagging."
            )

        try:
            yield
        except Exception as e:
            # Failure detected - initiate rollback if table state advanced
            current_version = self._get_table_version(table_name)

            if current_version > start_version and start_version >= 0:
                # FINDING-019: Add messaging for version 0 rollback
                if start_version == 0:
                    logger.info(
                        f"TRANSACTION FAILED on first run. Restoring table to empty state (version 0). "
                        f"You may want to DROP TABLE {table_name} and re-run if this persists."
                    )
                else:
                    logger.info(
                        f"TRANSACTION FAILED: {e}. Initiating rollback from {current_version} to {start_version}."
                    )
                self._rollback(table_name, start_version)

            raise e
        finally:
            # Always clear metadata to avoid polluting future commits
            try:
                self.spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
            except Exception:
                pass
