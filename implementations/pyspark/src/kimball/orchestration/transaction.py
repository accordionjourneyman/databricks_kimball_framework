from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from databricks.sdk.runtime import spark
from delta.tables import DeltaTable

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
        self.spark = spark_session or spark

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
        print(f"TRANSACTION ROLLBACK: Restoring {table_name} to version {version}...")
        try:
            self.spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
            print(f"ROLLBACK COMPLETE: {table_name} restored to {version}.")
        except Exception as e:
            print(f"CRITICAL: Failed to rollback {table_name}: {e}")
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
                print(f"ZOMBIE RECOVERY SKIPPED: Table {table_name} does not exist.")
                return False

            # Check history for commits tagged with this batch_id
            history = DeltaTable.forName(self.spark, table_name).history(10).collect()

            # Find commits made by this batch
            zombie_commits = [
                h
                for h in history
                if (h.get("userMetadata") or "") == batch_id
                or (h.get("userMetadata") or "").endswith(f"batch_id={batch_id}")
            ]

            if not zombie_commits:
                return False

            # We found partial commits. We must rollback to the version BEFORE the first zombie commit.
            # Sort by version ascending to find the first commit
            zombie_commits.sort(key=lambda x: x["version"])
            first_zombie_version = zombie_commits[0]["version"]
            restore_version = first_zombie_version - 1

            if restore_version < 0:
                print(
                    f"WARNING: Cannot rollback {table_name} below version 0. Manual intervention required."
                )
                return False

            print(
                f"ZOMBIE RECOVERY: Found {len(zombie_commits)} commits for crashed batch {batch_id}."
            )
            self._rollback(table_name, restore_version)
            return True

        except Exception as e:
            print(f"ZOMBIE RECOVERY FAILED: {e}")
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
        except Exception as e:
            print(f"WARNING: Could not set commit info metadata: {e}")

        try:
            yield
        except Exception as e:
            # Failure detected - initiate rollback if table state advanced
            current_version = self._get_table_version(table_name)

            if current_version > start_version and start_version >= 0:
                print(
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
