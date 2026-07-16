#!/usr/bin/env python3
"""
Databricks Unity Catalog cleanup tool for the Kimball Framework.

Scans for stale test schemas (databases) left behind by integration test
runs and drops them.  This is needed because:

  - Unity Catalog enforces a **50-schema-per-catalog** quota.
  - Each integration test run creates 6+ schemas (``kimball_*``).
  - If a test run is interrupted (Ctrl+C, crash, OOM), the ``test_db``
    fixture's teardown never runs and schemas leak permanently.

Usage::

    # Preview what would be deleted (dry-run)
    python tools/cleanup_databricks.py --dry-run

    # Delete all test schemas older than 7 days
    python tools/cleanup_databricks.py --older-than 7

    # Delete ALL test schemas (interactive confirmation required)
    python tools/cleanup_databricks.py --all

    # Use a specific catalog instead of the default
    python tools/cleanup_databricks.py --catalog my_catalog --dry-run

    # Load credentials from a custom .env file
    python tools/cleanup_databricks.py --env-file /path/to/.env --dry-run

Exit codes::

    0  — success (or dry-run completed)
    1  — missing credentials / connection error
    2  — user cancelled
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

# Schemas matching this prefix are considered "test schemas" created by the
# Kimball Framework integration test suite.
TEST_SCHEMA_PREFIXES = ("kimball_", "test_db_", "spark-warehouse-")


def _load_env_file(env_file: str | None) -> None:
    """Load environment variables from a .env file if python-dotenv is available."""
    if env_file is None:
        env_file = str(REPO_ROOT / ".env")
    if not os.path.exists(env_file):
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
        print(f"Loaded environment from {env_file}")
    except ImportError:
        print("warning: python-dotenv not installed; .env file ignored")


def _ensure_credentials() -> tuple[str, str]:
    """Verify that DATABRICKS_HOST and DATABRICKS_TOKEN are available."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print(
            "error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set.\n"
            "Set them in the environment or in a .env file at the repo root."
        )
        sys.exit(1)
    return host.rstrip("/"), token


def _is_test_schema(name: str) -> bool:
    """Return True if the schema name matches a known test prefix."""
    return any(name.startswith(p) for p in TEST_SCHEMA_PREFIXES)


def _get_catalog(ws: Any, catalog_name: str | None) -> str:
    """Resolve the catalog to scan.

    If ``catalog_name`` is provided, use it.  Otherwise try the workspace's
    default catalog, falling back to ``hive_metastore``.
    """
    if catalog_name:
        return catalog_name

    try:
        default = ws.catalogs.default()
        return default.name
    except Exception:
        return "hive_metastore"


def _list_test_schemas(ws: Any, catalog: str) -> list[dict]:
    """Return a list of test schemas in the given catalog.

    Each entry has keys: ``name``, ``created_at`` (ISO string), ``table_count``.
    """
    schemas: list[dict] = []
    try:
        for schema in ws.schemas.list(catalog_name=catalog):
            if _is_test_schema(schema.name):
                schemas.append({
                    "name": f"{catalog}.{schema.name}",
                    "created_at": str(getattr(schema, "created_at", "unknown")),
                    "table_count": getattr(schema, "table_count", 0),
                })
    except Exception as e:
        print(f"error: could not list schemas in catalog '{catalog}': {e}")
        sys.exit(1)

    return sorted(schemas, key=lambda s: s["name"])


def _parse_age(created_at_str: str) -> float | None:
    """Parse a Databricks timestamp string into a datetime, or return None."""
    if not created_at_str or created_at_str == "unknown":
        return None
    try:
        # Databricks timestamps are typically ISO 8601 with timezone info
        dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400  # days
    except (ValueError, TypeError):
        return None


def _drop_schema(ws: Any, full_name: str, dry_run: bool) -> bool:
    """Drop a schema with CASCADE.  Return True on success."""
    if dry_run:
        print(f"  [dry-run] would drop: {full_name}")
        return True

    try:
        ws.schemas.delete(full_name, force=True)
        print(f"  dropped: {full_name}")
        return True
    except Exception as e:
        print(f"  FAILED to drop {full_name}: {e}")
        return False


def _confirm(prompt: str) -> bool:
    """Ask the user for yes/no confirmation."""
    response = input(f"{prompt} [y/N] ").strip().lower()
    return response in ("y", "yes")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean up stale test schemas from Databricks Unity Catalog.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Unity Catalog name to scan (default: workspace default or hive_metastore)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file with DATABRICKS_HOST and DATABRICKS_TOKEN",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview schemas that would be deleted without actually dropping them",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        default=None,
        help="Only drop schemas older than N days (combine with --dry-run to preview)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Drop ALL test schemas matching the known prefixes (requires confirmation)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompts (use with caution)",
    )

    args = parser.parse_args()

    # Load credentials
    _load_env_file(args.env_file)
    host, token = _ensure_credentials()

    # Connect to Databricks
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print(
            "error: databricks-sdk is not installed.\n"
            "Install it with: pip install '.[remote]'"
        )
        return 1

    ws = WorkspaceClient(host=host, token=token)
    catalog = _get_catalog(ws, args.catalog)

    print(f"\nScanning catalog '{catalog}' for test schemas...")
    print(f"Prefixes: {', '.join(TEST_SCHEMA_PREFIXES)}")
    print()

    schemas = _list_test_schemas(ws, catalog)

    if not schemas:
        print("No test schemas found.  Nothing to clean up.")
        return 0

    # Filter by age if requested
    if args.older_than:
        filtered: list[dict] = []
        for s in schemas:
            age_days = _parse_age(s["created_at"])
            if age_days is None:
                print(f"  {s['name']}: created_at unknown, including in list")
                filtered.append(s)
            elif age_days >= args.older_than:
                filtered.append(s)
            else:
                print(f"  {s['name']}: {age_days:.1f} days old (younger than {args.older_than}d, skipping)")
        schemas = filtered

    if not schemas:
        print(f"No test schemas older than {args.older_than} days found.")
        return 0

    # Print summary
    print(f"Found {len(schemas)} test schema(s):")
    print()
    for s in schemas:
        age_str = ""
        age_days = _parse_age(s["created_at"])
        if age_days is not None:
            age_str = f"  ({age_days:.1f} days old)"
        print(f"  {s['name']}{age_str}")
    print()

    # Confirm
    if not args.yes:
        if args.dry_run:
            print("Dry-run mode.  No schemas will be deleted.")
        else:
            action = "delete ALL" if args.all else f"delete these {len(schemas)} schema(s)"
            if not _confirm(f"Are you sure you want to {action}?"):
                print("Cancelled.")
                return 2

    # Delete
    success_count = 0
    fail_count = 0
    for s in schemas:
        if _drop_schema(ws, s["name"], args.dry_run):
            success_count += 1
        else:
            fail_count += 1
        # Small delay to avoid rate limiting
        if not args.dry_run:
            time.sleep(0.25)

    # Summary
    print()
    if args.dry_run:
        print(f"Dry-run complete.  {len(schemas)} schema(s) would have been deleted.")
    else:
        print(
            f"Cleanup complete: {success_count} dropped, "
            f"{fail_count} failed, {len(schemas) - success_count - fail_count} skipped."
        )

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
