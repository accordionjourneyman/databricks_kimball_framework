from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from kimball.orchestration.services.descriptions import DescriptionManager


def _spark(columns=("id", "name"), previous=None):
    spark = MagicMock()
    spark.table.return_value.schema.fields = [
        SimpleNamespace(name=column) for column in columns
    ]
    property_result = MagicMock()
    property_result.first.return_value = (
        {"value": json.dumps(previous, sort_keys=True)}
        if previous is not None
        else None
    )
    spark.sql.return_value = property_result
    return spark


def test_description_sync_skips_ddl_when_manifest_is_unchanged() -> None:
    manifest = {"table": "Customers", "columns": {"name": "Display name"}}
    spark = _spark(previous=manifest)

    changed = DescriptionManager().sync(
        spark, "gold.dim_customer", "Customers", {"name": "Display name"}
    )

    assert changed is False
    assert spark.sql.call_count == 1
    assert "SHOW TBLPROPERTIES" in spark.sql.call_args.args[0]


def test_description_sync_only_changes_modified_and_removed_comments() -> None:
    previous = {
        "table": "Old table text",
        "columns": {"id": "Identifier", "name": "Old name"},
    }
    spark = _spark(previous=previous)

    changed = DescriptionManager().sync(
        spark, "gold.dim_customer", None, {"name": "New name"}
    )

    assert changed is True
    sql = [call.args[0] for call in spark.sql.call_args_list]
    assert "COMMENT ON TABLE `gold`.`dim_customer` IS NULL" in sql
    assert "ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `id` COMMENT NULL" in sql
    assert (
        "ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `name` COMMENT 'New name'"
        in sql
    )


def test_description_sync_rejects_unknown_columns_before_ddl() -> None:
    spark = _spark(columns=("id",), previous=None)

    with pytest.raises(ValueError, match="missing"):
        DescriptionManager().sync(
            spark, "gold.dim_customer", None, {"missing": "Not a column"}
        )
