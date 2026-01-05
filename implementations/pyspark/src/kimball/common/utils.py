from __future__ import annotations


def quote_table_name(table_name: str) -> str:
    """
    Properly quote a multi-part table name for SQL.
    Converts 'schema.table' to '`schema`.`table`' instead of '`schema.table`'.
    """
    parts = table_name.split(".")
    return ".".join(f"`{part}`" for part in parts)
