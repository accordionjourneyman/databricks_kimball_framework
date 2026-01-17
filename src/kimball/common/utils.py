from __future__ import annotations


def quote_table_name(table_name: str) -> str:
    """
    Properly quote a multi-part table name for SQL.

    Handles 1, 2, or 3 part names:
    - 'table' → '`table`'
    - 'schema.table' → '`schema`.`table`'
    - 'catalog.schema.table' → '`catalog`.`schema`.`table`'
    """
    parts = table_name.split(".")
    return ".".join(f"`{part}`" for part in parts)
