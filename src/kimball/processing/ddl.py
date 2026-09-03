"""Typed model and serializer for CREATE TABLE DDL (ADR-004 step 3).

Pure module: builds and serializes the DDL for a Delta table from typed
column specs. No SparkSession, no DeltaTable — the caller
(``TableCreator.create_table_with_clustering``) collects the spec list and
executes the returned SQL string.

Safety contract (property-tested): identifiers must match
``^[A-Za-z_][A-Za-z0-9_]*$``, data types and expressions pass the existing
whitelist regexes, and the serializer quotes every identifier so no
statement terminator, comment, or extra clause can be smuggled through a
name slot. Expression and data-type validation raise before any SQL is
built (fail closed, matching the previous inline behavior).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

CDF_METADATA_COLUMNS = frozenset(
    {
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "__merge_action",
    }
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_EXPRESSION_RE = re.compile(r"^[a-zA-Z0-9_().=<>\s\-,!+*/]+$")
_SQL_DATA_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_(),<>\s]*$")


def is_valid_identifier(name: str) -> bool:
    """True when *name* is a safe SQL identifier (existing contract)."""
    return bool(_IDENTIFIER_RE.match(name))


def is_safe_sql_expression(expression: str) -> bool:
    """True when *expression* passes the whitelist (existing contract)."""
    return bool(_SQL_EXPRESSION_RE.match(expression))


def is_safe_sql_data_type(data_type: str) -> bool:
    """True when *data_type* passes the whitelist (existing contract)."""
    return bool(_SQL_DATA_TYPE_RE.match(data_type))


@dataclass(frozen=True)
class ColumnSpec:
    """One column definition for a CREATE TABLE statement.

    ``name`` must be a valid SQL identifier; ``data_type`` must pass the
    Delta data-type whitelist. ``not_null`` renders the NOT NULL suffix.
    ``generated_expression`` renders a GENERATED ALWAYS AS clause (and
    then ``data_type`` is the generated column's declared type).
    """

    name: str
    data_type: str
    not_null: bool = False
    generated_expression: str | None = None

    def validate(self) -> None:
        if not is_valid_identifier(self.name):
            raise ValueError(f"Invalid column identifier: {self.name}")
        if not is_safe_sql_data_type(self.data_type):
            raise ValueError(f"Invalid column data type: {self.data_type}")
        if self.generated_expression is not None:
            if not is_safe_sql_expression(self.generated_expression):
                raise ValueError(
                    f"Invalid generated column expression: {self.generated_expression}"
                )


def quote_identifier(name: str) -> str:
    """Backtick-quote an already-validated identifier."""
    if not is_valid_identifier(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f"`{name}`"


def serialize_columns(columns: list[ColumnSpec], indent: str = "  ") -> str:
    """Serialize *columns* into the body lines of a CREATE TABLE statement.

    Raises on the first invalid spec (fail closed, before any SQL exists).
    """
    if not columns:
        raise ValueError("Cannot serialize an empty column list")
    rendered: list[str] = []
    for spec in columns:
        spec.validate()
        line = f"{quote_identifier(spec.name)} {spec.data_type}"
        if spec.not_null:
            line += " NOT NULL"
        if spec.generated_expression is not None:
            line += f" GENERATED ALWAYS AS ({spec.generated_expression})"
        rendered.append(f"{indent}{line}")
    return ",\n".join(rendered)
