"""Explicit, injectable secret references for runtime cryptographic operations."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, cast


class SecretResolutionError(ValueError):
    """Raised when a configured secret reference cannot be resolved."""


class SecretResolver:
    """Resolve ``env://`` and ``databricks://scope/key`` references.

    Secret values are returned to the caller but are never included in error
    messages or object representations. ``dbutils`` is injected to keep this
    module importable and testable outside Databricks.
    """

    def __init__(
        self,
        *,
        environ: Mapping[str, str] | None = None,
        dbutils: Any | None = None,
    ) -> None:
        self._environ = os.environ if environ is None else environ
        self._dbutils = dbutils

    def resolve(self, reference: str) -> str:
        if reference.startswith("env://"):
            name = reference.removeprefix("env://")
            if not name or name not in self._environ:
                raise SecretResolutionError(
                    f"Environment secret reference '{name or reference}' is unavailable"
                )
            return self._environ[name]
        if reference.startswith("databricks://"):
            path = reference.removeprefix("databricks://")
            parts = path.split("/", 1)
            if len(parts) != 2 or not all(parts):
                raise SecretResolutionError(
                    "Databricks secret references must be databricks://scope/key"
                )
            if self._dbutils is None:
                raise SecretResolutionError(
                    f"Databricks secret reference '{reference}' requires injected dbutils"
                )
            scope, key = parts
            try:
                return cast(str, self._dbutils.secrets.get(scope=scope, key=key))
            except Exception as exc:
                raise SecretResolutionError(
                    f"Databricks secret reference '{reference}' could not be resolved"
                ) from exc
        raise SecretResolutionError(
            "Secret references must use env:// or databricks://scope/key"
        )

    @classmethod
    def for_runtime(cls) -> SecretResolver:
        """Use Databricks ``dbutils`` when present, without an eager dependency."""
        try:
            runtime = __import__("databricks.sdk.runtime", fromlist=["dbutils"])
            dbutils = getattr(runtime, "dbutils", None)
        except (ImportError, AttributeError):
            dbutils = None
        return cls(dbutils=dbutils)
