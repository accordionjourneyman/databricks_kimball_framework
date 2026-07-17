from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kimball.common.secrets import SecretResolutionError, SecretResolver


def test_environment_secret_reference_is_resolved_without_logging_value() -> None:
    resolver = SecretResolver(environ={"TOKEN_KEY": "super-secret"})

    assert resolver.resolve("env://TOKEN_KEY") == "super-secret"


def test_missing_environment_secret_fails_closed() -> None:
    with pytest.raises(SecretResolutionError, match="TOKEN_KEY"):
        SecretResolver(environ={}).resolve("env://TOKEN_KEY")


def test_databricks_secret_reference_uses_injected_dbutils() -> None:
    dbutils = MagicMock()
    dbutils.secrets.get.return_value = "workspace-secret"

    value = SecretResolver(dbutils=dbutils).resolve("databricks://pii/customer-key")

    assert value == "workspace-secret"
    dbutils.secrets.get.assert_called_once_with(scope="pii", key="customer-key")


@pytest.mark.parametrize("reference", ["TOKEN_KEY", "vault://scope/key", "env://"])
def test_secret_reference_requires_a_supported_explicit_scheme(reference: str) -> None:
    with pytest.raises(SecretResolutionError):
        SecretResolver(environ={}).resolve(reference)
