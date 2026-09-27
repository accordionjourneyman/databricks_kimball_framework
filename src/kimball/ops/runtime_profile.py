"""Detect runtime capabilities that affect commit attribution and recovery."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any


class RuntimeFlavor(str, Enum):
    CLASSIC = "classic"
    SERVERLESS = "serverless"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RuntimeProfile:
    flavor: RuntimeFlavor
    supports_commit_tagging: bool

    @property
    def is_serverless(self) -> bool:
        return self.flavor is RuntimeFlavor.SERVERLESS


_CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)
_SERVERLESS = RuntimeProfile(RuntimeFlavor.SERVERLESS, False)
_UNKNOWN = RuntimeProfile(RuntimeFlavor.UNKNOWN, False)


def detect_runtime_profile(spark: Any) -> RuntimeProfile:
    """Probe commit-tagging support by setting+unsetting the userMetadata conf."""
    tag = "spark.databricks.delta.commitInfo.userMetadata"
    try:
        spark.conf.set(tag, "__kimball_probe__")
        try:
            spark.conf.unset(tag)
        except AttributeError:
            spark.conf.set(tag, "")
        return _CLASSIC
    except Exception:
        return _SERVERLESS if _looks_serverless() else _UNKNOWN


def _looks_serverless() -> bool:
    return bool(
        os.environ.get("DATABRICKS_SERVERLESS")
        or os.environ.get("SPARK_CONNECT_DRIVER")
        or os.environ.get("SPARK_REMOTE")
    )
