"""Canonical, secret-safe serialization shared by manifests and run fingerprints."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any, cast

from pydantic import BaseModel

CONFIG_FINGERPRINT_VERSION = "v2:"
DESCRIPTIVE_CONFIG_FIELDS = frozenset({"table_description", "column_descriptions"})
_SENSITIVE_KEY = re.compile(
    r"(?:password|passwd|token|credential|access[_-]?key|private[_-]?key|secret)",
    re.IGNORECASE,
)


def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible data with stable object-key ordering."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_digest(value: Any) -> str:
    """Return the full SHA-256 digest of canonical JSON."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def redact_secrets(value: Any) -> Any:
    """Recursively replace values under secret-like keys with a fixed marker.

    Secret reference fields identify a resolver lookup and are safe metadata;
    literal credentials under keys such as ``password`` or ``token`` are never
    copied into manifests or fingerprint payloads.
    """
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, child in value.items():
            name = str(key)
            is_reference = name.lower().endswith(("_ref", "_env"))
            redacted[name] = (
                "<redacted>"
                if _SENSITIVE_KEY.search(name) and not is_reference
                else redact_secrets(child)
            )
        return redacted
    if isinstance(value, (list, tuple)):
        return [redact_secrets(item) for item in value]
    return value


def canonical_config_payload(
    config: BaseModel | Mapping[str, Any], *, sql_text: str | None = None
) -> dict[str, Any]:
    """Return the complete normalized behavior config, excluding descriptions."""
    if isinstance(config, Mapping):
        payload = dict(config)
    else:
        payload = cast(BaseModel, config).model_dump(by_alias=True, mode="json")
    for field in DESCRIPTIVE_CONFIG_FIELDS:
        payload.pop(field, None)
    if sql_text is not None:
        payload["transformation_sql"] = sql_text
    return cast(dict[str, Any], redact_secrets(payload))


def is_current_config_fingerprint(value: str | None) -> bool:
    """Whether a persisted config fingerprint uses the current version format."""
    if value is None:
        return False
    return (
        re.fullmatch(re.escape(CONFIG_FINGERPRINT_VERSION) + r"[0-9a-f]{64}", value)
        is not None
    )
