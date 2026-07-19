"""
Default ``checkpointLocation`` resolver for the streaming module.

The streaming orchestrator asks for a path when the YAML does not
provide one. The rule is:

1. If ``$KIMBALL_STREAMING_CHECKPOINT_ROOT`` is set, use
   ``$KIMBALL_STREAMING_CHECKPOINT_ROOT/<sanitised_source_table>``.
2. Otherwise, fall back to
   ``/tmp/kimball_streaming_checkpoints/<sanitised_source_table>``.

In both cases the path is sanitised to be safe on every common
filesystem (alphanumerics, ``-`` and ``_`` only; everything else is
collapsed to ``_``).
"""

from __future__ import annotations

import os
import re

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitise(parts: list[str]) -> str:
    joined = "__".join(parts)
    return _SAFE.sub("_", joined).strip("_") or "kimball_streaming"


def default_checkpoint_path(
    source_table: str,
    etl_schema: str | None = None,
    root: str | None = None,
) -> str:
    """Return a default checkpoint path for ``source_table``."""
    if root is None:
        root = os.environ.get("KIMBALL_STREAMING_CHECKPOINT_ROOT")
        if root is None:
            if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
                root = "/Volumes/workspace/default/kimball_checkpoints"
            else:
                root = "/tmp/kimball_streaming_checkpoints"
    parts: list[str] = []
    if etl_schema:
        parts.append(etl_schema)
    parts.append(source_table)
    return os.path.join(root, _sanitise(parts))
