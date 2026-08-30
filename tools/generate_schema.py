"""Generate JSON Schema for pipeline YAML configs from Pydantic models.

Used by CI to produce ``schemas/pipeline-config.schema.json`` on every
push to main. The schema enables editor autocomplete (VSCode YAML
extension, IntelliJ) and external config validation.

Usage::

    python tools/generate_schema.py --output schemas/pipeline-config.schema.json

The output is deterministic: identical input always produces identical
output. Do not hand-edit the generated schema.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _resolve_refs(schema: dict, root: dict) -> dict:
    """Recursively resolve ``$ref`` references against *root*."""
    if not isinstance(schema, dict):
        return (
            [_resolve_refs(item, root) for item in schema]
            if isinstance(schema, list)
            else schema
        )
    if "$ref" in schema and isinstance(schema["$ref"], str):
        ref_path = schema["$ref"].lstrip("#/")
        parts = ref_path.split("/")
        target = root
        for part in parts:
            target = target[part]
        resolved = json.loads(json.dumps(target))
        return _resolve_refs(resolved, root)
    return {k: _resolve_refs(v, root) for k, v in schema.items()}


def generate_pipeline_schema() -> dict:
    """Build the pipeline-config JSON Schema from Pydantic models."""
    from kimball.common.config import TableConfig

    raw = TableConfig.model_json_schema()
    resolved = _resolve_refs(raw, raw)
    resolved.setdefault("$schema", "https://json-schema.org/draft/2020-12/schema")
    resolved.setdefault("title", "Kimball Pipeline Configuration")
    resolved.setdefault(
        "description",
        "Schema for kimball_framework pipeline YAML configs. "
        "Validated by Pydantic at load time. See docs/CONFIGURATION.md "
        "for field-level documentation.",
    )
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("schemas/pipeline-config.schema.json"),
        help="Where to write the generated schema (default: %(default)s)",
    )
    args = parser.parse_args()

    schema = generate_pipeline_schema()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(schema, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {args.output} ({len(json.dumps(schema))} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
