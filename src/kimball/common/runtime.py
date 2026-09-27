"""Typed, injectable settings for one Kimball pipeline execution.

Environment values are read and validated once at an entry boundary. Batch,
streaming, and library callers can pass the resulting ``RuntimeOptions`` to all
pipeline objects instead of consulting the process environment while a run is
in progress.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class RuntimeOptions(BaseModel):
    """Validated runtime settings and performance controls.

    The environment reader accepts explicit ``KIMBALL_*`` names and keeps
    Databricks/runtime detection and secret resolution at their adapter
    boundaries. Optional feature switches inherit from ``mode`` when unset.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    etl_schema: str | None = None
    checkpoint_root: str | None = None
    streaming_checkpoint_root: str | None = None
    mode: Literal["lite", "full"] = "lite"

    # Feature flags: None means inherit from mode.
    enable_checkpoints: bool | None = None
    enable_staging_cleanup: bool | None = None
    enable_metrics: bool | None = None
    enable_auto_cluster: bool | None = None

    # Runtime and performance controls.
    enable_dev_checks: bool = False
    batch_control_writes: bool = False
    enable_inline_optimize: bool = False
    enable_vacuum: bool = False
    approx_grain_check: bool = False
    skip_delete_detection: bool = False
    optimize_scd2_lazy_eval: bool = False
    single_window_scd2: bool = False
    use_approximate_unique: bool = False
    shuffle_partitions: Literal["auto"] | int = "auto"
    skew_threshold_mb: int = Field(default=256, gt=0)
    skew_factor: int = Field(default=5, gt=0)

    cleanup_registry_table: str = "default.kimball_staging_registry"
    checkpoint_table: str = "default.kimball_pipeline_checkpoints"

    # Retained for compatibility with older callers; runtime dependencies are
    # excluded from serialized settings and are never read from the environment.
    spark_session: Any = Field(default=None, exclude=True, repr=False)

    @field_validator("shuffle_partitions", mode="before")
    @classmethod
    def _parse_shuffle_partitions(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip().lower()
            if stripped == "auto":
                return "auto"
            try:
                value = int(stripped)
            except ValueError as exc:
                raise ValueError("must be 'auto' or a positive integer") from exc
        if isinstance(value, int) and value > 0:
            return value
        raise ValueError("must be 'auto' or a positive integer")

    @model_validator(mode="after")
    def _resolve_mode_flags(self) -> RuntimeOptions:
        full_mode = self.mode == "full"
        for field_name in (
            "enable_checkpoints",
            "enable_staging_cleanup",
            "enable_metrics",
            "enable_auto_cluster",
        ):
            if getattr(self, field_name) is None:
                setattr(self, field_name, full_mode)
        return self

    @classmethod
    def from_environment(
        cls, environ: Mapping[str, str] | None = None
    ) -> RuntimeOptions:
        """Read the supported environment variables once and validate them.

        Boolean variables use ``1`` or ``0``. Invalid values fail with the
        environment-variable name instead of silently selecting a default.
        """
        source = os.environ if environ is None else environ
        if "KIMBALL_SKIP_VALIDATION_IF_UNCHANGED" in source:
            raise ValueError(
                "KIMBALL_SKIP_VALIDATION_IF_UNCHANGED was removed because unchanged "
                "schema/configuration does not prove unchanged data. Remove the variable; "
                "data-quality, natural-key, and foreign-key checks now always run."
            )

        fields = {
            "etl_schema": "KIMBALL_ETL_SCHEMA",
            "checkpoint_root": "KIMBALL_CHECKPOINT_ROOT",
            "streaming_checkpoint_root": "KIMBALL_STREAMING_CHECKPOINT_ROOT",
            "mode": "KIMBALL_MODE",
            "enable_checkpoints": "KIMBALL_ENABLE_CHECKPOINTS",
            "enable_staging_cleanup": "KIMBALL_ENABLE_STAGING_CLEANUP",
            "enable_metrics": "KIMBALL_ENABLE_METRICS",
            "enable_auto_cluster": "KIMBALL_ENABLE_AUTO_CLUSTER",
            "enable_dev_checks": "KIMBALL_ENABLE_DEV_CHECKS",
            "batch_control_writes": "KIMBALL_BATCH_CONTROL_WRITES",
            "enable_inline_optimize": "KIMBALL_ENABLE_INLINE_OPTIMIZE",
            "enable_vacuum": "KIMBALL_ENABLE_VACUUM",
            "approx_grain_check": "KIMBALL_APPROX_GRAIN_CHECK",
            "skip_delete_detection": "KIMBALL_SKIP_DELETE_DETECTION",
            "optimize_scd2_lazy_eval": "KIMBALL_OPTIMIZE_SCD2_LAZY_EVAL",
            "single_window_scd2": "KIMBALL_SINGLE_WINDOW_SCD2",
            "use_approximate_unique": "KIMBALL_USE_APPROXIMATE_UNIQUE",
            "shuffle_partitions": "KIMBALL_SHUFFLE_PARTITIONS",
            "skew_threshold_mb": "KIMBALL_SKEW_THRESHOLD_MB",
            "skew_factor": "KIMBALL_SKEW_FACTOR",
            "cleanup_registry_table": "KIMBALL_CLEANUP_REGISTRY_TABLE",
            "checkpoint_table": "KIMBALL_CHECKPOINT_TABLE",
        }
        flag_fields = {
            "enable_checkpoints",
            "enable_staging_cleanup",
            "enable_metrics",
            "enable_auto_cluster",
            "enable_dev_checks",
            "batch_control_writes",
            "enable_inline_optimize",
            "enable_vacuum",
            "approx_grain_check",
            "skip_delete_detection",
            "optimize_scd2_lazy_eval",
            "single_window_scd2",
        }
        values: dict[str, Any] = {}
        for field_name, environment_name in fields.items():
            if environment_name not in source:
                continue
            raw = source[environment_name]
            if field_name in flag_fields:
                if raw not in {"0", "1"}:
                    raise ValueError(f"{environment_name} must be '0' or '1'")
                values[field_name] = raw == "1"
            elif field_name == "mode":
                values[field_name] = raw.strip().lower()
            elif field_name in {"skew_threshold_mb", "skew_factor"}:
                try:
                    values[field_name] = int(raw)
                except ValueError as exc:
                    raise ValueError(
                        f"{environment_name} must be a positive integer"
                    ) from exc
            elif field_name == "shuffle_partitions":
                values[field_name] = raw
            elif field_name == "use_approximate_unique":
                # Preserve the legacy N3 flag contract; its behavior is not
                # part of this runtime-settings refactor.
                values[field_name] = raw == "1"
            else:
                values[field_name] = raw or None

        try:
            return cls.model_validate(values)
        except ValidationError as exc:
            errors: list[str] = []
            for error in exc.errors(include_input=False):
                field_name = str(error["loc"][0]) if error["loc"] else "settings"
                environment_name = fields.get(field_name, field_name)
                errors.append(f"{environment_name}: {error['msg']}")
            raise ValueError(
                "Invalid runtime settings:\n- " + "\n- ".join(errors)
            ) from exc

    def resolved(
        self,
        *,
        etl_schema: str | None = None,
        target_etl_schema: str | None = None,
        checkpoint_root: str | None = None,
        target_checkpoint_root: str | None = None,
        streaming_checkpoint_root: str | None = None,
    ) -> RuntimeOptions:
        """Apply explicit arguments, then target values, then environment.

        A target checkpoint root is used for both Spark checkpointing and the
        default streaming query root. A dedicated streaming root remains the
        fallback when no explicit or target root was selected.
        """
        resolved_schema = etl_schema or target_etl_schema or self.etl_schema
        resolved_checkpoint = (
            checkpoint_root or target_checkpoint_root or self.checkpoint_root
        )
        resolved_streaming = (
            streaming_checkpoint_root
            or checkpoint_root
            or target_checkpoint_root
            or self.streaming_checkpoint_root
        )
        return self.model_copy(
            update={
                "etl_schema": resolved_schema,
                "checkpoint_root": resolved_checkpoint,
                "streaming_checkpoint_root": resolved_streaming,
            }
        )

    def feature_enabled(self, feature: str) -> bool:
        """Return the resolved value for a named resilience feature."""
        field_name = f"enable_{feature}"
        if field_name not in {
            "enable_checkpoints",
            "enable_staging_cleanup",
            "enable_metrics",
            "enable_auto_cluster",
        }:
            raise ValueError(f"Unknown runtime feature: {feature}")
        return bool(getattr(self, field_name))
