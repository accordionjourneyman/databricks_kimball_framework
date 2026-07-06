"""
Streaming CDF support for the Kimball framework.

This subpackage adds a Spark structured-streaming path that is selected
on a per-source basis via the YAML ``sources[*].streaming.enabled`` flag.
It reuses the existing batch code paths for everything except the read
itself: the merger, watermark manager, and config loader are unchanged.

Public surface:

- :class:`StreamingOrchestrator` — drop-in replacement for
  :class:`kimball.Orchestrator` that runs the same pipeline as a
  structured-streaming query.
- :class:`StreamCdfLoader` — low-level wrapper around
  ``spark.readStream.format("delta")`` with ``readChangeFeed=true``.
- :func:`default_checkpoint_path` — resolver for the
  ``checkpointLocation`` option.

Example YAML::

    sources:
      - name: silver.customers
        alias: c
        cdc_strategy: cdf
        primary_keys: [customer_id]
        streaming:
          enabled: true
          trigger: available_now      # or processing_time
          trigger_interval: "30 seconds"  # processing_time only
          checkpoint_location: /Volumes/main/etl/_checkpoints/customers
"""

from kimball.streaming.checkpoint import default_checkpoint_path
from kimball.streaming.loader import StreamCdfLoader
from kimball.streaming.orchestrator import StreamingOrchestrator

__all__ = [
    "StreamingOrchestrator",
    "StreamCdfLoader",
    "default_checkpoint_path",
]
