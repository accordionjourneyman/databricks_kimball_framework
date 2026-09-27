# Liquid Clustering

Set `cluster_by` to the columns used for common filters and lookups:

```yaml
cluster_by: [customer_id]
```

When the framework creates a table on Databricks, it applies the configured
columns with `CLUSTER BY`. Changing `cluster_by` does not alter an existing
table.

Set `optimize_after_merge: true` to request an `OPTIMIZE` after a successful
batch merge:

```yaml
cluster_by: [customer_sk, order_date]
optimize_after_merge: true
```

Inline optimization also requires `KIMBALL_ENABLE_INLINE_OPTIMIZE=1`. It is
disabled by default and adds work to each batch run. The environment setting
is documented in [Configuration](CONFIGURATION.md#runtime-settings-and-performance-controls).
