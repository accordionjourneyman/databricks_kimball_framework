# Continuous Integration Setup Guide

This document explains how to configure the continuous integration (CI) pipelines for the Kimball Framework.

There are two workflows:

1. **Fast CI** (`.github/workflows/ci.yml`) — runs on every push and pull request.
2. **Integration / Golden Tests** (`.github/workflows/integration.yml`) — runs manually or nightly against a real Databricks workspace.

The fast CI is free and self-contained. The integration workflow requires a Databricks workspace and a personal access token.

---

## 1. Fast CI

This workflow runs automatically on GitHub Actions for every push to `main` and every pull request.

It performs:

- Ruff linting (`ruff check src tests tools`)
- Ruff format check (`ruff format --check src tests tools`)
- Unit tests across Python 3.10, 3.11, and 3.12 (`pytest tests/unit`)
- Wheel build (`python -m build`)

No secrets or Databricks access are required. It uses GitHub-hosted Ubuntu runners.

---

## 2. Integration / Golden Tests

These tests exercise the full Kimball Framework against a real Delta Lake runtime, including:

- SCD Type 1 and SCD Type 2 dimensions
- Fact tables with foreign-key lookups
- Golden dummy datasets loaded from CSV

Because Databricks Free Edition does not provide classic all-purpose clusters, the runner defaults to **serverless** job compute. If you have a paid workspace with a classic cluster, you can optionally provide its cluster ID.

---

## 3. Required credentials

### 3.1 Local development

Create a `.env` file in the repository root by copying the example:

```bash
cp .env.example .env
```

Then edit `.env` and set at minimum:

```env
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
KIMBALL_TEST_CATALOG=main
```

> **Important:** The environment variable name must be exactly `DATABRICKS_TOKEN`. Other names such as `DATABRICKS_API_TOKEN` will not be recognized by the Databricks SDK or by the kimball-framework tooling.

`.env` is already listed in `.gitignore`, so it will not be committed.

### 3.2 Generate a Databricks personal access token

1. Open your Databricks workspace.
2. Click your user profile (top right) → **User settings**.
3. Go to **Developer** → **Access tokens** → **Generate new token**.
4. Give it a name, e.g. `github-actions-ci`.
5. Set an expiration (90 days is a reasonable default).
6. Copy the token immediately. Databricks shows it only once.
7. Paste the token into your `.env` file and into the GitHub secret described below.

### 3.3 Required token permissions

The token needs permissions to:

- Run jobs
- Read and write workspace files under `/Workspace/Users/ci/kimball_framework`
- Create schemas and tables in the configured `KIMBALL_TEST_CATALOG`

If you use a classic cluster instead of serverless, the token also needs permission to attach to that cluster.

### 3.4 GitHub repository secrets

Go to your repository on GitHub:

```
https://github.com/accordionjourneyman/databricks_kimball_framework/settings/secrets/actions
```

Create the following **repository secrets**:

| Secret name | Value |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `https://myworkspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | The personal access token generated above |

Optionally create:

| Secret name | Value |
|---|---|
| `DATABRICKS_CLUSTER_ID` | A classic all-purpose cluster ID. Leave unset to use serverless. |

Create the following **repository variable** (not a secret):

| Variable name | Value |
|---|---|
| `KIMBALL_TEST_CATALOG` | Catalog for test schemas, e.g. `main` or `hive_metastore` |

### 3.5 Why these are safe

- `.env` is git-ignored and never pushed.
- GitHub secrets are encrypted and only exposed to Actions workflows.
- Forks of the repository do **not** inherit your secrets.
- The integration workflow does not run on pull requests from forks.
- The `DATABRICKS_HOST` is just the workspace URL; it is not a secret by itself, but storing it as a secret keeps the workflow self-contained.

---

## 4. Run integration tests locally

Activate the virtual environment and run:

```bash
.venv\Scripts\Activate.ps1   # Windows PowerShell
source .venv/bin/activate    # Linux / macOS

python tools/run_databricks_tests.py tests/golden
```

This will:

1. Build the `kimball_framework` wheel.
2. Upload the wheel and test files to your Databricks workspace under `/Workspace/Users/ci/kimball_framework`.
3. Submit a serverless job that installs the wheel and runs `pytest tests/golden`.
4. Poll for completion and print the pytest output.

If you have a classic cluster, pass its ID:

```bash
python tools/run_databricks_tests.py tests/golden --cluster-id 0123-456789-abcdef0
```

---

## 5. Run integration tests from GitHub Actions

### 5.1 Nightly schedule

The workflow is configured to run every night at 02:00 UTC:

```yaml
on:
  schedule:
    - cron: "0 2 * * *"
```

If you are on Databricks Free Edition, nightly runs may consume your monthly DBU quota. To disable the schedule, edit `.github/workflows/integration.yml` and remove or comment out the `schedule:` block.

### 5.2 Manual trigger

You can run the workflow on demand:

1. Go to **Actions** → **Integration Tests**.
2. Click **Run workflow**.
3. Choose the test path (`tests/golden` or `tests/integration`).
4. Click **Run workflow**.

### 5.3 Adding environment protection (recommended)

For extra safety, create a GitHub environment with protection rules:

1. Go to **Settings** → **Environments** → **New environment**.
2. Name it `databricks-integration`.
3. Add protection rules such as **Required reviewers** or **Only allow deployment from main branch**.
4. Move the secrets from the repository level to this environment.
5. Update `.github/workflows/integration.yml` to include:

```yaml
jobs:
  databricks-integration:
    runs-on: ubuntu-latest
    environment: databricks-integration
```

This causes the workflow to pause until a maintainer approves it.

---

## 6. Local fallback without Databricks

If you do not provide `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, the `spark` fixture in `tests/conftest.py` falls back to a local SparkSession. This requires a Java/JDK installation on your machine. It is mainly useful for contributors who want to run fast unit tests without cloud access.

Integration and golden tests will generally not pass against a local Spark session because they rely on Databricks-specific features (managed Delta tables, identity columns, serverless behavior). They are intended to run on Databricks.

---

## 7. Troubleshooting

### `DATABRICKS_TOKEN` not found

Make sure your `.env` file uses `DATABRICKS_TOKEN=...`, not `DATABRICKS_API_TOKEN=...`.

### Serverless job fails to start

Databricks Free Edition has limited serverless concurrency. Wait a few minutes and try again, or check the job run logs in the Databricks UI under **Workflows** → **Job runs**.

### Cluster not found

If you set `DATABRICKS_CLUSTER_ID`, verify that the cluster exists and that your token can attach to it. If you leave it empty, the runner uses serverless.

### Schema already exists

Integration tests create schemas named `kimball_golden_raw` and `kimball_golden` under `KIMBALL_TEST_CATALOG`. If a previous run left them behind, the next run may fail or produce unexpected row counts. You can drop them manually:

```sql
DROP SCHEMA IF EXISTS {catalog}.kimball_golden_raw CASCADE;
DROP SCHEMA IF EXISTS {catalog}.kimball_golden CASCADE;
```
