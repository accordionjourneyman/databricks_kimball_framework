# Benchmarking

The benchmark system separates correctness, deterministic work guards, and
wall-clock performance. Only results produced on the same testbed and pinned
runtime are comparable.

## Suites

- `micro` measures focused hashing, validation, and configuration operations.
- `spark` measures the current end-to-end SCD1/SCD2/SCD7, validation,
  projection, multi-version Delta, and Type 7 broker range-lookup paths.
- Normal unit tests own deterministic action-count guarantees. A new Spark
  action, Delta write, or metadata operation is a correctness-style regression;
  it does not need a timing threshold.

Historical implementations are not embedded in the suite. `pytest-benchmark`
compares the current implementation with a saved result from a clean master
checkout.

## Authoritative local run

The pinned Docker environment on the same physical machine is the authoritative
testbed. Databricks Free Edition and GitHub-hosted runners are compatibility or
advisory environments because their load and startup time vary.

Run a smoke sample before a full benchmark:

```powershell
docker compose run --rm `
  -e KIMBALL_BENCHMARK_ROUNDS=1 `
  -e KIMBALL_BENCHMARK_WARMUPS=0 `
  kimball-tests python tools/benchmark_runner.py --suite spark --scale tiny
```

Create an authoritative baseline from a clean master checkout:

```powershell
docker compose run --rm kimball-tests python tools/benchmark_runner.py `
  --suite spark --scale tiny --save-baseline main
```

Compare a later run with that baseline:

```powershell
docker compose run --rm kimball-tests python tools/benchmark_runner.py `
  --suite spark --scale tiny `
  --compare benchmark-results/baselines/tiago-windows-docker/spark-tiny-main
```

Defaults are two warm-up rounds and five measured rounds. `tiny` is the normal
development scale, `small` is the pre-release scale, and `medium` is an explicit
capacity run.

## Result contract

Each run creates an ignored, immutable directory under
`benchmark-results/raw/<UTC-run-id>/` containing:

- `pytest-benchmark.json`: canonical sample distribution and timing data;
- `manifest.json`: commit, dirty state, testbed, hardware, Docker and runtime
  identity;
- `spark-metrics.json`: jobs, stages, tasks, I/O, shuffle, spill, executor time,
  and GC time parsed from the Spark event log;
- `comparison.json`: like-for-like median changes when a baseline is supplied;
- `summary.md`: human-readable run summary.

The runner rejects comparisons when testbed, Python, Java, Spark, Delta, or
Docker image identities differ. Do not copy baselines between machines.

## Regression policy

- Correctness and deterministic work-count regressions block pull requests.
- GitHub-hosted microbenchmark timings are advisory and use the separate
  `github-ubuntu-python` testbed.
- A local wall-clock regression is reported when the median is more than 15%
  slower than its matching baseline.
- Confirm a warning with a second clean run. A confirmed unexplained regression
  blocks a release, not an individual pull request.
- Report medians and sample spread; never claim an improvement from one sample.

## Storage and Bencher

GitHub Actions uploads raw results as immutable artifacts: 30 days for advisory
microbenchmarks and 90 days for Spark compatibility runs. Generated outputs and
baselines are not committed to the source branch.

Bencher Cloud is the long-term trend dashboard. The Docker test image includes
the Bencher CLI. Set `BENCHER_API_KEY` and optionally `BENCHER_PROJECT`, then
run the benchmark runner with `--publish` in an environment where the CLI is
available. The key
is read from the environment and is never included in command arguments or
result files.

Push workflows install the official Bencher action and publish the advisory
microbenchmark distribution when `BENCHER_API_KEY` is configured as a
repository secret. Configure `BENCHER_PROJECT` as a repository variable. Pull
requests never receive the key. Bencher records the median and interquartile
range from the canonical `pytest-benchmark` JSON.

Use distinct Bencher testbeds:

- `tiago-windows-docker` for authoritative local Spark/Delta measurements;
- `github-ubuntu-python` for hosted microbenchmarks;
- `github-ubuntu-spark` for hosted Spark correctness smoke runs.

A future self-hosted GitHub runner can execute the same Docker command. It polls
GitHub over outbound HTTPS; it does not require an inbound tunnel. Restrict it
to manual, scheduled, and protected-master workflows rather than fork pull
requests.
