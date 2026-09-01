"""Unit tests for CLI helpers and ops subcommands (pure-Python paths)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kimball.cli import discover_config_paths, main


class TestDiscoverConfigPaths:
    def test_single_file_preserved(self, tmp_path):
        f = tmp_path / "a.yml"
        f.write_text("x: 1")
        assert discover_config_paths([str(f)]) == [str(f)]

    def test_directory_expands_recursively_sorted(self, tmp_path):
        (tmp_path / "b.yml").write_text("x: 1")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "a.yaml").write_text("x: 1")
        result = discover_config_paths([str(tmp_path)])
        # Sorted lexicographically: b.yml < a.yaml? No — 'a.yaml' < 'b.yml'.
        # The set is sorted; sub/a.yaml sorts under its full path which starts
        # with .../sub/, so only relative ordering of the basenames differs.
        assert sorted(result) == result
        assert len(result) == 2

    def test_glob_expansion(self, tmp_path):
        for name in ("m1.yml", "m2.yml"):
            (tmp_path / name).write_text("x: 1")
        result = discover_config_paths([str(tmp_path / "m*.yml")])
        assert len(result) == 2

    def test_missing_path_preserved_for_error(self, tmp_path):
        missing = str(tmp_path / "nope.yml")
        assert discover_config_paths([missing]) == [missing]

    def test_duplicates_deduplicated(self, tmp_path):
        f = tmp_path / "a.yml"
        f.write_text("x: 1")
        assert discover_config_paths([str(f), str(f)]) == [str(f)]


class TestRecoverTimestampValidation:
    def test_invalid_timestamp_errors_before_spark(self, capsys):
        result = main(
            [
                "recover",
                "--target",
                "prod",
                "--table",
                "t1",
                "--timestamp",
                "not-a-date",
            ]
        )
        assert result == 1

    def test_valid_timestamp_reaches_recovery(self, capsys):
        # _ops_runtime_and_providers builds Spark providers via get_spark;
        # patch them out so only the parse logic runs for real.
        runtime = MagicMock()
        providers = MagicMock()
        result_mock = MagicMock()
        result_mock.to_dict.return_value = {"ok": True}
        result_mock.partial = False
        with (
            patch(
                "kimball.cli._ops_runtime_and_providers",
                return_value=(runtime, providers),
            ),
            patch(
                "kimball.ops.recover.recover_target", return_value=result_mock
            ) as rec,
        ):
            code = main(
                [
                    "recover",
                    "--target",
                    "prod",
                    "--table",
                    "t1",
                    "--timestamp",
                    "2025-06-01T00:00:00",
                ]
            )
        assert code == 0
        assert rec.call_args.kwargs["timestamp"] is not None


class TestExplainConfigPaths:
    def test_explain_config_ok(self, tmp_path, capsys):
        # A minimal valid config compiles -> verdict config-ok.
        cfg = tmp_path / "ok.yml"
        cfg.write_text(
            """
table_name: dim_ok
table_type: dimension
surrogate_key: sk
natural_keys: [id]
table_description: Test dimension for explain config-ok path.
sources:
  - name: s1
    alias: s1
    primary_keys: [id]
            """.strip()
        )
        targets = tmp_path / "targets.yml"
        targets.write_text(
            """
version: 1
targets:
  prod:
    catalog: workspace
    silver_schema: prod_silver
    gold_schema: prod_gold
    etl_schema: prod_ops
            """.strip()
        )
        code = main(
            [
                "explain",
                "--target",
                "prod",
                "--targets",
                str(targets),
                "--config",
                str(cfg),
            ]
        )
        assert code == 0

    def test_explain_config_error_returns_one(self, tmp_path, capsys):
        cfg = tmp_path / "bad.yml"
        cfg.write_text(
            """
table_name: dim_bad
table_type: dimension
            """.strip()
        )
        targets = tmp_path / "targets.yml"
        targets.write_text(
            """
version: 1
targets:
  prod:
    catalog: workspace
    silver_schema: prod_silver
    gold_schema: prod_gold
    etl_schema: prod_ops
            """.strip()
        )
        code = main(
            [
                "explain",
                "--target",
                "prod",
                "--targets",
                str(targets),
                "--config",
                str(cfg),
            ]
        )
        assert code == 1
        assert '"category"' in capsys.readouterr().out
