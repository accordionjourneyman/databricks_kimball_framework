"""Tests for kimball deploy pre-flight + decision (1.5)."""

from __future__ import annotations

from kimball.ops.deploy import deploy, preflight
from kimball.ops.providers import SourceHealthReport
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from tests.unit.ops.fakes import (
    FakeControl,
    FakeHistory,
    FakeSources,
    batch,
    commit,
    providers,
)

CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)
SERVERLESS = RuntimeProfile(RuntimeFlavor.SERVERLESS, False)


def _src_healthy():
    return FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, None, None, None)}
    )


def _pipe(table, table_type, digest):
    return {
        "table_name": table,
        "table_type": table_type,
        "semantic_config": {"table_type": table_type, "natural_keys": ["a"]},
        "semantic_digest": digest,
        "metadata_digest": "m",
        "dependencies": [],
        "writes": [],
    }


def _clean_providers():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    return providers(
        FakeControl(True, batches),
        FakeHistory(True, 3, (commit(3, "b1"),)),
        _src_healthy(),
    )


def test_preflight_clean():
    pre = preflight(_clean_providers(), CLASSIC, ["gold.t"], [("silver.s", True)])
    assert pre.ok


def test_preflight_zombie_blocks():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    pre = preflight(
        providers(FakeControl(True, batches), hist, _src_healthy()),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
    )
    assert not pre.ok
    assert pre.blockers[0].kind == "inconsistent_state"


def test_preflight_source_missing_blocks():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    src = FakeSources(
        {
            "silver.s": SourceHealthReport(
                "silver.s", False, None, None, None, None, None
            )
        }
    )
    pre = preflight(
        providers(FakeControl(True, batches), hist, src),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
    )
    assert any(b.kind == "source_missing" for b in pre.blockers)


def test_preflight_cdf_disabled_when_required():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    src = FakeSources(
        {
            "silver.s": SourceHealthReport(
                "silver.s", True, False, None, None, None, None
            )
        }
    )
    pre = preflight(
        providers(FakeControl(True, batches), hist, src),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
    )
    assert any(b.kind == "cdf_disabled" for b in pre.blockers)


def test_preflight_cdf_disabled_not_required_ok():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    src = FakeSources(
        {
            "silver.s": SourceHealthReport(
                "silver.s", True, False, None, None, None, None
            )
        }
    )
    pre = preflight(
        providers(FakeControl(True, batches), hist, src),
        CLASSIC,
        ["gold.t"],
        [("silver.s", False)],
    )
    assert pre.ok


def test_deploy_breaking_blocks_without_allow():
    prev = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    curr = {"pipelines": [_pipe("gold.t", "fact", "d2")]}
    res = deploy(
        prev, curr, _clean_providers(), CLASSIC, ["gold.t"], [("silver.s", True)]
    )
    assert res.blocked
    assert "breaking" in (res.reason or "")


def test_deploy_breaking_allowed_with_flag():
    prev = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    curr = {"pipelines": [_pipe("gold.t", "fact", "d2")]}
    res = deploy(
        prev,
        curr,
        _clean_providers(),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
        allow_breaking=True,
    )
    assert not res.blocked


def test_deploy_preflight_blocks():
    prev = curr = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    res = deploy(
        prev,
        curr,
        providers(FakeControl(True, batches), hist, _src_healthy()),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
    )
    assert res.blocked
    assert "pre-flight" in (res.reason or "")


def test_deploy_clean_not_blocked():
    prev = curr = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    res = deploy(
        prev, curr, _clean_providers(), CLASSIC, ["gold.t"], [("silver.s", True)]
    )
    assert not res.blocked
    assert res.plan.has_breaking_changes is False


class _FakeDbutils:
    def __init__(self, value: str = "v", raise_: bool = False) -> None:
        self._v = value
        self._raise = raise_

    @property
    def secrets(self):
        outer = self

        class _Secrets:
            def get(self, scope=None, key=None):  # noqa: ARG002
                if outer._raise:
                    raise RuntimeError("missing secret")
                return outer._v

        return _Secrets()


def test_check_secrets_env_set_ok():
    from kimball.common.secrets import SecretResolver
    from kimball.ops.deploy import _check_secrets

    resolver = SecretResolver(environ={"MYKEY": "x"}, dbutils=None)
    blockers, warnings = _check_secrets(("env://MYKEY",), resolver)
    assert not blockers and not warnings


def test_check_secrets_env_missing_blocks():
    from kimball.common.secrets import SecretResolver
    from kimball.ops.deploy import _check_secrets

    resolver = SecretResolver(environ={}, dbutils=None)
    blockers, warnings = _check_secrets(("env://MISSING",), resolver)
    assert len(blockers) == 1 and blockers[0].kind == "secret_unresolved"
    assert not warnings


def test_check_secrets_databricks_without_dbutils_warns():
    from kimball.common.secrets import SecretResolver
    from kimball.ops.deploy import _check_secrets

    resolver = SecretResolver(environ={}, dbutils=None)
    blockers, warnings = _check_secrets(("databricks://scope/key",), resolver)
    assert not blockers
    assert warnings and "cannot verify" in warnings[0]


def test_check_secrets_databricks_with_dbutils_ok():
    from kimball.common.secrets import SecretResolver
    from kimball.ops.deploy import _check_secrets

    resolver = SecretResolver(environ={}, dbutils=_FakeDbutils(value="secret"))
    blockers, warnings = _check_secrets(("databricks://scope/key",), resolver)
    assert not blockers and not warnings


def test_check_secrets_databricks_dbutils_fails_blocks():
    from kimball.common.secrets import SecretResolver
    from kimball.ops.deploy import _check_secrets

    resolver = SecretResolver(environ={}, dbutils=_FakeDbutils(raise_=True))
    blockers, warnings = _check_secrets(("databricks://scope/key",), resolver)
    assert len(blockers) == 1 and blockers[0].kind == "secret_unresolved"


def test_check_secrets_no_resolver_skips():
    from kimball.ops.deploy import _check_secrets

    blockers, warnings = _check_secrets(("env://X",), None)
    assert not blockers and not warnings


def test_deploy_secret_unresolved_blocks():
    from kimball.common.secrets import SecretResolver

    resolver = SecretResolver(environ={}, dbutils=None)
    prev = curr = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    res = deploy(
        prev,
        curr,
        _clean_providers(),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
        secret_refs=("env://MISSING",),
        secret_resolver=resolver,
    )
    assert res.blocked
    assert "pre-flight" in (res.reason or "")


def _pipe2(table, nkeys, digest):
    return {
        "table_name": table,
        "table_type": "dimension",
        "semantic_config": {"table_type": "dimension", "natural_keys": nkeys},
        "semantic_digest": digest,
        "metadata_digest": "m",
        "dependencies": [],
        "writes": [],
    }


def test_deploy_backfill_emits_non_blocking_warning():
    prev = {"pipelines": [_pipe2("gold.t", ["a"], "d1")]}
    curr = {"pipelines": [_pipe2("gold.t", ["a", "b"], "d2")]}
    res = deploy(
        prev, curr, _clean_providers(), CLASSIC, ["gold.t"], [("silver.s", True)]
    )
    assert not res.blocked  # backfill is not breaking
    assert any("requires backfill" in w for w in res.warnings)


def test_deploy_first_deploy_no_previous_not_blocked():
    curr = {"pipelines": [_pipe("gold.t", "dimension", "d1")]}
    res = deploy(
        {"pipelines": []},
        curr,
        _clean_providers(),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True)],
    )
    assert not res.blocked
    assert res.plan.has_breaking_changes is False


def test_preflight_serverless_warns_guard_inactive():
    pre = preflight(_clean_providers(), SERVERLESS, ["gold.t"], [("silver.s", True)])
    assert any(
        "Serverless" in w and "single-writer guard inactive" in w for w in pre.warnings
    )
    assert pre.ok  # serverless alone does not block


def test_preflight_dedups_shared_source():
    # A source listed twice (shared by two consumers) is checked once.
    from kimball.ops.providers import SourceHealthReport
    from tests.unit.ops.fakes import FakeSources

    calls = []
    base = FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, None, None, None)}
    )

    class _Counting:
        def get_source_health(
            self, source_table, watermark_version, recorded_schema_fingerprint=None
        ):
            calls.append(source_table)
            return base.get_source_health(
                source_table, watermark_version, recorded_schema_fingerprint
            )

    pre = preflight(
        providers(
            FakeControl(True, (batch("b1", "silver.s", "SUCCESS", 5),)),
            FakeHistory(True, 3, (commit(3, "b1"),)),
            _Counting(),
        ),
        CLASSIC,
        ["gold.t"],
        [("silver.s", True), ("silver.s", False)],
    )
    assert calls.count("silver.s") == 1
    assert pre.ok
