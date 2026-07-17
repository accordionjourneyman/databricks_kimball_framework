from kimball.common.runtime_policy import RuntimePolicy


def test_databricks_policy_uses_cluster_clause() -> None:
    policy = RuntimePolicy(is_databricks=True)
    assert policy.cluster_clause(["event_date"], None) == "\nCLUSTER BY (event_date)"


def test_local_policy_uses_partition_clause() -> None:
    policy = RuntimePolicy(is_databricks=False)
    assert (
        policy.cluster_clause(None, ["event_date"]) == "\nPARTITIONED BY (event_date)"
    )
