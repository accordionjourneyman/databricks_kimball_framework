from kimball.common.runtime_policy import RuntimePolicy


def test_databricks_policy_uses_cluster_clause() -> None:
    policy = RuntimePolicy(is_databricks=True)
    assert policy.cluster_clause(["event_date"]) == "\nCLUSTER BY (event_date)"


def test_non_databricks_policy_omits_cluster_clause() -> None:
    assert RuntimePolicy(is_databricks=False).cluster_clause(["event_date"]) == ""


def test_empty_cluster_columns_have_no_clause() -> None:
    assert RuntimePolicy(is_databricks=True).cluster_clause(None) == ""
