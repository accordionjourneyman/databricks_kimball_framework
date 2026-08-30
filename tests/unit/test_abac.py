"""Tests for ABAC (Attribute-Based Access Control) tag policies."""

from unittest.mock import MagicMock

from kimball.common.config import ABACPolicyConfig, TableConfig


class TestABACPolicyConfig:
    def test_row_filter_policy(self):
        config = ABACPolicyConfig(
            policy_name="hide_eu",
            policy_type="row_filter",
            udf_name="non_eu_region",
            udf_body="matched_value <> 'eu'",
            target_groups=["ANALYST_USA"],
            match_tag="geo_region",
        )
        assert config.policy_type == "row_filter"
        assert config.scope == "schema"

    def test_column_mask_policy(self):
        config = ABACPolicyConfig(
            policy_name="mask_ssn",
            policy_type="column_mask",
            udf_name="mask_SSN",
            udf_body="return '***-**-****'",
            target_groups=["ANALYST_USA"],
            match_tag="pii",
            tag_value="ssn",
        )
        assert config.policy_type == "column_mask"
        assert config.tag_value == "ssn"

    def test_abac_in_table_config(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            abac_policies=[
                ABACPolicyConfig(
                    policy_name="hide_eu",
                    policy_type="row_filter",
                    udf_name="non_eu_region",
                    udf_body="matched_value <> 'eu'",
                    target_groups=["ANALYST_USA"],
                    match_tag="geo_region",
                )
            ],
        )
        assert len(config.abac_policies) == 1
        assert config.abac_policies[0].policy_name == "hide_eu"


class TestABACManager:
    def _make_manager(self):
        from kimball.governance.abac import ABACManager

        mock_spark = MagicMock()
        return ABACManager(mock_spark, "main", "my_schema"), mock_spark

    def test_create_row_filter_policy(self):
        manager, mock_spark = self._make_manager()
        config = ABACPolicyConfig(
            policy_name="hide_eu",
            policy_type="row_filter",
            udf_name="non_eu_region",
            udf_body="matched_value <> 'eu'",
            target_groups=["ANALYST_USA"],
            match_tag="geo_region",
        )
        manager.create_policy(config)

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        # Should create UDF
        udf_calls = [c for c in calls if "CREATE OR REPLACE FUNCTION" in c]
        assert len(udf_calls) == 1
        assert "non_eu_region" in udf_calls[0]

        # Should create policy
        policy_calls = [c for c in calls if "CREATE OR REPLACE POLICY" in c]
        assert len(policy_calls) == 1
        assert "hide_eu" in policy_calls[0]
        assert "ROW FILTER" in policy_calls[0]
        assert "has_tag('geo_region')" in policy_calls[0]
        assert "USING COLUMNS (`matched_value`)" in policy_calls[0]

    def test_create_column_mask_policy(self):
        manager, mock_spark = self._make_manager()
        config = ABACPolicyConfig(
            policy_name="mask_ssn",
            policy_type="column_mask",
            udf_name="mask_SSN",
            udf_body="return '***-**-****'",
            target_groups=["ANALYST_USA"],
            match_tag="pii",
            tag_value="ssn",
        )
        manager.create_policy(config)

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        policy_calls = [c for c in calls if "COLUMN MASK" in c]
        assert len(policy_calls) == 1
        assert "mask_ssn" in policy_calls[0]

    def test_apply_tag(self):
        manager, mock_spark = self._make_manager()
        manager.apply_tag("customers", "region", "geo_region")

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        tag_calls = [c for c in calls if "SET TAGS" in c]
        assert len(tag_calls) == 1
        assert "geo_region" in tag_calls[0]
        assert "region" in tag_calls[0]

    def test_apply_tag_with_value(self):
        manager, mock_spark = self._make_manager()
        manager.apply_tag("customers", "ssn", "pii", value="ssn")

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        tag_calls = [c for c in calls if "SET TAGS" in c]
        assert len(tag_calls) == 1
        assert "'pii'='ssn'" in tag_calls[0]

    def test_drop_policy(self):
        manager, mock_spark = self._make_manager()
        manager.drop_policy("hide_eu")

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        drop_calls = [c for c in calls if "DROP POLICY" in c]
        assert len(drop_calls) == 1
        assert "hide_eu" in drop_calls[0]
