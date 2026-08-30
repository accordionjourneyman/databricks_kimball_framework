"""Tests for centralized data-quality rules store."""

from unittest.mock import MagicMock


class TestQualityRuleStore:
    """Tests for QualityRuleStore (observability/quality_rules.py)."""

    def _make_store(self):
        from kimball.observability.quality_rules import QualityRuleStore

        mock_spark = MagicMock()
        return QualityRuleStore(mock_spark, "test_etl"), mock_spark

    def test_ensure_table_called_on_init(self):
        store, mock_spark = self._make_store()
        # _ensure_table should have issued a CREATE TABLE IF NOT EXISTS
        mock_spark.sql.assert_called()
        call_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_sql
        assert "kimball_quality_rules" in call_sql

    def test_get_rules_empty_when_no_data(self):
        store, mock_spark = self._make_store()
        mock_spark.sql.return_value = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.sql.return_value.filter.return_value = mock_spark.sql.return_value

        # get_rules should return empty dict when no rows
        mock_spark.table.return_value = mock_spark.sql.return_value
        mock_spark.table.return_value.where.return_value = mock_spark.table.return_value
        mock_spark.table.return_value.collect.return_value = []

        result = store.get_rules("silver", "customer")
        assert result == {}

    def test_add_rule_calls_insert(self):
        store, mock_spark = self._make_store()
        store.add_rule("silver", "customer", "valid_email", "email IS NOT NULL")

        # Should call spark.sql with INSERT
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        insert_calls = [c for c in calls if "INSERT" in c]
        assert insert_calls

    def test_list_rules_returns_dataframe(self):
        store, mock_spark = self._make_store()
        store.list_rules()
        # list_rules should call spark.sql
        assert mock_spark.sql.called

    def test_get_rules_escapes_quote_in_tag(self):
        store, mock_spark = self._make_store()
        mock_spark.sql.return_value = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        store.get_rules("silver", "cust'omer")
        sqls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        lookup = [s for s in sqls if "WHERE" in s and "rule_name" in s][0]
        assert "tag = 'cust''omer'" in lookup  # doubled quote = escaped
        assert "tag = 'cust'omer'" not in lookup  # unescaped would break the predicate

    def test_list_rules_escapes_quote_in_layer(self):
        store, mock_spark = self._make_store()
        store.list_rules(layer="sil'ver")
        sqls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        filt = [s for s in sqls if "WHERE layer" in s][0]
        assert "layer = 'sil''ver'" in filt


class TestQualityRuleStoreIntegration:
    """Integration tests requiring a real Spark session."""

    def test_end_to_end_crud(self, spark):
        from kimball.observability.quality_rules import QualityRuleStore

        spark.sql("CREATE DATABASE IF NOT EXISTS test_dq_db")
        try:
            store = QualityRuleStore(spark, "test_dq_db")

            # Add rules
            store.add_rule("silver", "customer", "valid_email", "email IS NOT NULL")
            store.add_rule("silver", "customer", "valid_name", "name IS NOT NULL")
            store.add_rule("silver", "order", "valid_amount", "amount > 0")

            # Retrieve by layer+tag
            rules = store.get_rules("silver", "customer")
            assert "valid_email" in rules
            assert "valid_name" in rules
            assert rules["valid_email"] == "email IS NOT NULL"

            # Retrieve different tag
            order_rules = store.get_rules("silver", "order")
            assert "valid_amount" in order_rules

            # List all
            all_rules = store.list_rules()
            assert all_rules.count() >= 3

            # List by layer
            silver_rules = store.list_rules(layer="silver")
            assert silver_rules.count() >= 3
        finally:
            spark.sql("DROP DATABASE IF EXISTS test_dq_db CASCADE")

    def test_get_rules_returns_empty_for_unknown_tag(self, spark):
        from kimball.observability.quality_rules import QualityRuleStore

        spark.sql("CREATE DATABASE IF NOT EXISTS test_dq_empty_db")
        try:
            store = QualityRuleStore(spark, "test_dq_empty_db")
            rules = store.get_rules("gold", "nonexistent")
            assert rules == {}
        finally:
            spark.sql("DROP DATABASE IF EXISTS test_dq_empty_db CASCADE")
