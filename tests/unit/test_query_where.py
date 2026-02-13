"""Unit tests for QueryBuilder and SyncQueryBuilder where() overloads.

Tests the three forms: where(str), where(str, *params), and where(FilterExpression).
"""

from aerospike_fluent import Exp, parse_dsl
from aerospike_fluent.aio.operations.query import QueryBuilder
from aerospike_fluent.sync.operations.query import SyncQueryBuilder


def _query_builder():
    """Return a QueryBuilder with a fake client (no real connection)."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test")


class TestQueryBuilderWhere:
    """Test QueryBuilder.where() overloads."""

    def test_where_dsl_string_sets_filter_expression(self):
        """where(str) parses DSL and sets _filter_expression."""
        builder = _query_builder()
        expected = parse_dsl("$.age > 20")
        result = builder.where("$.age > 20")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_dsl_string_with_params_sets_filter_expression(self):
        """where(str, *params) formats DSL then parses (aligns with JFC String.format)."""
        builder = _query_builder()
        expected = parse_dsl("$.age > 21")
        result = builder.where("$.age > %s", 21)
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_dsl_string_with_multiple_params(self):
        """where(str, *params) with multiple format placeholders."""
        builder = _query_builder()
        expected = parse_dsl("$.age > 30 and $.name == 'Alice'")
        result = builder.where("$.age > %s and $.name == '%s'", 30, "Alice")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_filter_expression_sets_filter_expression(self):
        """where(FilterExpression) stores the expression directly."""
        builder = _query_builder()
        exp = Exp.gt(Exp.int_bin("a"), Exp.int_val(100))
        result = builder.where(exp)
        assert result is builder
        assert builder._filter_expression is exp

    def test_where_filter_expression_chains(self):
        """where(Exp) can be chained with other builder methods."""
        builder = _query_builder()
        exp = Exp.eq(Exp.string_bin("name"), Exp.string_val("Bob"))
        builder.where(exp).bins(["name"])
        assert builder._filter_expression is exp
        assert builder._bins == ["name"]


class TestSyncQueryBuilderWhere:
    """Test SyncQueryBuilder.where() overloads (same behavior as QueryBuilder)."""

    def _sync_builder(self):
        """Return a SyncQueryBuilder with fake deps (no real connection)."""
        return SyncQueryBuilder(
            async_client=object(),
            namespace="test",
            set_name="unit_test",
            loop_manager=object(),
        )

    def test_where_dsl_string_sets_filter_expression(self):
        """where(str) parses DSL and sets _filter_expression."""
        builder = self._sync_builder()
        expected = parse_dsl("$.age > 20")
        result = builder.where("$.age > 20")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_dsl_string_with_params_sets_filter_expression(self):
        """where(str, *params) formats DSL then parses (aligns with JFC String.format)."""
        builder = self._sync_builder()
        expected = parse_dsl("$.age > 21")
        result = builder.where("$.age > %s", 21)
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_filter_expression_sets_filter_expression(self):
        """where(FilterExpression) stores the expression directly."""
        builder = self._sync_builder()
        exp = Exp.gt(Exp.int_bin("a"), Exp.int_val(100))
        result = builder.where(exp)
        assert result is builder
        assert builder._filter_expression is exp
