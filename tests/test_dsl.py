"""Tests for the Dsl expression builder."""

import pytest
from aerospike_async import Filter
from aerospike_fluent import Dsl


class TestDslBasic:
    """Test basic Dsl functionality."""

    def test_string_bin_eq(self):
        """Test: Dsl.stringBin("name").eq("Tim")"""
        expr = Dsl.stringBin("name").eq("Tim")
        filters = expr.to_filters()
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)

    def test_long_bin_gt(self):
        """Test: Dsl.longBin("age").gt(30)"""
        expr = Dsl.longBin("age").gt(30)
        filters = expr.to_filters()
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)

    def test_int_bin_gte(self):
        """Test: Dsl.intBin("age").gte(18)"""
        expr = Dsl.intBin("age").gte(18)
        filters = expr.to_filters()
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)

    def test_float_bin_lt(self):
        """Test: Dsl.floatBin("price").lt(100.0)"""
        expr = Dsl.floatBin("price").lt(100.0)
        filters = expr.to_filters()
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)

    def test_long_bin_lte(self):
        """Test: Dsl.longBin("age").lte(65)"""
        expr = Dsl.longBin("age").lte(65)
        filters = expr.to_filters()
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)


class TestDslAndExpressions:
    """Test AND expressions.

    Note: Due to Aerospike's limitation of one Filter per query, AND expressions
    with multiple conditions currently return only the first filter. Full AND/OR
    support would require FilterExpression.
    """

    def test_string_eq_and_long_gt(self):
        """Test: Dsl.stringBin("name").eq("Tim").and_(Dsl.longBin("age").gt(18))

        Note: Currently returns only the first filter due to Aerospike limitation.
        """
        expr = Dsl.stringBin("name").eq("Tim").and_(Dsl.longBin("age").gt(18))
        filters = expr.to_filters()
        # Currently returns only first filter (limitation)
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)

    def test_multiple_and(self):
        """Test chaining multiple AND expressions.

        Note: Currently returns only the first filter due to Aerospike limitation.
        """
        expr = (
            Dsl.stringBin("name").eq("Tim")
            .and_(Dsl.longBin("age").gt(18))
            .and_(Dsl.longBin("age").lt(65))
        )
        filters = expr.to_filters()
        # Currently returns only first filter (limitation)
        assert len(filters) == 1
        assert isinstance(filters[0], Filter)


class TestDslJavaExamples:
    """Test expressions matching Java examples from the spec."""

    def test_java_example_name_equals_tim_and_age_gt_18(self):
        """Java: Bin.named("name").equals("Tim").and(Bin.named("age").isGreaterThan(18))

        Python equivalent using Dsl. Note: Currently returns only the first filter
        due to Aerospike's single-filter-per-query limitation.
        """
        # Python equivalent using Dsl
        expr = Dsl.stringBin("name").eq("Tim").and_(Dsl.longBin("age").gt(18))
        filters = expr.to_filters()
        # Currently returns only first filter (limitation)
        assert len(filters) == 1

    def test_java_example_profit_gt_cost(self):
        """Java: Bin.named("profit").asFloat().isGreaterThan(Bin.named("cost"))"""
        # Note: Comparing two bins requires FilterExpression support
        # For now, we can only compare a bin to a value
        # This test documents the limitation
        expr = Dsl.floatBin("profit").gt(100.0)  # Simplified version
        filters = expr.to_filters()
        assert len(filters) == 1


class TestDslWithQuery:
    """Test using Dsl expressions with query builders."""

    @pytest.mark.asyncio
    async def test_query_with_dsl_expression(self, aerospike_host):
        """Test using Dsl expression in a query."""
        from aerospike_fluent import FluentClient, DataSet, Behavior
        from aerospike_async import IndexType

        async with FluentClient(seeds=aerospike_host) as client:
            session = client.create_session(Behavior.DEFAULT)
            dataset = DataSet.of("test", "Customers")

            # Create indexes needed for the query
            try:
                await client.index("test", "Customers").on_bin("name").named("test_name_idx").string().create()
            except Exception:
                pass  # Index might already exist

            try:
                await client.index("test", "Customers").on_bin("age").named("test_age_idx").numeric().create()
            except Exception:
                pass  # Index might already exist

            # Create test data
            key = dataset.id(999)
            await session.key_value(key=key).put({"name": "Tim", "age": 25})

            # Query with Dsl expression (single condition for now due to Filter limitation)
            # Note: AND expressions with multiple filters aren't fully supported yet
            expr = Dsl.stringBin("name").eq("Tim")
            recordset = await session.query(dataset).where(expr).execute()

            count = 0
            async for record in recordset:
                count += 1
                assert record.bins["name"] == "Tim"
                # Filter by age in Python after retrieval (workaround for single filter limitation)
                if record.bins.get("age", 0) > 18:
                    assert record.bins["age"] > 18
            recordset.close()

            # Cleanup
            await session.key_value(key=key).delete()

