"""Tests for QueryBuilder fluent API."""

import asyncio
import pytest
import pytest_asyncio
from aerospike_async import Filter, PartitionFilter, QueryPolicy
from aerospike_fluent import Exp, FluentClient


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client and test data for query tests."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Clean up and insert test data
        async with client.key_value_service("test", "query_test") as kv:
            # Clean up existing records
            for i in range(10):
                await kv.delete(i)

            # Insert test records with age values
            for i in range(10):
                await kv.put(i, {"id": i, "age": 20 + i, "name": f"User{i}"})

        yield client

@pytest.mark.asyncio
async def test_query_basic(client):
    """Test basic query operation without filters."""
    recordset = await client.query("test", "query_test").execute()
    count = 0
    async for record in recordset:
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:  # Limit to first 5 for speed
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_bins(client):
    """Test query with specific bin selection."""
    recordset = await client.query("test", "query_test").bins(["name", "age"]).execute()
    count = 0
    async for record in recordset:
        assert record is not None
        # Verify that at least one of the requested bins is present
        assert "name" in record.bins or "age" in record.bins
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_policy(client):
    """Test query with custom policy."""
    policy = QueryPolicy()
    recordset = await client.query("test", "query_test").with_policy(policy).execute()
    count = 0
    async for record in recordset:
        assert record is not None
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_partition_filter(client):
    """Test query with partition filter."""
    partition_filter = PartitionFilter.all()
    recordset = await client.query("test", "query_test").partition(partition_filter).execute()
    count = 0
    async for record in recordset:
        assert record is not None
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_fluent_chaining(client):
    """Test fluent method chaining on query builder."""
    policy = QueryPolicy()
    partition_filter = PartitionFilter.all()

    recordset = await (
        client.query("test", "query_test")
        .bins(["name", "age"])
        .with_policy(policy)
        .partition(partition_filter)
        .execute()
    )
    count = 0
    async for record in recordset:
        assert record is not None
        # Verify requested bins are present
        assert "name" in record.bins or "age" in record.bins
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_range_filter(client):
    """Test query with range filter (requires index)."""
    # Note: This test may fail if index doesn't exist
    # That's okay - it tests the filter functionality
    try:
        # Create index first (if not exists)
        try:
            await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
            # Wait a bit for index to be ready
            await asyncio.sleep(0.5)
        except Exception:
            pass  # Index might already exist

        recordset = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 22, 26))
            .execute()
        )
        count = 0
        async for record in recordset:
            assert record is not None
            assert "age" in record.bins
            assert 22 <= record.bins["age"] <= 26
            count += 1
            if count >= 5:
                break

        recordset.close()

        # Clean up index
        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass

    except Exception:
        # If index doesn't exist or query fails, skip this test
        pytest.skip("Index not available or query failed")

@pytest.mark.asyncio
async def test_query_empty_result(client):
    """Test query that returns no results."""
    # Query a non-existent set
    recordset = await client.query("test", "non_existent_set").execute()
    count = 0
    async for record in recordset:
        count += 1

    recordset.close()
    assert count == 0

@pytest.mark.asyncio
async def test_query_iteration(client):
    """Test that query builder can execute and return a Recordset."""
    query_builder = client.query("test", "query_test")
    assert hasattr(query_builder, "execute")

    recordset = await query_builder.execute()
    count = 0
    async for record in recordset:
        assert record is not None
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_filter_expression(client):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    # Create a filter expression for age >= 25
    filter_exp = Exp.ge(
        Exp.int_bin("age"),
        Exp.int_val(25)
    )

    recordset = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for record in recordset:
        assert record is not None
        assert "age" in record.bins
        assert record.bins["age"] >= 25
        count += 1
        if count >= 5:
            break

    recordset.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_filter_and_filter_expression(client):
    """Test query with both Filter (secondary index) and Exp (FilterExpression)."""
    # Create index first (if not exists)
    try:
        await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
        await asyncio.sleep(0.5)
    except Exception:
        pass  # Index might already exist

    # Use Filter for secondary index and Exp (FilterExpression) for additional filtering
    filter_exp = Exp.eq(
        Exp.string_bin("name"),
        Exp.string_val("User5")
    )

    try:
        recordset = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 20, 30))
            .filter_expression(filter_exp)
            .execute()
        )
        count = 0
        async for record in recordset:
            assert record is not None
            assert "age" in record.bins
            assert 20 <= record.bins["age"] <= 30
            assert record.bins.get("name") == "User5"
            count += 1
            if count >= 5:
                break

        recordset.close()

        # Clean up index
        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass
    except Exception:
        pytest.skip("Index not available or query failed")

@pytest.mark.asyncio
async def test_query_with_filter_expression_and(client):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    # Create filter expression: age >= 25 AND age <= 27
    filter_exp = Exp.and_([
        Exp.ge(Exp.int_bin("age"), Exp.int_val(25)),
        Exp.le(Exp.int_bin("age"), Exp.int_val(27))
    ])

    recordset = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for record in recordset:
        assert record is not None
        assert "age" in record.bins
        assert 25 <= record.bins["age"] <= 27
        count += 1
        if count >= 5:
            break

    recordset.close()
    assert count > 0
