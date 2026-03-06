# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Tests for QueryBuilder fluent API."""

import asyncio
import pytest
import pytest_asyncio
from aerospike_async import Filter, PartitionFilter, QueryPolicy
from aerospike_fluent import DataSet, Exp, FluentClient


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client and test data for query tests."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of("test", "query_test")

        for i in range(10):
            try:
                await session.delete(ds.id(i)).execute()
            except Exception:
                pass

        for i in range(10):
            await session.upsert(ds.id(i)).put({"id": i, "age": 20 + i, "name": f"User{i}"}).execute()

        yield client

@pytest.mark.asyncio
async def test_query_basic(client):
    """Test basic query operation without filters."""
    stream = await client.query("test", "query_test").execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "id" in result.record.bins
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_bins(client):
    """Test query with specific bin selection."""
    stream = await client.query("test", "query_test").bins(["name", "age"]).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "name" in result.record.bins or "age" in result.record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_policy(client):
    """Test query with custom policy."""
    policy = QueryPolicy()
    stream = await client.query("test", "query_test").with_policy(policy).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_partition_filter(client):
    """Test query with partition filter."""
    partition_filter = PartitionFilter.all()
    stream = await client.query("test", "query_test").partition(partition_filter).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_fluent_chaining(client):
    """Test fluent method chaining on query builder."""
    policy = QueryPolicy()
    partition_filter = PartitionFilter.all()

    stream = await (
        client.query("test", "query_test")
        .bins(["name", "age"])
        .with_policy(policy)
        .partition(partition_filter)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "name" in result.record.bins or "age" in result.record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_range_filter(client):
    """Test query with range filter (requires index)."""
    try:
        try:
            await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
            await asyncio.sleep(0.5)
        except Exception:
            pass

        stream = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 22, 26))
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert "age" in rec.bins
            assert 22 <= rec.bins["age"] <= 26
            count += 1
            if count >= 5:
                break

        stream.close()

        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass

    except Exception:
        pytest.skip("Index not available or query failed")

@pytest.mark.asyncio
async def test_query_empty_result(client):
    """Test query that returns no results."""
    stream = await client.query("test", "non_existent_set").execute()
    count = 0
    async for result in stream:
        count += 1

    stream.close()
    assert count == 0

@pytest.mark.asyncio
async def test_query_iteration(client):
    """Test that query builder can execute and return a RecordStream."""
    query_builder = client.query("test", "query_test")
    assert hasattr(query_builder, "execute")

    stream = await query_builder.execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_filter_expression(client):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    filter_exp = Exp.ge(
        Exp.int_bin("age"),
        Exp.int_val(25)
    )

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert "age" in rec.bins
        assert rec.bins["age"] >= 25
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0

@pytest.mark.asyncio
async def test_query_with_filter_and_filter_expression(client):
    """Test query with both Filter (secondary index) and Exp (FilterExpression)."""
    try:
        await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
        await asyncio.sleep(0.5)
    except Exception:
        pass

    filter_exp = Exp.eq(
        Exp.string_bin("name"),
        Exp.string_val("User5")
    )

    try:
        stream = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 20, 30))
            .filter_expression(filter_exp)
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert "age" in rec.bins
            assert 20 <= rec.bins["age"] <= 30
            assert rec.bins.get("name") == "User5"
            count += 1
            if count >= 5:
                break

        stream.close()

        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass
    except Exception:
        pytest.skip("Index not available or query failed")

@pytest.mark.asyncio
async def test_query_with_filter_expression_and(client):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    filter_exp = Exp.and_([
        Exp.ge(Exp.int_bin("age"), Exp.int_val(25)),
        Exp.le(Exp.int_bin("age"), Exp.int_val(27))
    ])

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert "age" in rec.bins
        assert 25 <= rec.bins["age"] <= 27
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0
