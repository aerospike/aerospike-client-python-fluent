"""Tests for DataSet integration with FluentClient."""

import pytest

from aerospike_fluent import DataSet, FluentClient


@pytest.mark.asyncio
async def test_key_value_with_dataset(aerospike_host, client_policy):
    """Test key_value operation using DataSet."""
    users = DataSet.of("test", "users")

    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Put a record using DataSet
        await client.key_value(dataset=users, key="user1").put({"name": "John", "age": 30})

        # Get the record using DataSet
        record = await client.key_value(dataset=users, key="user1").get()
        assert record is not None
        assert record.bins["name"] == "John"
        assert record.bins["age"] == 30

        # Clean up
        await client.key_value(dataset=users, key="user1").delete()

@pytest.mark.asyncio
async def test_key_value_with_key_object(aerospike_host, client_policy):
    """Test key_value operation using Key object."""
    users = DataSet.of("test", "users")
    key = users.id("user2")

    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Put a record using Key object
        await client.key_value(key=key).put({"name": "Jane", "age": 25})

        # Get the record using Key object
        record = await client.key_value(key=key).get()
        assert record is not None
        assert record.bins["name"] == "Jane"
        assert record.bins["age"] == 25

        # Clean up
        await client.key_value(key=key).delete()

@pytest.mark.asyncio
async def test_query_with_dataset(aerospike_host, client_policy):
    """Test query operation using DataSet."""
    users = DataSet.of("test", "query_test")

    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Put some test data
        await client.key_value(dataset=users, key="q1").put({"id": "q1", "value": 10})
        await client.key_value(dataset=users, key="q2").put({"id": "q2", "value": 20})

        # Query using DataSet
        recordset = await client.query(dataset=users).execute()
        count = 0
        async for record in recordset:
            assert record is not None
            assert "id" in record.bins
            count += 1
            if count >= 2:
                break
        recordset.close()

        # Clean up
        await client.key_value(dataset=users, key="q1").delete()
        await client.key_value(dataset=users, key="q2").delete()

@pytest.mark.asyncio
async def test_query_with_single_key(aerospike_host, client_policy):
    """Test query operation using a single Key."""
    users = DataSet.of("test", "users")
    key = users.id("user3")

    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Put a record
        await client.key_value(key=key).put({"name": "Bob", "age": 35})

        # Query using single Key
        recordset = await client.query(key=key).execute()
        count = 0
        async for record in recordset:
            assert record is not None
            assert record.bins["name"] == "Bob"
            count += 1
        recordset.close()
        assert count == 1

        # Clean up
        await client.key_value(key=key).delete()

@pytest.mark.asyncio
async def test_query_with_multiple_keys(aerospike_host, client_policy):
    """Test query operation using multiple Keys."""
    users = DataSet.of("test", "users")
    keys = users.ids("user4", "user5")

    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Put records
        await client.key_value(key=keys[0]).put({"name": "Alice", "age": 28})
        await client.key_value(key=keys[1]).put({"name": "Charlie", "age": 32})

        # Query using multiple Keys (positional argument)
        recordset = await client.query(keys).execute()
        count = 0
        names = []
        async for record in recordset:
            assert record is not None
            names.append(record.bins["name"])
            count += 1
        recordset.close()
        assert count == 2
        assert "Alice" in names
        assert "Charlie" in names

        # Clean up
        await client.key_value(key=keys[0]).delete()
        await client.key_value(key=keys[1]).delete()
