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

"""Tests for Session wrapper."""

import logging

import pytest
import pytest_asyncio
from datetime import timedelta

logger = logging.getLogger(__name__)

from aerospike_fluent import Behavior, DataSet, FluentClient


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest_asyncio.fixture
async def session(client):
    """Setup session with default behavior for testing."""
    return client.create_session(Behavior.DEFAULT)


@pytest.mark.asyncio
async def test_session_creation_default_behavior(client):
    """Test creating a session with default behavior."""
    session = client.create_session()
    assert session is not None
    assert session.behavior.name == "DEFAULT"
    assert session.client is client


@pytest.mark.asyncio
async def test_session_creation_custom_behavior(client):
    """Test creating a session with custom behavior."""
    custom_behavior = Behavior.DEFAULT.derive_with_changes(
        name="custom",
        total_timeout=timedelta(seconds=10),
        max_retries=5,
    )
    session = client.create_session(custom_behavior)
    assert session is not None
    assert session.behavior.name == "custom"
    assert session.behavior.total_timeout == timedelta(seconds=10)
    assert session.behavior.max_retries == 5
    assert session.client is client


@pytest.mark.asyncio
async def test_session_repr(session):
    """Test session string representation."""
    repr_str = repr(session)
    assert "Session" in repr_str
    assert "DEFAULT" in repr_str


@pytest.mark.asyncio
async def test_session_upsert_with_key(session):
    """Test session.upsert() with Key object."""
    users = DataSet.of("test", "users")
    key = users.id("user123")

    # Upsert a record (using execute() pattern)
    await session.upsert(key=key).set_bins({"name": "John", "age": 30}).execute()

    # Verify it was written
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "John", "age": 30}


@pytest.mark.asyncio
async def test_session_upsert_with_dataset(session):
    """Test session.upsert() with DataSet."""
    users = DataSet.of("test", "users")

    # Upsert using DataSet (using execute() pattern)
    await session.upsert(dataset=users, key_value="user456").set_bins(
        {"name": "Jane", "age": 25}
    ).execute()

    # Verify it was written
    key = users.id("user456")
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "Jane", "age": 25}


@pytest.mark.asyncio
async def test_session_upsert_with_namespace_set(session):
    """Test session.upsert() with explicit namespace/set."""
    from aerospike_async import Key

    key = Key("test", "users", "user789")
    await session.upsert(
        namespace="test", set_name="users", key_value="user789"
    ).set_bins({"name": "Bob", "age": 35}).execute()

    # Verify it was written by querying the specific key
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "Bob", "age": 35}


@pytest.mark.asyncio
async def test_session_insert(session):
    """Test session.insert() method."""
    users = DataSet.of("test", "users")
    key = users.id("insert_test")

    # Insert a record (using execute() pattern)
    await session.insert(key=key).set_bins({"name": "Insert", "value": 1}).execute()

    # Verify it was written
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "Insert", "value": 1}


@pytest.mark.asyncio
async def test_session_update(session):
    """Test session.update() method."""
    users = DataSet.of("test", "users")
    key = users.id("update_test")

    # First create a record
    await session.upsert(key=key).set_bins({"name": "Original", "age": 20}).execute()

    # Update it (using execute() pattern)
    await session.update(key=key).set_bins({"age": 21}).execute()

    # Verify the update
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins["age"] == 21
        # Note: update may or may not preserve other bins depending on implementation
        # This test just verifies the method works


@pytest.mark.asyncio
async def test_session_delete(session):
    """Test session.delete() method."""
    users = DataSet.of("test", "users")
    key = users.id("delete_test")

    # First create a record
    await session.upsert(key=key).set_bins({"name": "ToDelete"}).execute()

    # Verify it exists
    exists = await session.exists(key=key).exists()
    assert exists is True

    # Delete it
    deleted = await session.delete(key=key).delete()
    assert deleted is True

    # Verify it's gone
    exists = await session.exists(key=key).exists()
    assert exists is False


@pytest.mark.asyncio
async def test_session_touch(session):
    """Test session.touch() method."""
    users = DataSet.of("test", "users")
    key = users.id("touch_test")

    # Create a record
    await session.upsert(key=key).set_bins({"name": "TouchMe"}).execute()

    # Touch it (updates TTL)
    await session.touch(key=key).touch()

    # Verify it still exists
    exists = await session.exists(key=key).exists()
    assert exists is True


@pytest.mark.asyncio
async def test_session_exists(session):
    """Test session.exists() method."""
    users = DataSet.of("test", "users")
    key = users.id("exists_test")

    # First ensure the record doesn't exist
    await session.delete(key=key).delete()

    # Record doesn't exist yet
    exists = await session.exists(key=key).exists()
    assert exists is False

    # Create the record
    await session.upsert(key=key).set_bins({"name": "Exists"}).execute()

    # Now it exists
    exists = await session.exists(key=key).exists()
    assert exists is True


@pytest.mark.asyncio
async def test_session_query_delegation(session):
    """Test that session.query() delegates to client correctly."""
    users = DataSet.of("test", "users")
    key = users.id("query_test")

    # Create a record
    await session.upsert(key=key).set_bins({"name": "QueryTest", "value": 42}).execute()

    # Query using session
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "QueryTest", "value": 42}


@pytest.mark.asyncio
async def test_session_key_value_delegation(session):
    """Test that session.key_value() delegates to client correctly."""
    users = DataSet.of("test", "users")
    key = users.id("kv_test")

    # Use session.key_value() directly
    await session.key_value(key=key).put({"name": "KVTest"})

    # Verify
    record = await session.key_value(key=key).get()
    assert record is not None
    assert record.bins == {"name": "KVTest"}


@pytest.mark.asyncio
async def test_session_index_delegation(session):
    """Test that session.index() delegates to client correctly."""
    users = DataSet.of("test", "users")

    # Create index builder using session
    index_builder = session.index(dataset=users)
    assert index_builder is not None
    assert index_builder._namespace == "test"
    assert index_builder._set_name == "users"


@pytest.mark.asyncio
async def test_session_key_value_service_delegation(session):
    """Test that session.key_value_service() delegates to client correctly."""
    users = DataSet.of("test", "users")

    # Create key-value service using session
    async with session.key_value_service(dataset=users) as kv:
        await kv.put("service_test", {"name": "ServiceTest"})
        record = await kv.get("service_test")
        assert record is not None
        assert record.bins == {"name": "ServiceTest"}


@pytest.mark.asyncio
async def test_session_upsert_error_no_key(session):
    """Test that upsert raises error when no key is provided."""
    with pytest.raises(ValueError, match="Either key.*or key_value must be provided"):
        await session.upsert().set_bins({"name": "Test"}).execute()


@pytest.mark.asyncio
async def test_session_multiple_sessions_different_behaviors(client):
    """Test creating multiple sessions with different behaviors."""
    default_session = client.create_session(Behavior.DEFAULT)
    fast_session = client.create_session(
        Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5),
        )
    )

    assert default_session.behavior.name == "DEFAULT"
    assert fast_session.behavior.name == "fast"
    assert fast_session.behavior.total_timeout == timedelta(seconds=5)
    assert default_session.behavior.total_timeout == timedelta(seconds=30)


@pytest.mark.asyncio
async def test_session_transaction_session(session):
    """Test that session.transaction_session() works."""
    tx_session = session.transaction_session()
    assert tx_session is not None


@pytest.mark.asyncio
async def test_session_behavior_immutability(session):
    """Test that behavior is immutable."""
    original_timeout = session.behavior.total_timeout

    # Try to derive a new behavior
    new_behavior = session.behavior.derive_with_changes(
        name="new",
        total_timeout=timedelta(seconds=60),
    )

    # Original behavior should be unchanged
    assert session.behavior.total_timeout == original_timeout
    assert new_behavior.total_timeout == timedelta(seconds=60)
    assert new_behavior.name == "new"


@pytest.mark.asyncio
async def test_session_query_with_dataset(session):
    """Test session.query() with DataSet."""
    users = DataSet.of("test", "users")

    # Create a record
    key = users.id("dataset_query_test")
    await session.upsert(key=key).set_bins({"name": "DatasetQuery"}).execute()

    # Query using DataSet by specific key
    stream = await session.query(key=key).execute()
    async for result in stream:
        rec = result.record
        assert rec.bins == {"name": "DatasetQuery"}


@pytest.mark.asyncio
async def test_session_query_with_multiple_keys(session):
    """Test session.query() with multiple keys."""
    users = DataSet.of("test", "users")

    # Create multiple records
    keys = users.ids("batch1", "batch2", "batch3")
    for key in keys:
        await session.upsert(key=key).set_bins({"name": f"Batch{key.value}"}).execute()

    # Query with multiple keys (positional argument)
    count = 0
    stream = await session.query(keys).execute()
    async for result in stream:
        rec = result.record
        count += 1
        assert "Batch" in rec.bins["name"]

    assert count == 3


@pytest.mark.asyncio
async def test_session_truncate(session):
    """Test session.truncate() deletes all records in a set."""
    users = DataSet.of("test", "users")

    # Create some records
    key1 = users.id("user1")
    key2 = users.id("user2")
    key3 = users.id("user3")

    await session.upsert(key=key1).set_bins({"name": "User1"}).execute()
    await session.upsert(key=key2).set_bins({"name": "User2"}).execute()
    await session.upsert(key=key3).set_bins({"name": "User3"}).execute()

    # Verify records exist
    assert await session.exists(key=key1).exists()
    assert await session.exists(key=key2).exists()
    assert await session.exists(key=key3).exists()

    await session.truncate(users)

    import asyncio
    for attempt in range(100):
        gone = (
            not await session.exists(key=key1).exists()
            and not await session.exists(key=key2).exists()
            and not await session.exists(key=key3).exists()
        )
        if gone:
            logger.debug("truncate propagated after %d retries", attempt)
            break
        await asyncio.sleep(0.1)

    assert not await session.exists(key=key1).exists()
    assert not await session.exists(key=key2).exists()
    assert not await session.exists(key=key3).exists()
