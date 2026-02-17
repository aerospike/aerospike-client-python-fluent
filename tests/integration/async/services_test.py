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

"""Tests for service pattern (KeyValueService and TransactionalSession)."""

import pytest
import pytest_asyncio
from aerospike_fluent import FluentClient


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Clean up test records before each test
        async with client.key_value_service("test", "test") as kv:
            await kv.delete(1)
            await kv.delete(2)
            await kv.delete("user1")
        yield client

@pytest.mark.asyncio
async def test_key_value_service_put_get(client):
    """Test KeyValueService put and get operations."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put(1, {"name": "John", "age": 30})

        record = await kv.get(1)
        assert record is not None
        assert record.bins == {"name": "John", "age": 30}

@pytest.mark.asyncio
async def test_key_value_service_multiple_operations(client):
    """Test multiple operations using the same service."""
    async with client.key_value_service("test", "test") as kv:
        # Put multiple records
        await kv.put(1, {"name": "John"})
        await kv.put(2, {"name": "Jane"})

        # Get records
        record1 = await kv.get(1)
        record2 = await kv.get(2)

        assert record1 is not None
        assert record1.bins == {"name": "John"}
        assert record2 is not None
        assert record2.bins == {"name": "Jane"}

@pytest.mark.asyncio
async def test_key_value_service_get_with_bins(client):
    """Test getting specific bins with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put(1, {"name": "John", "age": 30, "city": "NYC"})

        record = await kv.get(1, bins=["name", "age"])
        assert record is not None
        assert record.bins == {"name": "John", "age": 30}
        assert "city" not in record.bins

@pytest.mark.asyncio
async def test_key_value_service_delete(client):
    """Test delete operation with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put(1, {"name": "John"})

        existed = await kv.delete(1)
        assert existed is True

        exists = await kv.exists(1)
        assert exists is False

@pytest.mark.asyncio
async def test_key_value_service_exists(client):
    """Test exists operation with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        exists = await kv.exists(1)
        assert exists is False

        await kv.put(1, {"name": "John"})

        exists = await kv.exists(1)
        assert exists is True

@pytest.mark.asyncio
async def test_key_value_service_add(client):
    """Test add operation with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put(1, {"counter": 10})

        await kv.add(1, {"counter": 5})

        record = await kv.get(1)
        assert record is not None
        assert record.bins == {"counter": 15}

@pytest.mark.asyncio
async def test_key_value_service_append_prepend(client):
    """Test append and prepend operations with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put(1, {"name": "John"})

        await kv.append(1, {"name": " Doe"})
        await kv.prepend(1, {"name": "Mr. "})

        record = await kv.get(1)
        assert record is not None
        assert record.bins == {"name": "Mr. John Doe"}

@pytest.mark.asyncio
async def test_key_value_service_string_keys(client):
    """Test using string keys with KeyValueService."""
    async with client.key_value_service("test", "test") as kv:
        await kv.put("user1", {"name": "John"})

        record = await kv.get("user1")
        assert record is not None
        assert record.bins == {"name": "John"}

@pytest.mark.asyncio
async def test_transactional_session_basic(client):
    """Test basic TransactionalSession usage."""
    async with client.transaction_session() as session:
        kv = session.key_value("test", "test")
        await kv.put(1, {"name": "John"})
        await kv.put(2, {"name": "Jane"})

        record1 = await kv.get(1)
        record2 = await kv.get(2)

        assert record1 is not None
        assert record2 is not None
        # Transaction would be auto-committed on exit (when supported)

@pytest.mark.asyncio
async def test_transactional_session_context_manager(client):
    """Test TransactionalSession context manager behavior."""
    session = client.transaction_session()
    assert not session._active

    async with session:
        assert session._active

    # Session should be inactive after exit
    assert not session._active
