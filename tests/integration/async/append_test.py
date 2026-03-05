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

"""Tests for append and prepend operations."""

import pytest
import pytest_asyncio
from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest.fixture
def test_set():
    """DataSet fixture for append tests."""
    return DataSet.of("test", "append_test")


class TestAppend:
    """Test string append operations."""

    async def test_append(self, client: FluentClient, test_set: DataSet):
        """Test appending strings to a bin."""
        session = client.create_session()
        key = test_set.id("append")
        bin_name = "appendbin"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Perform some appends and check results
        await session.upsert(key).bin(bin_name).append("Hello").execute()
        await session.upsert(key).bin(bin_name).append(" World").execute()

        # Verify result
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins[bin_name] == "Hello World"

        # Test append and get combined
        result = await (
            session.upsert(key)
                .bin(bin_name).append("!")
                .bin(bin_name).get()
                .execute()
        )

        assert result is not None
        assert result.bins[bin_name] == "Hello World!"

        # Cleanup
        await session.delete(key).delete()

    async def test_prepend(self, client: FluentClient, test_set: DataSet):
        """Test prepending strings to a bin."""
        session = client.create_session()
        key = test_set.id("prepend")
        bin_name = "prependbin"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Perform some prepends and check results
        await session.upsert(key).bin(bin_name).prepend("!").execute()
        await session.upsert(key).bin(bin_name).prepend("World").execute()

        # Verify result
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins[bin_name] == "World!"

        # Test prepend and get combined
        result = await (
            session.upsert(key)
                .bin(bin_name).prepend("Hello ")
                .bin(bin_name).get()
                .execute()
        )

        assert result is not None
        assert result.bins[bin_name] == "Hello World!"

        # Cleanup
        await session.delete(key).delete()

    async def test_append_to_multiple_keys(self, client: FluentClient, test_set: DataSet):
        """Test appending to multiple keys."""
        session = client.create_session()
        bin_name = "appendbin"
        
        key1 = test_set.id("append_multi_1")
        key2 = test_set.id("append_multi_2")

        # Delete records if they exist
        try:
            await session.delete(key1).delete()
        except Exception:
            pass
        try:
            await session.delete(key2).delete()
        except Exception:
            pass

        # Create initial records
        await session.upsert(key1).bin(bin_name).append("First").execute()
        await session.upsert(key2).bin(bin_name).append("Second").execute()

        # Append more
        await session.upsert(key1).bin(bin_name).append("_1").execute()
        await session.upsert(key2).bin(bin_name).append("_2").execute()

        # Verify
        record1 = await session.key_value(key=key1).get()
        record2 = await session.key_value(key=key2).get()
        
        assert record1.bins[bin_name] == "First_1"
        assert record2.bins[bin_name] == "Second_2"

        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()
