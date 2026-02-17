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

"""Tests for numeric add (increment) operations."""

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
    """DataSet fixture for add tests."""
    return DataSet.of("test", "add_test")


class TestAdd:
    """Test numeric add (increment) operations."""

    async def test_add(self, client: FluentClient, test_set: DataSet):
        """Test adding integers to a bin."""
        session = client.create_session()
        key = test_set.id("addkey")
        bin_name = "addbin"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Perform some adds and check results
        await session.upsert(key).bin(bin_name).add(10).execute()
        await session.upsert(key).bin(bin_name).add(5).execute()

        # Verify result
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins[bin_name] == 15

        # Test add and get combined
        result = await session.upsert(key) \
            .bin(bin_name).add(30) \
            .bin(bin_name).get() \
            .execute()

        assert result is not None
        assert result.bins[bin_name] == 45

        # Cleanup
        await session.delete(key).delete()

    async def test_add_negative(self, client: FluentClient, test_set: DataSet):
        """Test adding negative values (decrement)."""
        session = client.create_session()
        key = test_set.id("add_negative")
        bin_name = "counter"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Start with 100
        await session.upsert(key).bin(bin_name).add(100).execute()
        
        # Subtract 30
        await session.upsert(key).bin(bin_name).add(-30).execute()

        # Verify result
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins[bin_name] == 70

        # Cleanup
        await session.delete(key).delete()

    async def test_increment_by_alias(self, client: FluentClient, test_set: DataSet):
        """Test that increment_by is an alias for add."""
        session = client.create_session()
        key = test_set.id("increment_alias")
        bin_name = "counter"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Use increment_by (same as add)
        await session.upsert(key).bin(bin_name).increment_by(10).execute()
        await session.upsert(key).bin(bin_name).increment_by(5).execute()

        # Verify result
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins[bin_name] == 15

        # Cleanup
        await session.delete(key).delete()

    async def test_add_batch(self, client: FluentClient, test_set: DataSet):
        """Test adding to multiple keys."""
        session = client.create_session()
        bin_name = "addbin"
        
        # Create keys 10-19
        keys = [test_set.id(i) for i in range(10, 20)]

        # Delete all keys first
        for key in keys:
            try:
                await session.delete(key).delete()
            except Exception:
                pass

        # Add 10 to each key
        for key in keys:
            await session.upsert(key).bin(bin_name).add(10).execute()

        # Add 5 more to each key
        for key in keys:
            await session.upsert(key).bin(bin_name).add(5).execute()

        # Verify all keys have value 15
        for key in keys:
            record = await session.key_value(key=key).get()
            assert record is not None
            assert record.bins[bin_name] == 15

        # Add 30 more and verify
        for key in keys:
            result = await session.upsert(key) \
                .bin(bin_name).add(30) \
                .bin(bin_name).get() \
                .execute()
            assert result is not None
            assert result.bins[bin_name] == 45

        # Cleanup
        for key in keys:
            await session.delete(key).delete()
