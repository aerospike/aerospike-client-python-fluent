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

"""Tests for operate operations."""

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
    """DataSet fixture for operate tests."""
    return DataSet.of("test", "operate_test")


class TestOperate:
    """Test combined operate operations."""

    async def test_operate(self, client: FluentClient, test_set: DataSet):
        """Test combined operations (add + set + get) in single call."""
        session = client.create_session()
        key = test_set.id("operate")
        bin_name1 = "optintbin"
        bin_name2 = "optstringbin"

        # Write initial record
        await session.upsert(key).bin(bin_name1).set_to(7).bin(bin_name2).set_to("string value").execute()

        # Verify initial values
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins[bin_name1] == 7
        assert record.record.bins[bin_name2] == "string value"

        # Add integer, write new string
        await (
            session.upsert(key)
                .bin(bin_name1).add(4)
                .bin(bin_name2).set_to("new string")
                .execute()
        )

        # Read record and verify values after operations
        result = await (await session.query(key).execute()).first_or_raise()
        assert result.record is not None
        assert result.record.bins is not None
        # After add(4) to 7, bin1 should be 11
        assert result.record.bins[bin_name1] == 11
        # bin2 should have new string
        assert result.record.bins[bin_name2] == "new string"

        # Cleanup
        await session.delete(key).execute()

    async def test_operate_multiple_increments(self, client: FluentClient, test_set: DataSet):
        """Test multiple increment operations on same bin."""
        session = client.create_session()
        key = test_set.id("operate_multi_inc")
        bin_name = "counter"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Set initial value
        await session.upsert(key).bin(bin_name).set_to(0).execute()

        # Increment multiple times in separate calls
        await session.upsert(key).bin(bin_name).add(5).execute()
        await session.upsert(key).bin(bin_name).add(10).execute()
        await session.upsert(key).bin(bin_name).add(15).execute()

        # Verify final value
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins[bin_name] == 30

        # Cleanup
        await session.delete(key).execute()

    async def test_operate_set_and_get(self, client: FluentClient, test_set: DataSet):
        """Test setting and getting in same operation."""
        session = client.create_session()
        key = test_set.id("operate_set_get")
        bin_name = "mybin"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Set value
        await session.upsert(key).bin(bin_name).set_to("test_value").execute()

        # Read and verify
        result = await (await session.query(key).execute()).first_or_raise()
        assert result.record is not None
        assert result.record.bins[bin_name] == "test_value"

        # Cleanup
        await session.delete(key).execute()
