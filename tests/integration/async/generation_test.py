"""Tests for generation (optimistic locking) operations."""

import pytest
import pytest_asyncio
from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet
from aerospike_async.exceptions import ServerError


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest.fixture
def test_set():
    """DataSet fixture for generation tests."""
    return DataSet.of("test", "generation_test")


class TestGeneration:
    """Test generation-based optimistic locking."""

    async def test_generation_basic(self, client: FluentClient, test_set: DataSet):
        """Test that generation increments with each update."""
        session = client.create_session()
        key = test_set.id("generation_basic")
        bin_name = "genbin"

        # Delete record if it already exists
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # First write - generation should be 1
        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()

        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.generation == 1

        # Second write - generation should be 2
        await session.upsert(key).bin(bin_name).set_to("genvalue2").execute()

        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.generation == 2
        assert record.bins[bin_name] == "genvalue2"

        # Cleanup
        await session.delete(key).delete()

    async def test_generation_check_success(self, client: FluentClient, test_set: DataSet):
        """Test successful update with correct generation."""
        session = client.create_session()
        key = test_set.id("generation_check_success")
        bin_name = "genbin"

        # Delete and create fresh record
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()
        await session.upsert(key).bin(bin_name).set_to("genvalue2").execute()

        # Get current generation
        record = await session.key_value(key=key).get()
        current_gen = record.generation

        # Update with correct generation - should succeed
        await session.upsert(key) \
            .ensure_generation_is(current_gen) \
            .bin(bin_name).set_to("genvalue3") \
            .execute()

        # Verify update succeeded
        record = await session.key_value(key=key).get()
        assert record.bins[bin_name] == "genvalue3"

        # Cleanup
        await session.delete(key).delete()

    async def test_generation_check_failure(self, client: FluentClient, test_set: DataSet):
        """Test that update fails with incorrect generation."""
        session = client.create_session()
        key = test_set.id("generation_check_failure")
        bin_name = "genbin"

        # Delete and create fresh record
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()

        # Try to update with wrong generation - should fail
        with pytest.raises(ServerError) as exc_info:
            await session.upsert(key) \
                .ensure_generation_is(9999) \
                .bin(bin_name).set_to("genvalue_should_fail") \
                .execute()

        # Verify it's a generation error
        error_str = str(exc_info.value).lower()
        assert "generation" in error_str or "gen" in error_str

        # Verify original value unchanged
        record = await session.key_value(key=key).get()
        assert record.bins[bin_name] == "genvalue1"

        # Cleanup
        await session.delete(key).delete()

    async def test_generation_concurrent_update(self, client: FluentClient, test_set: DataSet):
        """Test optimistic locking pattern for concurrent updates."""
        session = client.create_session()
        key = test_set.id("generation_concurrent")
        bin_name = "counter"

        # Delete and create fresh record
        try:
            await session.delete(key).delete()
        except Exception:
            pass

        # Initialize counter
        await session.upsert(key).bin(bin_name).set_to(0).execute()

        # Simulate read-modify-write pattern
        record = await session.key_value(key=key).get()
        current_value = record.bins[bin_name]
        current_gen = record.generation

        # Update with generation check
        new_value = current_value + 10
        await session.upsert(key) \
            .ensure_generation_is(current_gen) \
            .bin(bin_name).set_to(new_value) \
            .execute()

        # Verify
        record = await session.key_value(key=key).get()
        assert record.bins[bin_name] == 10
        assert record.generation == current_gen + 1

        # Cleanup
        await session.delete(key).delete()
