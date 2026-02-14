"""Tests for replace and replace_if_exists operations."""

import pytest
import pytest_asyncio
from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.exceptions import AerospikeError


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest.fixture
def users():
    """DataSet fixture for replace tests."""
    return DataSet.of("test", "replace_test")


class TestReplaceOperations:
    """Test replace and replace_if_exists operations."""

    async def test_replace_creates_new_record(self, client: FluentClient, users: DataSet):
        """Test that replace() creates a new record if it doesn't exist."""
        session = client.create_session()
        key = users.id("replace_new_record")
        
        # Ensure record doesn't exist
        try:
            await session.delete(key).delete()
        except Exception:
            pass
        
        # replace() should create the record
        await session.replace(key).put({"name": "New User", "status": "active"})
        
        # Verify record was created
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins["name"] == "New User"
        assert record.bins["status"] == "active"
        
        # Cleanup
        await session.delete(key).delete()

    async def test_replace_replaces_existing_record(self, client: FluentClient, users: DataSet):
        """Test that replace() completely replaces an existing record."""
        session = client.create_session()
        key = users.id("replace_existing")
        
        # Create initial record with multiple bins
        await session.key_value(key=key).put({
            "name": "Original",
            "age": 30,
            "extra_bin": "should be deleted"
        })
        
        # Replace with new bins (extra_bin should be deleted)
        await session.replace(key).put({"name": "Replaced", "status": "new"})
        
        # Verify record was replaced
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins["name"] == "Replaced"
        assert record.bins["status"] == "new"
        assert "age" not in record.bins  # Old bin should be deleted
        assert "extra_bin" not in record.bins  # Old bin should be deleted
        
        # Cleanup
        await session.delete(key).delete()

    async def test_replace_if_exists_fails_on_missing_record(self, client: FluentClient, users: DataSet):
        """Test that replace_if_exists() fails if record doesn't exist."""
        session = client.create_session()
        key = users.id("replace_if_exists_missing")
        
        # Ensure record doesn't exist
        try:
            await session.delete(key).delete()
        except Exception:
            pass
        
        # replace_if_exists() should fail
        with pytest.raises(AerospikeError):
            await session.replace_if_exists(key).put({"name": "Should Fail"})

    async def test_replace_if_exists_replaces_existing_record(self, client: FluentClient, users: DataSet):
        """Test that replace_if_exists() replaces an existing record."""
        session = client.create_session()
        key = users.id("replace_if_exists_existing")
        
        # Create initial record
        await session.key_value(key=key).put({
            "name": "Original",
            "extra": "should be deleted"
        })
        
        # replace_if_exists() should succeed
        await session.replace_if_exists(key).put({"name": "Replaced", "status": "updated"})
        
        # Verify record was replaced
        record = await session.key_value(key=key).get()
        assert record is not None
        assert record.bins["name"] == "Replaced"
        assert record.bins["status"] == "updated"
        assert "extra" not in record.bins  # Old bin should be deleted
        
        # Cleanup
        await session.delete(key).delete()

    async def test_batch_replace_if_exists(self, client: FluentClient, users: DataSet):
        """Test replace_if_exists in batch operations."""
        session = client.create_session()
        
        key1 = users.id("batch_replace_exists_1")
        key2 = users.id("batch_replace_exists_2")
        
        # Create initial records
        await session.key_value(key=key1).put({"value": "original1"})
        await session.key_value(key=key2).put({"value": "original2"})
        
        # Batch replace_if_exists
        await session.batch() \
            .replace_if_exists(key1).bin("value").set_to("replaced1") \
            .replace_if_exists(key2).bin("value").set_to("replaced2") \
            .execute()
        
        # Verify
        record1 = await session.key_value(key=key1).get()
        assert record1.bins["value"] == "replaced1"
        
        record2 = await session.key_value(key=key2).get()
        assert record2.bins["value"] == "replaced2"
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()
