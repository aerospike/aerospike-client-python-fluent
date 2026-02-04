"""Tests for batch operations with multi-key chaining.

Tests both:
1. Heterogeneous batch operations (different ops on different keys) - session.batch()
2. Homogeneous batch operations (same op on multiple keys) - session.exists/delete/query with multiple keys
"""

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
def users():
    """DataSet fixture for batch tests."""
    return DataSet.of("test", "batch_test")


class TestBatchOperations:
    """Test batch operation builder with multi-key chaining."""

    async def test_batch_insert_multiple_keys(self, client: FluentClient, users: DataSet):
        """Test inserting multiple records in a single batch."""
        session = client.create_session()
        
        key1 = users.id("batch_user_1")
        key2 = users.id("batch_user_2")
        key3 = users.id("batch_user_3")
        
        # Clean up first
        try:
            await session.delete(key1).delete()
            await session.delete(key2).delete()
            await session.delete(key3).delete()
        except Exception:
            pass
        
        # Insert multiple records with chained operations
        results = await session.batch() \
            .insert(key1).bin("name").set_to("Alice").bin("age").set_to(25) \
            .insert(key2).bin("name").set_to("Bob").bin("age").set_to(30) \
            .insert(key3).put({"name": "Charlie", "age": 35}) \
            .execute()
        
        assert len(results) == 3
        
        # Verify records were created
        record1 = await session.key_value(key=key1).get()
        assert record1 is not None
        assert record1.bins["name"] == "Alice"
        assert record1.bins["age"] == 25
        
        record2 = await session.key_value(key=key2).get()
        assert record2 is not None
        assert record2.bins["name"] == "Bob"
        assert record2.bins["age"] == 30
        
        record3 = await session.key_value(key=key3).get()
        assert record3 is not None
        assert record3.bins["name"] == "Charlie"
        assert record3.bins["age"] == 35
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()
        await session.delete(key3).delete()

    async def test_batch_mixed_operations(self, client: FluentClient, users: DataSet):
        """Test batch with mixed insert, update, and delete operations."""
        session = client.create_session()
        
        key1 = users.id("batch_mixed_1")
        key2 = users.id("batch_mixed_2")
        key3 = users.id("batch_mixed_3")
        
        # Setup: create initial records
        await session.key_value(key=key1).put({"counter": 10})
        await session.key_value(key=key2).put({"name": "ToDelete"})
        
        # Clean key3 if exists
        try:
            await session.delete(key3).delete()
        except Exception:
            pass
        
        # Execute mixed batch operations
        results = await session.batch() \
            .update(key1).bin("counter").increment_by(5) \
            .delete(key2) \
            .insert(key3).bin("status").set_to("new") \
            .execute()
        
        assert len(results) == 3
        
        # Verify update worked
        record1 = await session.key_value(key=key1).get()
        assert record1 is not None
        assert record1.bins["counter"] == 15
        
        # Verify delete worked
        record2 = await session.key_value(key=key2).get()
        assert record2 is None
        
        # Verify insert worked
        record3 = await session.key_value(key=key3).get()
        assert record3 is not None
        assert record3.bins["status"] == "new"
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key3).delete()

    async def test_batch_upsert_operations(self, client: FluentClient, users: DataSet):
        """Test batch upsert operations."""
        session = client.create_session()
        
        key1 = users.id("batch_upsert_1")
        key2 = users.id("batch_upsert_2")
        
        # Clean up first
        try:
            await session.delete(key1).delete()
            await session.delete(key2).delete()
        except Exception:
            pass
        
        # First batch: create records
        await session.batch() \
            .upsert(key1).bin("value").set_to("initial1") \
            .upsert(key2).bin("value").set_to("initial2") \
            .execute()
        
        # Verify initial values
        record1 = await session.key_value(key=key1).get()
        assert record1.bins["value"] == "initial1"
        
        # Second batch: update existing records (upsert)
        await session.batch() \
            .upsert(key1).bin("value").set_to("updated1") \
            .upsert(key2).bin("value").set_to("updated2") \
            .execute()
        
        # Verify updated values
        record1 = await session.key_value(key=key1).get()
        assert record1.bins["value"] == "updated1"
        
        record2 = await session.key_value(key=key2).get()
        assert record2.bins["value"] == "updated2"
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()

    async def test_batch_delete_multiple_keys(self, client: FluentClient, users: DataSet):
        """Test deleting multiple records in a single batch."""
        session = client.create_session()
        
        key1 = users.id("batch_del_1")
        key2 = users.id("batch_del_2")
        key3 = users.id("batch_del_3")
        
        # Setup: create records
        await session.key_value(key=key1).put({"data": "1"})
        await session.key_value(key=key2).put({"data": "2"})
        await session.key_value(key=key3).put({"data": "3"})
        
        # Delete all in one batch
        results = await session.batch() \
            .delete(key1) \
            .delete(key2) \
            .delete(key3) \
            .execute()
        
        assert len(results) == 3
        
        # Verify all deleted
        assert await session.key_value(key=key1).get() is None
        assert await session.key_value(key=key2).get() is None
        assert await session.key_value(key=key3).get() is None

    async def test_batch_empty_raises_error(self, client: FluentClient):
        """Test that executing an empty batch raises an error."""
        session = client.create_session()
        
        with pytest.raises(ValueError, match="No operations to execute"):
            await session.batch().execute()

    async def test_batch_bin_string_operations(self, client: FluentClient, users: DataSet):
        """Test batch with string bin operations (append/prepend)."""
        session = client.create_session()
        
        key1 = users.id("batch_str_1")
        key2 = users.id("batch_str_2")
        
        # Setup
        await session.key_value(key=key1).put({"message": "Hello"})
        await session.key_value(key=key2).put({"message": "World"})
        
        # Append and prepend in batch
        await session.batch() \
            .update(key1).bin("message").append(" World") \
            .update(key2).bin("message").prepend("Hello ") \
            .execute()
        
        # Verify
        record1 = await session.key_value(key=key1).get()
        assert record1.bins["message"] == "Hello World"
        
        record2 = await session.key_value(key=key2).get()
        assert record2.bins["message"] == "Hello World"
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()


class TestHomogeneousBatchOperations:
    """
    Test homogeneous batch operations (same operation on multiple keys).
    
    Tests for homogeneous batch operations:
    - batchExists
    - batchReads (via query)
    - batchReadHeaders (via query with no bins)
    - batchDelete
    """

    @pytest_asyncio.fixture
    async def setup_batch_data(self, client: FluentClient, users: DataSet):
        """Setup test data for batch operations."""
        session = client.create_session()
        size = 10
        key_prefix = "batchkey"
        value_prefix = "batchvalue"
        
        # Create test records
        for i in range(1, size + 1):
            key = users.id(f"{key_prefix}{i}")
            list_data = [j * i for j in range(i)]
            
            if i != 6:
                await session.key_value(key=key).put({
                    "bbin": f"{value_prefix}{i}",
                    "lbin": list_data,
                })
            else:
                # Record 6 has integer value instead of string
                await session.key_value(key=key).put({
                    "bbin": i,
                    "lbin": list_data,
                })
        
        yield {
            "session": session,
            "size": size,
            "key_prefix": key_prefix,
            "value_prefix": value_prefix,
            "users": users,
        }
        
        # Cleanup
        for i in range(1, size + 1):
            key = users.id(f"{key_prefix}{i}")
            try:
                await session.delete(key).delete()
            except Exception:
                pass

    async def test_batch_exists_homogeneous(
        self, client: FluentClient, users: DataSet, setup_batch_data
    ):
        """
        Test batch exists operation on multiple keys.
        Test batch exists operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])
        
        # Check existence of all keys
        results = await session.exists(keys).respond_all_keys().execute()
        
        assert len(results) == size
        for i, exists in enumerate(results):
            assert exists is True, f"exists[{i}] is False"

    async def test_batch_reads_homogeneous(
        self, client: FluentClient, users: DataSet, setup_batch_data
    ):
        """
        Test batch read operation on multiple keys via query.
        Test batch reads operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        value_prefix = data["value_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])
        
        # Read all keys with specific bin
        recordset = await session.query(keys).bins(["bbin"]).execute()
        
        records = []
        async for record in recordset:
            records.append(record)
        
        assert len(records) == size
        
        for i, rec in enumerate(records):
            if i != 5:  # Record 6 (index 5) has integer value
                val = rec.bins.get("bbin")
                assert val == f"{value_prefix}{i + 1}", f"record[{i}] has wrong value"
            else:
                val = rec.bins.get("bbin")
                assert val == i + 1, f"record[{i}] has wrong integer value"

    async def test_batch_read_headers_homogeneous(
        self, client: FluentClient, users: DataSet, setup_batch_data
    ):
        """
        Test batch read headers (metadata only) via query.
        Test batch read headers operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])
        
        # Read headers only (no bins)
        recordset = await session.query(keys).with_no_bins().execute()
        
        records = []
        async for record in recordset:
            records.append(record)
        
        assert len(records) == size
        
        for i, rec in enumerate(records):
            assert rec is not None
            assert rec.generation != 0, f"record[{i}] generation is 0"

    async def test_batch_delete_homogeneous(
        self, client: FluentClient, users: DataSet
    ):
        """
        Test batch delete operation on multiple keys.
        Test batch delete operation.
        """
        session = client.create_session()
        
        # Create test records
        first_key = 10000
        num_keys = 10
        keys = users.ids(*[first_key + i for i in range(num_keys)])
        
        for i, key in enumerate(keys):
            await session.key_value(key=key).put({"bbin": first_key + i})
        
        # Ensure keys exist
        exists_results = await session.exists(keys).respond_all_keys().execute()
        assert len(exists_results) == num_keys
        for status in exists_results:
            assert status is True
        
        # Delete all keys using homogeneous batch delete
        delete_results = await session.delete(keys).respond_all_keys().execute()
        assert len(delete_results) == num_keys
        
        # Ensure keys no longer exist
        exists_after = await session.exists(keys).respond_all_keys().execute()
        assert len(exists_after) == num_keys
        for status in exists_after:
            assert status is False

    async def test_batch_exists_with_varargs(
        self, client: FluentClient, users: DataSet
    ):
        """Test batch exists using varargs style."""
        session = client.create_session()
        
        key1 = users.id("vararg_exist_1")
        key2 = users.id("vararg_exist_2")
        key3 = users.id("vararg_exist_3")
        
        # Create some records
        await session.key_value(key=key1).put({"data": "1"})
        await session.key_value(key=key2).put({"data": "2"})
        # key3 intentionally not created
        
        # Check exists using varargs
        results = await session.exists(key1, key2, key3).execute()
        
        assert len(results) == 3
        assert results[0] is True   # key1 exists
        assert results[1] is True   # key2 exists
        assert results[2] is False  # key3 does not exist
        
        # Cleanup
        await session.delete(key1).delete()
        await session.delete(key2).delete()

    async def test_batch_delete_with_varargs(
        self, client: FluentClient, users: DataSet
    ):
        """Test batch delete using varargs style."""
        session = client.create_session()
        
        key1 = users.id("vararg_del_1")
        key2 = users.id("vararg_del_2")
        key3 = users.id("vararg_del_3")
        
        # Create records
        await session.key_value(key=key1).put({"data": "1"})
        await session.key_value(key=key2).put({"data": "2"})
        await session.key_value(key=key3).put({"data": "3"})
        
        # Delete using varargs
        results = await session.delete(key1, key2, key3).execute()
        
        assert len(results) == 3
        
        # Verify all deleted
        exists_results = await session.exists(key1, key2, key3).execute()
        for exists in exists_results:
            assert exists is False
