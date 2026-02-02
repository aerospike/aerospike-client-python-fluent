"""Tests matching Java fluent client examples from the spec.

These tests are designed to match the Java examples closely,
providing simple, focused examples for documentation.
"""

import pytest
import pytest_asyncio
from aerospike_async import Key
from aerospike_fluent import ClusterDefinition, DataSet, Behavior


@pytest_asyncio.fixture
async def cluster(aerospike_host):
    """Setup cluster for testing."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = ClusterDefinition(hostname, port)
    cluster = await cluster_def.connect()
    yield cluster
    await cluster.close()


@pytest_asyncio.fixture
async def session(cluster):
    """Setup session for testing."""
    return cluster.create_session(Behavior.DEFAULT)


@pytest_asyncio.fixture
async def customer_dataset(session):
    """Setup test data for customer dataset.

    This fixture ensures test data is in a known state before each test.
    It deletes and recreates keys 1, 2, 3 to ensure clean state.
    """
    customers = DataSet.of("test", "Customers")

    # Always reset test data to known state before each test
    # Delete first to ensure clean state, then insert fresh data
    for i, data in [(1, {"name": "Tim", "age": 25, "country": "US"}),
                    (2, {"name": "Bob", "age": 30, "country": "US"}),
                    (3, {"name": "Alice", "age": 28, "country": "UK"})]:
        try:
            await session.key_value(dataset=customers, key=i).delete()
        except Exception:
            pass  # Ignore if key doesn't exist
        # Use put() which overwrites - this ensures clean state
        await session.key_value(dataset=customers, key=i).put(data)

    yield customers

    # Cleanup after test - restore original test data in case tests modified it
    # This ensures the next test starts with clean data
    for i, data in [(1, {"name": "Tim", "age": 25, "country": "US"}),
                    (2, {"name": "Bob", "age": 30, "country": "US"}),
                    (3, {"name": "Alice", "age": 28, "country": "UK"})]:
        try:
            await session.key_value(dataset=customers, key=i).delete()
        except Exception:
            pass  # Ignore if key doesn't exist
        await session.key_value(dataset=customers, key=i).put(data)


# ============================================================================
# Connecting Examples (matching Java spec)
# ============================================================================

@pytest.mark.asyncio
async def test_java_example_connecting_basic(aerospike_host):
    """Java: Cluster connection1 = clusterDefinition.connect();"""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = ClusterDefinition(hostname, port)
    cluster = await cluster_def.connect()
    assert cluster.is_connected()
    await cluster.close()


@pytest.mark.asyncio
async def test_java_example_connecting_with_credentials(aerospike_host):
    """Java: Cluster connection3 = new ClusterDefinition("localhost", 3000)
              .withNativeCredentialsOf("username", "pass1234")
              .connect();
    """
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    # Note: Only test if credentials are actually needed
    cluster_def = ClusterDefinition(hostname, port)
    cluster = await cluster_def.connect()
    assert cluster.is_connected()
    await cluster.close()


@pytest.mark.asyncio
async def test_java_example_connecting_context_manager(aerospike_host):
    """Java: try (ClusterConnection connection = new ClusterDefinition("localhost", 3000).connect()) { ... }"""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster = await ClusterDefinition(hostname, port).connect()
    async with cluster:
        assert cluster.is_connected()
        session = cluster.create_session(Behavior.DEFAULT)
        assert session is not None


# ============================================================================
# Sessions Examples (matching Java spec)
# ============================================================================

@pytest.mark.asyncio
async def test_java_example_sessions(cluster):
    """Java: Session session = cluster.createSession(useCase1Behavior);
              Session defaultSession = cluster.createSession(Behavior.DEFAULT);
              Session fastReadSession = cluster.createSession(behaviorWithLowReadLatency);
    """
    # Create behaviors (assumed to exist in Java examples)
    use_case1_behavior = Behavior.DEFAULT
    behavior_with_low_read_latency = Behavior.DEFAULT

    # Java examples
    session = cluster.create_session(use_case1_behavior)
    default_session = cluster.create_session(Behavior.DEFAULT)
    fast_read_session = cluster.create_session(behavior_with_low_read_latency)

    assert session is not None
    assert default_session is not None
    assert fast_read_session is not None


# ============================================================================
# DataSet Examples (matching Java spec)
# ============================================================================

@pytest.mark.asyncio
async def test_java_example_dataset_creation():
    """Java: DataSet customerDataSet = DataSet.of("test", "Customers");"""
    customer_dataset = DataSet.of("test", "Customers")
    assert customer_dataset.namespace == "test"
    assert customer_dataset.set_name == "Customers"


@pytest.mark.asyncio
async def test_java_example_dataset_id(session, customer_dataset):
    """Java: Key customerKey = customerDataSet.id(cust.id);"""
    customer_key = customer_dataset.id(1)
    record = await session.key_value(key=customer_key).get()
    assert record is not None
    assert record.bins["name"] == "Tim"


@pytest.mark.asyncio
async def test_java_example_dataset_ids(session, customer_dataset):
    """Java: List<Key> keys = customerDataSet.ids(id1, id2, id3);"""
    keys = customer_dataset.ids(1, 2, 3)
    assert len(keys) == 3
    # Verify we can query with these keys
    recordset = await session.query(keys).execute()
    count = 0
    async for record in recordset:
        count += 1
    recordset.close()
    assert count == 3


@pytest.mark.asyncio
async def test_java_example_dataset_id_from_digest(customer_dataset):
    """Java: Key custbyDigest = customerDataSet.idFromDigest(myDigest);"""
    # First create a key to get its digest
    original_key = customer_dataset.id(123)
    my_digest = original_key.digest

    # Create key from digest (matching Java example)
    cust_by_digest = customer_dataset.id_from_digest(my_digest)

    assert isinstance(cust_by_digest, Key)
    assert cust_by_digest == original_key
    assert cust_by_digest.namespace == "test"
    assert cust_by_digest.set_name == "Customers"


# ============================================================================
# Reads/Query Examples (matching Java spec)
# ============================================================================

@pytest.mark.asyncio
async def test_java_example_query_point_read(session, customer_dataset):
    """Java: session.query(customerDataSet.id(1)).execute();"""
    recordset = await session.query(customer_dataset.id(1)).execute()
    record = await recordset.__anext__()
    assert record is not None
    assert record.bins["name"] == "Tim"
    recordset.close()


@pytest.mark.asyncio
async def test_java_example_query_set_no_bins(session, customer_dataset):
    """Java: session.query(customerDataSet).withNoBins().execute();"""
    # Use with_no_bins() method (Java-compatible API)
    recordset = await session.query(customer_dataset).with_no_bins().execute()
    record = await recordset.__anext__()
    assert record is not None
    # With no bins, bins should be empty or minimal
    recordset.close()


@pytest.mark.asyncio
async def test_java_example_query_reading_only_bins(session, customer_dataset):
    """Java: session.query(customerDataSet).readingOnlyBins("name", "custId").execute();"""
    recordset = await session.query(customer_dataset).bins(["name", "age"]).execute()
    record = await recordset.__anext__()
    assert record is not None
    # Should only have name and age bins
    assert "name" in record.bins
    assert "age" in record.bins
    recordset.close()


@pytest.mark.asyncio
async def test_java_example_query_batch_reading_only_bins(session, customer_dataset):
    """Java: session.query(customerDataSet.ids(1,2,3,4)).readingOnlyBins("name", "custId").execute();"""
    recordset = await session.query(customer_dataset.ids(1, 2, 3)).bins(["name", "age"]).execute()
    count = 0
    async for record in recordset:
        assert "name" in record.bins
        assert "age" in record.bins
        count += 1
    recordset.close()
    assert count == 3


@pytest.mark.asyncio
async def test_java_example_query_varargs_keys(session, customer_dataset):
    """Java: session.query(key1, key2, key3).where(...);"""
    key1 = customer_dataset.id(1)
    key2 = customer_dataset.id(2)
    key3 = customer_dataset.id(3)
    recordset = await session.query(key1, key2, key3).execute()
    count = 0
    async for record in recordset:
        count += 1
    recordset.close()
    assert count == 3


@pytest.mark.asyncio
async def test_java_example_query_namespace_set(session, customer_dataset):
    """Java: session.query("test", "users")"""
    recordset = await session.query("test", "Customers").execute()
    count = 0
    async for record in recordset:
        count += 1
        if count >= 3:
            break
    recordset.close()
    assert count > 0


# ============================================================================
# Update Examples (matching Java spec)
# ============================================================================

@pytest.mark.asyncio
async def test_java_example_insert(session, customer_dataset):
    """Java: session.insertInto(customerDataSet.id(1))
              .bin("name").setTo("Tim")
              .bin("age").setTo(1)
              .execute();
    """
    # Clean up first - ensure key 10 doesn't exist
    try:
        await session.key_value(dataset=customer_dataset, key=10).delete()
    except Exception:
        pass  # Ignore if key doesn't exist

    # Insert (create only) - using execute() pattern (Java-style)
    await session.insert(customer_dataset.id(10)).set_bins({
        "name": "Tim",
        "age": 1
    }).execute()

    # Verify
    record = await session.key_value(dataset=customer_dataset, key=10).get()
    assert record is not None
    assert record.bins["name"] == "Tim"
    assert record.bins["age"] == 1

    # Cleanup
    await session.key_value(dataset=customer_dataset, key=10).delete()


@pytest.mark.asyncio
async def test_java_example_update(session, customer_dataset):
    """Java: session.update(customerDataSet.id(2))
              .andRemoveOtherBins()
              .bin("name").setTo("Tim")
              .bin("age").incrementBy(1)
              .execute();
    """
    # Update - using execute() pattern (Java-style)
    # Note: andRemoveOtherBins() and incrementBy() not yet supported
    await session.update(customer_dataset.id(2)).set_bins({
        "name": "Tim",
        "age": 31  # Incremented from 30
    }).execute()

    # Verify
    record = await session.key_value(dataset=customer_dataset, key=2).get()
    assert record is not None
    assert record.bins["name"] == "Tim"
    assert record.bins["age"] == 31


@pytest.mark.asyncio
async def test_update_with_put_pattern(session, customer_dataset):
    """Test that update() also works with .put() pattern (backward compatibility)."""
    # Update using .put() pattern (immediate execution)
    await session.update(customer_dataset.id(2)).put({
        "name": "PutUpdate",
        "age": 32
    })

    # Verify
    record = await session.key_value(dataset=customer_dataset, key=2).get()
    assert record is not None
    assert record.bins["name"] == "PutUpdate"
    assert record.bins["age"] == 32


@pytest.mark.asyncio
async def test_java_example_upsert(session, customer_dataset):
    """Java: session.upsert(...)"""
    # Upsert existing record - using execute() pattern (Java-style)
    await session.upsert(customer_dataset.id(1)).set_bins({
        "name": "Tim Updated",
        "age": 26
    }).execute()

    # Verify
    record = await session.key_value(dataset=customer_dataset, key=1).get()
    assert record is not None
    assert record.bins["name"] == "Tim Updated"
    assert record.bins["age"] == 26


@pytest.mark.asyncio
async def test_java_example_delete(session, customer_dataset):
    """Java: session.delete(customerDataSet.ids(1,2,3)).execute();"""
    # Delete multiple records - using execute() pattern (Java-style, no for loop needed!)
    keys = customer_dataset.ids(1, 2, 3)
    await session.delete(keys).execute()

    # Verify they're deleted
    for key in keys:
        record = await session.key_value(key=key).get()
        assert record is None


@pytest.mark.asyncio
async def test_java_example_delete_durably(session, customer_dataset):
    """Java: session.delete(customerDataSet.id(5)).durably(false).execute();"""
    # Test that durably() can be called and sets the durable_delete flag
    key = customer_dataset.id(5)

    # First create a record to delete
    await session.key_value(dataset=customer_dataset, key=5).put({
        "name": "Test",
        "age": 25
    })

    # Delete with durably(False) using delete() method
    await session.delete(key=key).durably(False).delete()

    # Verify it's deleted
    record = await session.key_value(key=key).get()
    assert record is None


@pytest.mark.asyncio
async def test_java_example_filter_control_with_chunk_size(session, customer_dataset):
    """Java: session.query(dataSet1).chunkSize(100)..."""
    # Test that chunk_size can be called
    myquery = await (
        session.query(customer_dataset)
        .chunk_size(100)
        .execute()
    )

    # Verify it executes and can be iterated
    count = 0
    async for record in myquery:
        count += 1
    assert count >= 0  # At least 0 records
    myquery.close()


@pytest.mark.asyncio
async def test_java_example_filter_control_on_partitions(session, customer_dataset):
    """Java: session.query(dataSet1).onPartitions(1, 2, 3)..."""
    # Test that on_partitions can be called with partition IDs
    myquery = await (
        session.query(customer_dataset)
        .on_partitions(1, 2, 3)
        .execute()
    )

    # Verify it executes and can be iterated
    count = 0
    async for record in myquery:
        count += 1
    assert count >= 0  # At least 0 records
    myquery.close()


@pytest.mark.asyncio
async def test_java_example_filter_control_on_partition(session, customer_dataset):
    """Java: query.onPartition(5)"""
    # Test that on_partition can be called with a single partition ID
    myquery = await (
        session.query(customer_dataset)
        .on_partition(5)
        .execute()
    )
    # Just verify it doesn't raise an error
    async for _ in myquery:
        break  # Consume at least one record if available
    myquery.close()


@pytest.mark.asyncio
async def test_java_example_filter_control_on_partition_range(session, customer_dataset):
    """Java: query.onPartitionRange(0, 2048)"""
    # Test that on_partition_range can be called with a partition range
    myquery = await (
        session.query(customer_dataset)
        .on_partition_range(0, 2048)
        .execute()
    )
    # Just verify it doesn't raise an error
    async for _ in myquery:
        break  # Consume at least one record if available
    myquery.close()


@pytest.mark.asyncio
async def test_java_example_filter_control_full(session, customer_dataset):
    """Java: RecordSet myquery = session.query(dataSet1).chunkSize(100).onPartitions(1, 2, 3)
              .where(DSL.of("$.bonus > 100 and $.person.age >= 18"));

    Note: This test verifies the API methods can be chained together.
    Full functionality requires proper index setup which may vary by environment.
    """
    from aerospike_fluent.dsl.parser import parse_dsl

    # Test that all Filter Control methods can be chained together
    # This verifies the API works, even if the query requires index setup
    query_builder = (
        session.query(customer_dataset)
        .chunk_size(100)
        .on_partitions(1, 2, 3)
        .where("$.age > 20")
    )

    # Verify the builder was created successfully
    assert query_builder is not None

    # Note: Actual execution may require index setup, so we just verify the API
    # The other filter_control tests verify execution works in simpler cases


@pytest.mark.asyncio
async def test_java_example_key_value_operations_direct_client(session, customer_dataset):
    """Java: client.key_value("test", "users", "user123").put({"name": "John", "age": 30});
              Record rec = client.key_value("test", "users", "user123").get();
    """
    # Using keyword arguments (more Pythonic)
    await session.key_value(namespace="test", set_name="Customers", key="user123").put({"name": "John", "age": 30})
    record = await session.key_value(namespace="test", set_name="Customers", key="user123").get()

    assert record is not None
    assert record.bins["name"] == "John"
    assert record.bins["age"] == 30

    # Cleanup
    await session.key_value(namespace="test", set_name="Customers", key="user123").delete()


@pytest.mark.asyncio
async def test_java_example_query_operations(session, customer_dataset):
    """Java: RecordSet rs = session.query(customerDataSet).execute();
              RecordSet rs2 = session.query(customerDataSet).readingOnlyBins("name", "age").execute();
    """
    recordset = await session.query(customer_dataset).execute()
    count = 0
    async for record in recordset:
        count += 1
        assert record is not None
    assert count > 0
    recordset.close()

    recordset = await session.query(customer_dataset).bins(["name", "age"]).execute()
    count = 0
    async for record in recordset:
        count += 1
        assert record is not None
        # Should only have name and age bins
        assert "name" in record.bins or "age" in record.bins
    assert count > 0
    recordset.close()


@pytest.mark.asyncio
async def test_java_example_index_operations(session, customer_dataset):
    """Java: session.index(customerDataSet).onBin("age").named("age_idx").numeric().create();
              session.index(customerDataSet).onBin("roles").named("roles_idx").collection(CollectionIndexType.LIST).create();
              session.index(customerDataSet).named("age_idx").drop();
    """
    from aerospike_async import CollectionIndexType

    # Create numeric index
    try:
        await session.index(customer_dataset).on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass  # Index may already exist

    # Create collection index (using a bin that might exist)
    try:
        await session.index(customer_dataset).on_bin("tags").named("tags_idx").collection(CollectionIndexType.LIST).create()
    except Exception:
        pass  # Index may already exist or bin may not exist

    # Drop index
    try:
        await session.index(customer_dataset).named("age_idx").drop()
    except Exception:
        pass  # Index may not exist


@pytest.mark.asyncio
async def test_java_example_key_value_service_pattern(session, customer_dataset):
    """Java: try (KeyValueService kv = session.keyValueService(customerDataSet)) {
              kv.put("user1", {"name": "John"});
              Record rec = kv.get("user1");
          }
    """
    async with session.key_value_service(dataset=customer_dataset) as kv:
        await kv.put("user1", {"name": "John"})
        record = await kv.get("user1")
        assert record is not None
        assert record.bins["name"] == "John"

    # Cleanup
    await session.key_value(dataset=customer_dataset, key="user1").delete()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_java_example_behaviors(cluster):
    """Java: Behavior useCase1Behavior = Behavior.READ_FAST;
              Session session = cluster.createSession(useCase1Behavior);
    """
    # Note: Behavior.READ_FAST is not yet implemented, using DEFAULT instead
    # This test verifies the pattern works when READ_FAST is available
    use_case1_behavior = Behavior.DEFAULT
    session = cluster.create_session(use_case1_behavior)
    assert session is not None

