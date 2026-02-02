"""Tests for KeyValueOperation fluent API."""

import pytest
import pytest_asyncio
from aerospike_async import ListOperation, ListPolicy, ListOrderType, MapOperation, MapPolicy, MapOrder, MapReturnType, Operation, RecordExistsAction, ServerError
from aerospike_fluent import FluentClient


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for testing."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Clean up test record before each test
        await client.key_value(
            namespace="test",
            set_name="test",
            key=1
        ).delete()
        yield client

@pytest.mark.asyncio
async def test_put_get_basic(client):
    """Test basic put and get operations."""
    # Put a record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John", "age": 30})

    # Get the record
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "John", "age": 30}

@pytest.mark.asyncio
async def test_put_get_int(client):
    """Test putting and getting integer values."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin": 42})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"bin": 42}

@pytest.mark.asyncio
async def test_put_get_float(client):
    """Test putting and getting float values."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin": 3.14159})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"bin": 3.14159}

@pytest.mark.asyncio
async def test_put_get_string(client):
    """Test putting and getting string values."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin": "hello world"})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"bin": "hello world"}

@pytest.mark.asyncio
async def test_put_get_bool(client):
    """Test putting and getting boolean values."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bint": True, "binf": False})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"bint": True, "binf": False}

@pytest.mark.asyncio
async def test_get_specific_bins(client):
    """Test getting only specific bins."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John", "age": 30, "city": "NYC"})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bins(["name", "age"]).get()

    assert record is not None
    assert record.bins == {"name": "John", "age": 30}
    assert "city" not in record.bins

@pytest.mark.asyncio
async def test_delete(client):
    """Test delete operation."""
    # Put a record first
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John"})

    # Verify it exists
    exists = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).exists()
    assert exists is True

    # Delete it
    existed = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).delete()
    assert existed is True

    # Verify it no longer exists
    exists = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).exists()
    assert exists is False

@pytest.mark.asyncio
async def test_delete_nonexistent(client):
    """Test deleting a non-existent record."""
    existed = await client.key_value(
        namespace="test",
        set_name="test",
        key=999
    ).delete()
    assert existed is False

@pytest.mark.asyncio
async def test_exists(client):
    """Test exists operation."""
    # Record doesn't exist yet
    exists = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).exists()
    assert exists is False

    # Put a record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John"})

    # Now it exists
    exists = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).exists()
    assert exists is True

@pytest.mark.asyncio
async def test_add(client):
    """Test add (increment) operation."""
    # Put initial value
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"counter": 10})

    # Add to it
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).add({"counter": 5})

    # Verify result
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()
    assert record is not None
    assert record.bins == {"counter": 15}

@pytest.mark.asyncio
async def test_append(client):
    """Test append operation."""
    # Put initial string
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John"})

    # Append to it
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).append({"name": " Doe"})

    # Verify result
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()
    assert record is not None
    assert record.bins == {"name": "John Doe"}

@pytest.mark.asyncio
async def test_prepend(client):
    """Test prepend operation."""
    # Put initial string
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Doe"})

    # Prepend to it
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).prepend({"name": "John "})

    # Verify result
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()
    assert record is not None
    assert record.bins == {"name": "John Doe"}

@pytest.mark.asyncio
async def test_touch(client):
    """Test touch operation (update TTL without modifying data)."""
    # Put a record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John"})

    # Get initial TTL
    record1 = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()
    initial_ttl = record1.ttl

    # Touch the record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).touch()

    # Get record again - data should be unchanged, TTL may be updated
    record2 = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()
    assert record2 is not None
    assert record2.bins == {"name": "John"}

@pytest.mark.asyncio
async def test_get_nonexistent(client):
    """Test getting a non-existent record."""
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=999
    ).get()
    assert record is None

@pytest.mark.asyncio
async def test_string_key(client):
    """Test using string keys."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key="user123"
    ).put({"name": "John"})

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key="user123"
    ).get()

    assert record is not None
    assert record.bins == {"name": "John"}

@pytest.mark.asyncio
async def test_fluent_chaining(client):
    """Test that fluent method chaining works correctly."""
    # Chain configuration methods (bins, with_policy, etc.)
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"counter": 0, "name": "test"})

    # Chain bins() before get()
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bins(["counter"]).get()

    assert record is not None
    assert record.bins == {"counter": 0}
    assert "name" not in record.bins

@pytest.mark.asyncio
async def test_operate_put_and_get(client):
    """Test operate with Put and Get operations."""
    # Write initial record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin1": 7, "bin2": "string value"})

    # Use operate to put a new value and get the record
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([
        Operation.put("bin2", "new string"),
        Operation.get()
    ])

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("bin2") == "new string"
    assert record.bins.get("bin1") == 7

@pytest.mark.asyncio
async def test_operate_get_only(client):
    """Test operate with Get operation only."""
    # Write initial record
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin1": "value1", "bin2": 42})

    # Use operate to get the record
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([Operation.get()])

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("bin1") == "value1"
    assert record.bins.get("bin2") == 42

@pytest.mark.asyncio
async def test_operate_list_append(client):
    """Test operate with ListOperation.append."""
    # Create initial record with list
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"listbin": [1, 2, 3]})

    # Append to list and get size
    list_policy = ListPolicy(ListOrderType.ORDERED, None)
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([
        ListOperation.append("listbin", 4, list_policy),
        ListOperation.size("listbin")
    ])

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("listbin")
    # When multiple operations target the same bin, results are returned as a list
    if isinstance(size, list):
        size = size[-1]  # Get the last result (from the size operation)
    assert size == 4

@pytest.mark.asyncio
async def test_operate_map_put_and_get(client):
    """Test operate with MapOperation.put and get_by_key."""
    # Create initial record with map
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"mapbin": {"key1": "value1"}})

    # Put new key-value pair and get it back
    map_policy = MapPolicy(None, None)
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([
        MapOperation.put("mapbin", "key2", "value2", map_policy),
        MapOperation.get_by_key("mapbin", "key2", MapReturnType.VALUE)
    ])

    assert record is not None
    assert record.bins is not None
    value = record.bins.get("mapbin")
    # When multiple operations target the same bin, results are returned as a list
    if isinstance(value, list):
        value = value[-1]  # Get the last result (from the get_by_key operation)
    assert value == "value2"

@pytest.mark.asyncio
async def test_operate_map_clear(client):
    """Test operate with MapOperation.clear."""
    # Create initial record with map
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"mapbin": {"key1": "value1", "key2": "value2"}})

    # Clear the map and get size
    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([
        MapOperation.clear("mapbin"),
        MapOperation.size("mapbin")
    ])

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("mapbin")
    assert size == 0

@pytest.mark.asyncio
async def test_bin_chaining_set_to(client):
    """Test bin chaining API with set_to."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim").bin("age").set_to(1).bin("gender").set_to("male").execute()

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}

@pytest.mark.asyncio
async def test_bin_chaining_increment_by(client):
    """Test bin chaining API with increment_by."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"age": 30})

    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("age").increment_by(1).execute()

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins["age"] == 31

@pytest.mark.asyncio
async def test_bin_chaining_mixed_operations(client):
    """Test bin chaining with both set_to and increment_by."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim", "age": 1})

    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim Updated").bin("age").increment_by(1).execute()

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 2}

@pytest.mark.asyncio
async def test_and_remove_other_bins(client):
    """Test and_remove_other_bins functionality."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim", "age": 30, "gender": "male", "city": "NYC"})

    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim Updated").bin("age").set_to(26).and_remove_other_bins().execute()

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 26}

@pytest.mark.asyncio
async def test_set_bins_execute(client):
    """Test set_bins with execute method."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).set_bins({"name": "Tim", "age": 1, "gender": "male"}).execute()

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}

@pytest.mark.asyncio
async def test_durably_delete(client):
    """Test durably method for delete operations."""
    await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim"})

    result = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).durably(False).delete()

    assert result is True

    record = await client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is None

@pytest.mark.asyncio
async def test_insert_creates_new_record(client):
    """Test that insert() creates a new record successfully."""
    session = client.create_session()
    await session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25})

    record = await session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 25}

@pytest.mark.asyncio
async def test_insert_fails_if_record_exists(client):
    """Test that insert() fails if record already exists."""
    session = client.create_session()
    await session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Alice"})

    with pytest.raises(ServerError) as exc_info:
        await session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyExistsError" in str(exc_info.value) or "RecordExistsError" in str(exc_info.value) or "KeyAlreadyExistsError" in str(exc_info.value)

@pytest.mark.asyncio
async def test_update_succeeds_if_record_exists(client):
    """Test that update() succeeds if record exists."""
    session = client.create_session()
    await session.key_value(key=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25})
    await session.update(key_value=1, namespace="test", set_name="test").put({"age": 26})

    record = await session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 26}

@pytest.mark.asyncio
async def test_update_fails_if_record_not_exists(client):
    """Test that update() fails if record does not exist."""
    session = client.create_session()
    with pytest.raises(ServerError) as exc_info:
        await session.update(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyNotFoundError" in str(exc_info.value) or "RecordNotFoundError" in str(exc_info.value)

@pytest.mark.asyncio
async def test_replace_succeeds_if_record_exists(client):
    """Test that replace() succeeds if record exists and replaces all bins."""
    session = client.create_session()
    await session.key_value(key=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25, "city": "NYC"})
    await session.replace(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})

    record = await session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Bob"}
    assert "age" not in record.bins
    assert "city" not in record.bins

@pytest.mark.asyncio
async def test_replace_fails_if_record_not_exists(client):
    """Test that replace() fails if record does not exist."""
    session = client.create_session()
    with pytest.raises(ServerError) as exc_info:
        await session.replace(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyNotFoundError" in str(exc_info.value) or "RecordNotFoundError" in str(exc_info.value)
