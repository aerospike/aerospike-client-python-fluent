"""Tests for SyncFluentClient KeyValue operations."""

import pytest
from aerospike_async import ListOperation, ListPolicy, ListOrderType, MapOperation, MapPolicy, MapOrder, MapReturnType, Operation, ServerError
from aerospike_fluent import DataSet, SyncFluentClient


@pytest.fixture
def client(aerospike_host, client_policy):
    """Setup sync fluent client for testing."""
    with SyncFluentClient(seeds=aerospike_host, policy=client_policy) as client:
        # Clean up test record before each test
        client.key_value(
            namespace="test",
            set_name="test",
            key=1
        ).delete()
        yield client

def test_put_get_basic(client):
    """Test basic put and get operations."""
    # Put a record
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John", "age": 30})

def test_put_get_with_dataset(client):
    """Test put and get using DataSet."""
    users = DataSet.of("test", "test")
    key = users.id(2)

def test_put_get_with_key_object(client):
    """Test put and get using Key object."""
    users = DataSet.of("test", "test")
    key = users.id(3)

def test_exists(client):
    """Test exists operation."""
    # Record doesn't exist yet
    exists = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).exists()
    assert not exists

def test_delete(client):
    """Test delete operation."""
    # Put a record
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John"})

def test_get_with_bins(client):
    """Test get with specific bin selection."""
    # Put a record with multiple bins
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "John", "age": 30, "city": "NYC"})

    # Get only specific bins
    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bins(["name", "age"]).get()

    assert record is not None
    assert "name" in record.bins
    assert "age" in record.bins
    assert "city" not in record.bins


def test_truncate(client):
    """Test client.truncate() deletes all records in a set."""
    users = DataSet.of("test", "users")

    # Create some records
    key1 = users.id("user1")
    key2 = users.id("user2")
    key3 = users.id("user3")

    client.key_value(key=key1).put({"name": "User1"})
    client.key_value(key=key2).put({"name": "User2"})
    client.key_value(key=key3).put({"name": "User3"})

    # Verify records exist
    assert client.key_value(key=key1).exists()
    assert client.key_value(key=key2).exists()
    assert client.key_value(key=key3).exists()

    # Truncate the set
    client.truncate(users)

    # Verify all records are deleted
    assert not client.key_value(key=key1).exists()
    assert not client.key_value(key=key2).exists()
    assert not client.key_value(key=key3).exists()

def test_operate_put_and_get(client):
    """Test operate with Put and Get operations."""
    # Write initial record
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin1": 7, "bin2": "string value"})

    # Use operate to put a new value and get the record
    record = client.key_value(
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

def test_operate_get_only(client):
    """Test operate with Get operation only."""
    # Write initial record
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"bin1": "value1", "bin2": 42})

    # Use operate to get the record
    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).operate([Operation.get()])

    # Verify the record was returned
    assert record is not None
    assert record.bins is not None
    assert record.bins.get("bin1") == "value1"
    assert record.bins.get("bin2") == 42

def test_operate_list_append(client):
    """Test operate with ListOperation.append."""
    # Create initial record with list
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"listbin": [1, 2, 3]})

    # Append to list and get size
    list_policy = ListPolicy(ListOrderType.ORDERED, None)
    record = client.key_value(
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

def test_operate_map_put_and_get(client):
    """Test operate with MapOperation.put and get_by_key."""
    # Create initial record with map
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"mapbin": {"key1": "value1"}})

    # Put new key-value pair and get it back
    map_policy = MapPolicy(None, None)
    record = client.key_value(
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

def test_operate_map_clear(client):
    """Test operate with MapOperation.clear."""
    # Create initial record with map
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"mapbin": {"key1": "value1", "key2": "value2"}})

    # Clear the map and get size
    record = client.key_value(
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

def test_bin_chaining_set_to(client):
    """Test bin chaining API with set_to."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim").bin("age").set_to(1).bin("gender").set_to("male").execute()

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}

def test_bin_chaining_increment_by(client):
    """Test bin chaining API with increment_by."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"age": 30})

    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("age").increment_by(1).execute()

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins["age"] == 31

def test_bin_chaining_mixed_operations(client):
    """Test bin chaining with both set_to and increment_by."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim", "age": 1})

    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim Updated").bin("age").increment_by(1).execute()

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 2}

def test_and_remove_other_bins(client):
    """Test and_remove_other_bins functionality."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim", "age": 30, "gender": "male", "city": "NYC"})

    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).bin("name").set_to("Tim Updated").bin("age").set_to(26).and_remove_other_bins().execute()

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 26}

def test_set_bins_execute(client):
    """Test set_bins with execute method."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).set_bins({"name": "Tim", "age": 1, "gender": "male"}).execute()

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}

def test_durably_delete(client):
    """Test durably method for delete operations."""
    client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).put({"name": "Tim"})

    result = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).durably(False).delete()

    assert result is True

    record = client.key_value(
        namespace="test",
        set_name="test",
        key=1
    ).get()

    assert record is None

def test_insert_creates_new_record(client):
    """Test that insert() creates a new record successfully."""
    session = client.create_session()
    session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25})

    record = session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 25}

def test_insert_fails_if_record_exists(client):
    """Test that insert() fails if record already exists."""
    session = client.create_session()
    session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Alice"})

    with pytest.raises(ServerError) as exc_info:
        session.insert(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyExistsError" in str(exc_info.value) or "RecordExistsError" in str(exc_info.value) or "KeyAlreadyExistsError" in str(exc_info.value)

def test_update_succeeds_if_record_exists(client):
    """Test that update() succeeds if record exists."""
    session = client.create_session()
    session.key_value(key_value=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25})
    session.update(key_value=1, namespace="test", set_name="test").put({"age": 26})

    record = session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 26}

def test_update_fails_if_record_not_exists(client):
    """Test that update() fails if record does not exist."""
    session = client.create_session()
    with pytest.raises(ServerError) as exc_info:
        session.update(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyNotFoundError" in str(exc_info.value) or "RecordNotFoundError" in str(exc_info.value)

def test_replace_succeeds_if_record_exists(client):
    """Test that replace() succeeds if record exists and replaces all bins."""
    session = client.create_session()
    session.key_value(key_value=1, namespace="test", set_name="test").put({"name": "Alice", "age": 25, "city": "NYC"})
    session.replace(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})

    record = session.key_value(key=1, namespace="test", set_name="test").get()
    assert record is not None
    assert record.bins == {"name": "Bob"}
    assert "age" not in record.bins
    assert "city" not in record.bins

def test_replace_fails_if_record_not_exists(client):
    """Test that replace() fails if record does not exist."""
    session = client.create_session()
    with pytest.raises(ServerError) as exc_info:
        session.replace(key_value=1, namespace="test", set_name="test").put({"name": "Bob"})
    assert "KeyNotFoundError" in str(exc_info.value) or "RecordNotFoundError" in str(exc_info.value)
