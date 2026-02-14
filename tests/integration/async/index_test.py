"""Tests for IndexBuilder fluent API."""

import os
import pytest
import pytest_asyncio
from aerospike_async import CollectionIndexType
from aerospike_fluent import FluentClient
from aerospike_fluent.exceptions import AerospikeError


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client for index tests."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest.mark.asyncio
async def test_client_policy_use_services_alternate_from_env(client_policy, aerospike_host):
    """Verify AEROSPIKE_USE_SERVICES_ALTERNATE is loaded and applied to client_policy."""
    assert client_policy.use_services_alternate is True
    env_val = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower()
    assert env_val in ("true", "1", "yes", ""), f"unexpected AEROSPIKE_USE_SERVICES_ALTERNATE={env_val!r}"
    assert aerospike_host, "AEROSPIKE_HOST should be set (e.g. 127.0.0.1:3100)"


@pytest.mark.asyncio
async def test_create_numeric_index(client):
    """Test creating a numeric index."""
    index_name = "test_numeric_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create numeric index
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

@pytest.mark.asyncio
async def test_create_string_index(client):
    """Test creating a string index."""
    index_name = "test_string_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create string index
    await client.index("test", "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

@pytest.mark.asyncio
async def test_create_index_with_collection_type(client):
    """Test creating an index with collection index type."""
    index_name = "test_collection_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create index with collection type
    await (
        client.index("test", "test")
        .on_bin("roles")
        .named(index_name)
        .string()
        .collection(CollectionIndexType.LIST)
        .create()
    )

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

@pytest.mark.asyncio
async def test_drop_index(client):
    """Test dropping an index."""
    index_name = "test_drop_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create index first
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Drop the index
    await client.index("test", "test").named(index_name).drop()

@pytest.mark.asyncio
async def test_drop_nonexistent_index(client):
    """Test dropping a non-existent index (should not raise error)."""
    # Dropping non-existent index should not raise error
    await client.index("test", "test").named("non_existent_idx").drop()

@pytest.mark.asyncio
async def test_index_fluent_chaining(client):
    """Test fluent method chaining on index builder."""
    index_name = "test_chain_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Test chaining
    await (
        client.index("test", "test")
        .on_bin("age")
        .named(index_name)
        .numeric()
        .create()
    )

    # Verify we can chain drop too
    await client.index("test", "test").named(index_name).drop()

@pytest.mark.asyncio
async def test_create_index_missing_bin_name(client):
    """Test that creating index without bin name raises error."""
    with pytest.raises(ValueError, match="bin_name"):
        await client.index("test", "test").named("test_idx").numeric().create()

@pytest.mark.asyncio
async def test_create_index_missing_index_name(client):
    """Test that creating index without index name raises error."""
    with pytest.raises(ValueError, match="index_name"):
        await client.index("test", "test").on_bin("age").numeric().create()

@pytest.mark.asyncio
async def test_create_index_missing_index_type(client):
    """Test that creating index without index type raises error."""
    with pytest.raises(ValueError, match="index_type"):
        await client.index("test", "test").on_bin("age").named("test_idx").create()

@pytest.mark.asyncio
async def test_create_duplicate_index_fails(client):
    """Test that creating duplicate index names fails."""
    index_name = "test_duplicate_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create first index
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Try to create another index with same name should fail
    with pytest.raises(AerospikeError):
        await client.index("test", "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass
