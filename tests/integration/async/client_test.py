"""Tests for FluentClient."""

import pytest
from aerospike_fluent import FluentClient


@pytest.mark.asyncio
async def test_client_connection(aerospike_host, client_policy):
    """Test that we can connect to Aerospike using the fluent client."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        assert client.is_connected
        # Test that we can create a key-value operation builder
        kv_op = client.key_value(
            namespace="test",
            set_name="test",
            key="test_key"
        )
        assert kv_op is not None

@pytest.mark.asyncio
async def test_client_context_manager(aerospike_host, client_policy):
    """Test that the context manager properly manages connection lifecycle."""
    client = FluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

    async with client:
        assert client.is_connected

    # After exiting context, connection should be closed
    assert not client.is_connected

@pytest.mark.asyncio
async def test_client_manual_connect_close(aerospike_host, client_policy):
    """Test manual connect and close methods."""
    client = FluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

    await client.connect()
    assert client.is_connected

    await client.close()
    assert not client.is_connected
