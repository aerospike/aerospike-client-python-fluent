"""Tests for SyncFluentClient."""

from aerospike_fluent import SyncFluentClient


def test_client_connection(aerospike_host, client_policy):
    """Test that we can connect to Aerospike using the sync fluent client."""
    with SyncFluentClient(seeds=aerospike_host, policy=client_policy) as client:
        assert client.is_connected
        # Test that we can create a key-value operation builder
        kv_op = client.key_value(
            namespace="test",
            set_name="test",
            key="test_key"
        )
        assert kv_op is not None

def test_client_context_manager(aerospike_host, client_policy):
    """Test that the context manager properly manages connection lifecycle."""
    client = SyncFluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

def test_client_manual_connect_close(aerospike_host, client_policy):
    """Test manual connect and close methods."""
    client = SyncFluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

    client.connect()
    assert client.is_connected

    client.close()
    assert not client.is_connected
