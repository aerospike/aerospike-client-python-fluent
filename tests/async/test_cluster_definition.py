"""Tests for ClusterDefinition and Cluster."""

import pytest
import pytest_asyncio

from aerospike_fluent import ClusterDefinition, Host, Behavior


@pytest_asyncio.fixture
async def cluster(aerospike_host):
    """Setup cluster for testing."""
    # Parse host:port from aerospike_host
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


@pytest.mark.asyncio
async def test_cluster_definition_basic_connection(cluster):
    """Test basic ClusterDefinition connection."""
    assert cluster.is_connected()

    # Create a session
    session = cluster.create_session(Behavior.DEFAULT)
    assert session is not None
    assert session.behavior.name == "DEFAULT"


@pytest.mark.asyncio
async def test_cluster_definition_with_hosts(aerospike_host):
    """Test ClusterDefinition with Host objects."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    hosts = [Host(hostname, port)]
    cluster_def = ClusterDefinition(hosts=hosts)
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
        session = cluster.create_session()
        assert session is not None
    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_cluster_definition_with_credentials(aerospike_host):
    """Test ClusterDefinition with credentials (if auth is enabled)."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    # Test with empty credentials (should work if no auth)
    cluster_def = ClusterDefinition(hostname, port).with_native_credentials("", "")
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_cluster_definition_services_alternate(aerospike_host):
    """Test ClusterDefinition with services alternate."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = ClusterDefinition(hostname, port).using_services_alternate()
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_cluster_definition_preferring_racks(aerospike_host):
    """Test ClusterDefinition with preferred racks."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = ClusterDefinition(hostname, port).preferring_racks(1, 2)
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_cluster_definition_context_manager(aerospike_host):
    """Test ClusterDefinition with async context manager."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = ClusterDefinition(hostname, port)
    async with await cluster_def.connect() as cluster:
        assert cluster.is_connected()
        session = cluster.create_session()
        assert session is not None


@pytest.mark.asyncio
async def test_cluster_create_session(cluster):
    """Test creating sessions from cluster."""
    # Create session with default behavior
    session1 = cluster.create_session()
    assert session1 is not None

    # Create session with explicit behavior
    session2 = cluster.create_session(Behavior.DEFAULT)
    assert session2 is not None

    # Create session with custom behavior
    custom_behavior = Behavior.DEFAULT.derive_with_changes(
        name="test",
        max_retries=3
    )
    session3 = cluster.create_session(custom_behavior)
    assert session3 is not None
    assert session3.behavior.name == "test"


@pytest.mark.asyncio
async def test_cluster_create_transactional_session(cluster):
    """Test creating transactional session from cluster."""
    tx_session = cluster.create_transactional_session()
    assert tx_session is not None


@pytest.mark.asyncio
async def test_host_parse_hosts():
    """Test Host.parse_hosts() method."""
    hosts = Host.parse_hosts("host1:3000,host2:3001", 3000)
    assert len(hosts) == 2
    assert hosts[0].name == "host1"
    assert hosts[0].port == 3000
    assert hosts[1].name == "host2"
    assert hosts[1].port == 3001

    # Test with default port
    hosts2 = Host.parse_hosts("host1,host2", 3000)
    assert len(hosts2) == 2
    assert hosts2[0].port == 3000
    assert hosts2[1].port == 3000


@pytest.mark.asyncio
async def test_host_of():
    """Test Host.of() static method."""
    host = Host.of("localhost", 3000)
    assert host.name == "localhost"
    assert host.port == 3000


