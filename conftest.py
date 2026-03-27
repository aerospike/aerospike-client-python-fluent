"""
Pytest configuration to load environment variables from aerospike.env.

If aerospike.env exists, only that file is read (override=True); aerospike.env.example
is not merged. If aerospike.env is missing, aerospike.env.example supplies defaults
for variables not already in os.environ (override=False).
"""
import os
import pytest
import pytest_asyncio
from pathlib import Path

from aerospike_async import ClientPolicy, new_client


def load_env_file(env_file_path, *, override: bool = True) -> None:
    """Load KEY=value / export KEY=value lines from a file into os.environ."""
    if not os.path.exists(env_file_path):
        return

    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue

            # Parse export VAR=value format
            if line.startswith('export '):
                line = line[7:]  # Remove 'export ' prefix

            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if override or key not in os.environ:
                    os.environ[key] = value


def pytest_configure(config):
    """Called after command line options have been parsed and all plugins and initial conftest files been loaded."""
    root = Path(__file__).parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        load_env_file(env_local, override=True)
        print(f"Loaded environment variables from {env_local}\n")
    else:
        # Defaults only for unset keys so CI and explicit exports keep precedence.
        load_env_file(env_example, override=False)
        print(f"Loaded default environment variables from {env_example} (no {env_local.name})\n")
    
    # Ensure python path includes the tests directory for imports
    import sys
    tests_dir = Path(__file__).parent / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))


def _use_services_alternate_from_env() -> bool:
    v = os.environ.get('AEROSPIKE_USE_SERVICES_ALTERNATE', 'true').strip().lower()
    return v in ('true', '1', 'yes')


@pytest.fixture(scope="session")
def client_policy():
    """Fixture providing ClientPolicy with use_services_alternate from AEROSPIKE_USE_SERVICES_ALTERNATE."""
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()
    return policy


@pytest.fixture(scope="session")
def aerospike_host():
    """Fixture providing the Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST', 'localhost:3000')


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def enterprise(aerospike_host, client_policy):
    """True when the test cluster is Enterprise Edition (queried via info)."""
    client = await new_client(client_policy, aerospike_host)
    try:
        result = await client.info("edition")
        return any("Enterprise" in v for v in result.values())
    finally:
        await client.close()


@pytest.fixture(scope="session") 
def aerospike_host_tls():
    """Fixture providing the TLS-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_TLS', 'localhost:3107')


@pytest.fixture(scope="session")
def aerospike_host_sec():
    """Fixture providing the security-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_SEC', 'localhost:3109')

