"""
Pytest configuration to automatically load environment variables from aerospike.env
"""
import os
import pytest
from pathlib import Path

from aerospike_async import ClientPolicy


def load_env_file(env_file_path):
    """Load environment variables from a .env file"""
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
                os.environ[key] = value


def pytest_configure(config):
    """Called after command line options have been parsed and all plugins and initial conftest files been loaded."""
    # Load environment variables from aerospike.env (in project root)
    env_file = Path(__file__).parent / "aerospike.env"
    load_env_file(env_file)
    
    # Print loaded environment variables for debugging
    print(f"Loaded environment variables from {env_file}\n")
    
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


@pytest.fixture(scope="session") 
def aerospike_host_tls():
    """Fixture providing the TLS-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_TLS', 'localhost:3107')


@pytest.fixture(scope="session")
def aerospike_host_sec():
    """Fixture providing the security-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_SEC', 'localhost:3109')

