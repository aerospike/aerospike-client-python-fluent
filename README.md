# Aerospike Fluent Client for Python

A fluent API wrapper for the Aerospike async Python client, providing a more intuitive and chainable interface for database operations.

## Requirements

- Python 3.10, 3.11, 3.12, 3.13, or 3.14
- Aerospike async Python client (installed as a local dependency)

## Setup

### Configure Aerospike Connection

Edit the `aerospike.env` file to match your Aerospike database node configuration:
```bash
export AEROSPIKE_HOST=localhost:3101
```

For manual runs (outside of pytest), source the environment file:
```bash
source aerospike.env
```

Note: Tests automatically load `aerospike.env` via `conftest.py`.

## Installation

### Option 1: Install from source (recommended for development)

```bash
# Install the fluent client and its dependencies
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

### Option 2: Install using requirements.txt

```bash
# Install runtime dependencies
pip install -r requirements.txt

# Or install development dependencies
pip install -r requirements-dev.txt
```

**Note:** The `aerospike-client-python-async` dependency is configured to use the remote git repository via SSH (`git@github.com:aerospike/aerospike-client-python-async.git`) on the `rust-async` branch. This is automatically installed when you run `pip install -e .`. To use a different branch, tag, or commit, update the dependency in `pyproject.toml` (e.g., `@main`, `@v0.2.0`, or `@<commit-hash>`).

## Development

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint code
ruff check .
```

### macOS File Descriptor Limit

On macOS, you may encounter `OSError: [Errno 24] Too many open files` errors when running the full test suite. This occurs because:

- **Default macOS limit**: The default file descriptor limit on macOS is 256 (`ulimit -n` shows the current limit)
- **Why it's not enough**: The fluent client creates multiple connections, event loops, and file handles during testing. Running the full test suite can easily exceed 256 open file descriptors, especially with async operations that maintain multiple concurrent connections.

**Solution**: Increase the file descriptor limit before running tests:

```bash
# Check current limit
ulimit -n

# Increase limit (recommended: 4096)
ulimit -n 4096

# Verify the new limit
ulimit -n

# Now run tests
pytest
```

**Note**: This change is temporary for the current shell session. To make it permanent, add `ulimit -n 4096` to your shell profile (e.g., `~/.zshrc` or `~/.bash_profile`).

## Usage

```python
import asyncio
import os
from aerospike_fluent import FluentClient

async def main():
    # Get host from environment (set in aerospike.env)
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    
    # Use context manager for automatic connection management
    async with FluentClient(seeds=host) as client:
        # Fluent API operations
        record = await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).get()
        
        print(record.bins if record else "Record not found")

asyncio.run(main())
```

## License

[To be determined]

