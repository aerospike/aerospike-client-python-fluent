# Aerospike Fluent Client for Python

A fluent API wrapper for the Aerospike async Python client, providing a more intuitive and chainable interface for database operations.

> **Status:** Pre-alpha -- internal development.

## Prerequisites

- **Python** 3.10 - 3.14
- **Aerospike server** -- required for integration tests
- **Rust toolchain** (rustc + cargo) -- only needed if building the async client from source
- **Java 11+** -- only needed if regenerating the ANTLR DSL parser

## Install the Python Async Client

The fluent client depends on the [Aerospike async Python client](https://github.com/aerospike/aerospike-client-python-async).

### Option 1: Pre-built wheel (recommended -- no Rust needed)

Download the wheel for your platform and Python version from the
[GitHub Releases page](https://github.com/aerospike/aerospike-client-python-async/releases),
then install it:

```bash
pip install aerospike_async-0.3.0a2-cp313-cp313-macosx_11_0_arm64.whl  # example
```

### Option 2: Build from source (requires Rust)

```bash
git clone git@github.com:aerospike/aerospike-client-python-async.git
cd aerospike-client-python-async
git checkout v0.3.0-alpha.2
pip install -r requirements.txt
make dev
```

See the [async client README](https://github.com/aerospike/aerospike-client-python-async/blob/rust-async/README.md) for detailed Rust setup instructions.

## Install the Fluent Client

```bash
pip install -e ".[dev]"
```

## Configuration

Edit `aerospike.env` to match your Aerospike server:

```bash
export AEROSPIKE_HOST=127.0.0.1:3100
```

For manual runs (outside of pytest), source it:

```bash
source aerospike.env
```

Tests automatically load `aerospike.env` via `conftest.py`.

## Running Tests

```bash
make test          # all tests
make test-unit     # unit tests only
make test-int      # integration tests only (requires running Aerospike server)
```

### macOS File Descriptor Limit

On macOS, you may encounter `OSError: [Errno 24] Too many open files` when running the full test suite. The default limit (256) is not enough for the concurrent async connections created during testing.

```bash
ulimit -n 4096
```

To make this permanent, add it to your shell profile (`~/.zshrc` or `~/.bash_profile`).

## Development

```bash
# Regenerate the ANTLR DSL parser (requires Java 11+)
make generate-dsl

# Lint
ruff check .
```

## Usage

```python
import asyncio
from aerospike_fluent import Behavior, DataSet, FluentClient

async def main():
    async with FluentClient("localhost:3100") as client:
        session = client.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # Fluent key-value operations
        await session.upsert(key=users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"})
        await session.upsert(key=users.id(2)).put({"name": "Bob", "age": 35, "country": "US"})

        # Query with string DSL -- stream results one at a time (memory-efficient)
        results = await (
            session.query(users)
            .where("$.age > %s and $.country == '%s'", 25, "US")
            .execute()
        )
        async for rec in results:
            print(rec.bins)

        # execute() returns a lazy async stream
        all_users = await session.query(users).execute()
        # collect() drains the stream into a list
        user_list = await all_users.collect()

asyncio.run(main())
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
