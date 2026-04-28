# Aerospike Python SDK

A high-level API on the Aerospike Python Async Client, providing an intuitive and chainable interface for database operations.

> **Status:** Pre-alpha -- internal development.

## Prerequisites

- **Python** 3.10 - 3.14 (recommended: [pyenv](https://github.com/pyenv/pyenv) with a dedicated environment for this repo)
- **Aerospike server** -- required for integration tests
- **Rust toolchain** (rustc + cargo) -- only needed if building the Aerospike Python Async Client from source
- **Java 11+** -- required for the one-time AEL parser build (`make generate-ael`)

## Install the Aerospike Python Async Client

This SDK depends on the [Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async),
published on PyPI as [`aerospike-async`](https://pypi.org/project/aerospike-async/).
The version is pinned in `pyproject.toml` under `[project] dependencies`. A normal
`pip install -e ".[dev]"` resolves it from PyPI automatically -- no Rust toolchain
or git checkout required for ordinary development.

### Local PAC checkout (development against an unreleased PAC tree)

To test against a sibling PAC working tree (e.g. for a feature not yet on PyPI),
install it editable first and pass `--no-deps` to this SDK so pip doesn't try
to re-resolve PAC from PyPI:

```bash
pip install -e /path/to/aerospike-client-python-async
pip install -e ".[dev]" --no-deps
```

Or use `requirements-local.txt` (gitignored path example).

## Install this package

Use the interpreter from your pyenv environment (see `.cursor/rules/guiding-principles.mdc` for the usual env name), then:

```bash
make generate-ael          # one-time: build the ANTLR AEL parser (requires Java 11+)
pip install -e ".[dev]"
```

`make generate-ael` only needs to be re-run if `aerospike_sdk/ael/antlr4/Condition.g4` changes.

## Configuration

Copy `aerospike.env.example` to `aerospike.env` in the repo root and adjust hosts or ports. `aerospike.env` is not committed.

```bash
cp aerospike.env.example aerospike.env
source aerospike.env
```

Pytest loads `aerospike.env` when present; otherwise `conftest.py` loads `aerospike.env.example` for unset variables only (so CI env vars still win).

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

## Documentation

API docs are built with [Sphinx](https://www.sphinx-doc.org/) (Furo theme, MyST-Parser for Markdown).

```bash
pip install -e ".[docs]"   # one-time: install Sphinx toolchain

make docs                  # build static HTML to docs/_build/html/
make docs-serve            # live-reloading local preview
```

Docstrings use Google style with Sphinx cross-references (`:meth:`, `:class:`, etc.).

## Development

```bash
# Regenerate the ANTLR AEL parser (only needed if Condition.g4 changes)
make generate-ael

# Lint
ruff check .
```

## Usage

```python
import asyncio
from aerospike_sdk import Behavior, DataSet, Client

async def main():
    async with Client("localhost:3100") as client:
        session = client.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # High-level key-value operations
        await session.upsert(key=users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"})
        await session.upsert(key=users.id(2)).put({"name": "Bob", "age": 35, "country": "US"})

        # Query with string AEL -- stream results one at a time (memory-efficient)
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

## Versioning

PSDK follows [SemVer](https://semver.org/) for releases. Pre-releases use the
`MAJOR.MINOR.PATCH-{alpha,beta,rc}.N` form (e.g. `0.1.0-alpha.1`). PyPI
normalizes these on upload to the equivalent PEP 440 spelling (`0.1.0a1`).

### Single source of truth

The top-level `VERSION` file is the only place where the version lives:

```
0.9.0-alpha.1
```

`pyproject.toml` reads it dynamically:

```toml
[project]
dynamic = ["version"]

[tool.setuptools.dynamic]
version = {file = "VERSION"}
```

So the wheel and the working tree are guaranteed to match.

### Bumping the version

Bumps are manual and happen in PRs against `dev`. Promotion workflows
(`dev → stage → main`) do not mutate the version.

```bash
# 1. Edit VERSION:
#    e.g. 0.1.0-alpha.1  →  0.1.0-alpha.2
echo '0.1.0-alpha.2' > VERSION

# 2. Confirm:
bin/get-version    # prints 0.1.0-alpha.2

# 3. Open a PR against dev with just this change.
```

### Bumping the PAC pin

PSDK depends on a published release of the
[Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async)
on PyPI as `aerospike-async`. The pin lives in `pyproject.toml` under
`[project] dependencies`:

```toml
"aerospike-async==0.4.0a1",
```

To bump: change the version to the new release on PyPI, then reinstall:

```bash
pip install --upgrade "aerospike-async==0.4.0aN"
```

Open the PR against `dev`. PSDK's own `VERSION` does not need to change for
a PAC pin bump unless the underlying API contract has shifted enough to
warrant it.

### Reading the version programmatically

Anywhere a build script, CI step, or release tool needs the version:

```bash
bin/get-version    # → 0.9.0-alpha.1
```

The script reads `VERSION` and trims trailing whitespace. No Python or
setuptools runtime dependency — usable from any shell, container, or CI
environment.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
