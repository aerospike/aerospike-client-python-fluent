# Aerospike Python SDK

A modern, developer-friendly interface for Aerospike. Async-first, Pythonic API
with a chainable session model, fluent query builder, and AEL string filters
over the [Aerospike Python Async Client](https://pypi.org/project/aerospike-async/).

> **Status:** Public preview (alpha). Not yet production-ready; feedback welcome
> via [GitHub Issues](https://github.com/aerospike/aerospike-client-python-sdk/issues).

## Resources

- **PyPI:** https://pypi.org/project/aerospike-sdk/
- **Documentation:** https://aerospike.com/docs/develop/client/sdk/
- **API Reference:** https://aerospike-python-sdk.readthedocs.io/
- **Source:** https://github.com/aerospike/aerospike-client-python-sdk
- **Issues:** https://github.com/aerospike/aerospike-client-python-sdk/issues

## Installation

```bash
pip install aerospike-sdk
```

Pin to a specific release if you need reproducible builds:

```bash
pip install aerospike-sdk==0.9.0a1
```

This installs the SDK plus its dependency on the Aerospike Python Async Client
(`aerospike-async`). No Rust toolchain or git checkout required for ordinary
use — pre-built wheels are available for Linux, macOS, and Windows on Python
3.10–3.14.

## Quick start

```python
import asyncio
from aerospike_sdk import Behavior, Client, DataSet


async def main():
    async with Client("localhost:3000") as client:
        session = client.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # High-level key-value writes
        await session.upsert(users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"}).execute()
        await session.upsert(users.id(2)).put({"name": "Bob", "age": 35, "country": "US"}).execute()

        # Filtered query with AEL — streams results memory-efficiently
        results = await (
            session.query(users)
            .where("$.age > %s and $.country == '%s'", 25, "US")
            .execute()
        )
        async for row in results:
            if row.is_ok and row.record is not None:
                print(row.record.bins)

        # Or drain the entire stream into a list
        all_users = await session.query(users).execute()
        rows = await all_users.collect()


asyncio.run(main())
```

See the [Quick Start guide](https://aerospike.com/docs/develop/client/sdk/) for
a deeper walkthrough; the [API reference](https://aerospike-python-sdk.readthedocs.io/)
covers every public class and method in detail.

## Documentation

- **Guides and tutorials** (user-facing, hand-curated):
  https://aerospike.com/docs/develop/client/sdk/
- **API reference** (auto-generated from docstrings, hosted on Read the Docs):
  https://aerospike-python-sdk.readthedocs.io/

The two complement each other: the guide site introduces concepts and works
through realistic examples, while the API reference is the exhaustive source
for `Client`, `Session`, query/update builders, AEL, behavior policies, and
every public symbol.

## Versioning

PSDK follows [SemVer](https://semver.org/). Pre-releases use the
`MAJOR.MINOR.PATCH-{alpha,beta,rc}.N` form (e.g. `0.9.0-alpha.1`). PyPI
normalizes these on upload to the equivalent PEP 440 spelling (`0.9.0a1`).

The top-level `VERSION` file is the single source of truth; `pyproject.toml`
reads it dynamically, so the wheel and the working tree are guaranteed to
match. See the [Development](#development--contributing) section below for the
bump procedure.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Development / Contributing

The sections below are for SDK *contributors*. Downstream users do **not** need
any of this — `pip install aerospike-sdk` is sufficient to use the package.

### Prerequisites

- **Python** 3.10 - 3.14 (recommended: [pyenv](https://github.com/pyenv/pyenv) with a dedicated environment)
- **Aerospike server** — required for integration tests
- **Rust toolchain** (`rustc` + `cargo`) — only needed if building the Aerospike Python Async Client from source
- **Java 11+** — required for the one-time AEL parser build (`make generate-ael`)

### Setting up a dev environment

```bash
make generate-ael          # one-time: build the ANTLR AEL parser (requires Java 11+)
pip install -e ".[dev]"    # install with dev extras
```

`make generate-ael` only needs to be re-run if `aerospike_sdk/ael/antlr4/Condition.g4` changes.

### Local PAC checkout (development against an unreleased PAC tree)

To test against a sibling Aerospike Python Async Client working tree (e.g. for
a feature not yet on PyPI), install it editable first and pass `--no-deps` to
this SDK so pip doesn't try to re-resolve PAC from PyPI:

```bash
pip install -e /path/to/aerospike-client-python-async
pip install -e ".[dev]" --no-deps
```

Or use `requirements-local.txt` (gitignored path example).

### Configuration

Copy `aerospike.env.example` to `aerospike.env` in the repo root and adjust
hosts or ports. `aerospike.env` is not committed.

```bash
cp aerospike.env.example aerospike.env
source aerospike.env
```

Pytest loads `aerospike.env` when present; otherwise `conftest.py` loads
`aerospike.env.example` for unset variables only (so CI env vars still win).

### Running tests

```bash
make test          # all tests
make test-unit     # unit tests only
make test-int      # integration tests only (requires running Aerospike server)
```

**macOS file descriptor limit.** On macOS, you may encounter `OSError: [Errno
24] Too many open files` when running the full test suite. The default limit
(256) is not enough for the concurrent async connections created during
testing.

```bash
ulimit -n 4096
```

To make this permanent, add it to your shell profile (`~/.zshrc` or
`~/.bash_profile`).

### Building docs locally

API docs are built with [Sphinx](https://www.sphinx-doc.org/) (Furo theme,
MyST-Parser for Markdown). The same Sphinx config is what
[Read the Docs](https://aerospike-python-sdk.readthedocs.io/) builds from.

```bash
pip install -e ".[docs]"   # one-time: install Sphinx toolchain
make docs                  # build static HTML to docs/_build/html/
make docs-serve            # live-reloading local preview
```

Docstrings use Google style with Sphinx cross-references (`:meth:`, `:class:`,
etc.).

### Lint

```bash
ruff check .
```

### Bumping the version

Bumps are manual and happen in PRs against `dev`. Promotion workflows
(`dev → stage → main`) do not mutate the version.

```bash
# 1. Edit VERSION:
#    e.g. 0.9.0-alpha.1  →  0.9.0-alpha.2
echo '0.9.0-alpha.2' > VERSION

# 2. Confirm:
bin/get-version    # prints 0.9.0-alpha.2

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

Open the PR against `dev`. PSDK's own `VERSION` does not need to change for a
PAC pin bump unless the underlying API contract has shifted enough to warrant
it.

### Reading the version programmatically

Anywhere a build script, CI step, or release tool needs the version:

```bash
bin/get-version    # → 0.9.0-alpha.1
```

The script reads `VERSION` and trims trailing whitespace. No Python or
setuptools runtime dependency — usable from any shell, container, or CI
environment.
