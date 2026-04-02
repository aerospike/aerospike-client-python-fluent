# Logging

The fluent client produces logs from three layers, each identified by its
Python logger name:

| Logger name | Layer | Description |
|-------------|-------|-------------|
| `aerospike_core` | Rust core | Cluster management, connection pooling, wire protocol |
| `aerospike_async` | Python Async Client (PAC) | Client init, connection lifecycle |
| `aerospike_fluent` | Python Fluent Client (PFC) | Session operations, DSL parsing, index monitoring |

All three layers use Python's standard `logging` module, so any handler or
formatter you configure will receive messages from all layers.

## Quick Start

The simplest way to enable logging is via environment variables:

```bash
export AEROSPIKE_LOG_LEVEL=DEBUG
export AEROSPIKE_LOG_FILE=/tmp/aerospike.log   # optional; omit for stderr
```

The test suite and example scripts read these variables automatically (see
`conftest.py` and `examples/_env.py`).

## Programmatic Configuration

For full control, configure the loggers directly:

```python
import logging

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
))

for name in ("aerospike_core", "aerospike_async", "aerospike_fluent"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
```

You can set different levels per layer — for example, `WARNING` for the core
and `DEBUG` for the fluent client:

```python
logging.getLogger("aerospike_core").setLevel(logging.WARNING)
logging.getLogger("aerospike_fluent").setLevel(logging.DEBUG)
```

## Log Output

A typical log line looks like:

```
2026-03-30 14:22:01,234 INFO     aerospike_async: connecting to 127.0.0.1:3100
2026-03-30 14:22:01,289 INFO     aerospike_async: connected to 127.0.0.1:3100
2026-03-30 14:22:01,290 DEBUG    aerospike_core.cluster: Tending cluster...
```

The `%(name)s` field identifies which layer produced the message, making it
straightforward to filter or route logs.

## File Logging

To write logs to a file instead of stderr:

```python
handler = logging.FileHandler("/tmp/aerospike.log")
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
))

for name in ("aerospike_core", "aerospike_async", "aerospike_fluent"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
```

Or via environment variables in `aerospike.env`:

```bash
export AEROSPIKE_LOG_LEVEL=DEBUG
export AEROSPIKE_LOG_FILE=/tmp/aerospike.log
```

## Recommended Levels

| Level | Use case |
|-------|----------|
| `WARNING` (default) | Production — only unexpected conditions |
| `INFO` | Connection lifecycle, index creation events |
| `DEBUG` | Full detail including cluster tend cycles, expression parsing |
