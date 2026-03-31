# Connecting to a Cluster

## Basic Connection

The simplest way to connect is with a host and port:

=== "Async"

    ```python
    from aerospike_fluent import FluentClient

    async with FluentClient("localhost", 3000) as client:
        session = client.create_session()
        # ... use session ...
    ```

=== "Sync"

    ```python
    from aerospike_fluent import SyncFluentClient

    with SyncFluentClient("localhost", 3000) as client:
        session = client.create_session()
        # ... use session ...
    ```

Both clients support context managers that automatically close the connection on exit.

## ClusterDefinition

For advanced configuration, use `ClusterDefinition`:

```python
from aerospike_fluent import ClusterDefinition

cluster_def = (
    ClusterDefinition("localhost", 3000)
    .with_native_credentials("username", "password")
    .using_services_alternate()
    .with_ip_map({"10.0.0.1": "3.72.54.187"})
)

async with cluster_def.connect() as client:
    # ...
```

### TLS

```python
cluster = (
    ClusterDefinition("localhost", 3100)
    .with_tls_config_of()
        .tls_name("myTlsName")
        .ca_file("/path/to/ca.pem")
    .done()
    .with_native_credentials("username", "password")
    .connect()
)
```

### Rack Awareness

```python
cluster = (
    ClusterDefinition("localhost", 3000)
    .preferring_racks(1, 2)
    .connect()
)
```

## Sessions

A **Session** binds a connected client to a set of policy defaults via a
[`Behavior`](../api/behavior.md). All reads and writes go through a session.

```python
from aerospike_fluent import Behavior

session = client.create_session(Behavior.DEFAULT)
fast_session = client.create_session(Behavior.READ_FAST)
consistent_session = client.create_session(Behavior.STRICTLY_CONSISTENT)
```

!!! tip
    Create different sessions for different workloads. A "fast read" session with
    short timeouts and a "batch import" session with longer timeouts can coexist
    on the same client.

## Behaviors

Predefined behaviors:

| Behavior | Description |
|----------|-------------|
| `Behavior.DEFAULT` | Balanced defaults |
| `Behavior.READ_FAST` | Low-latency reads |
| `Behavior.STRICTLY_CONSISTENT` | Strong consistency mode |
| `Behavior.FAST_RACK_AWARE` | Prefer local rack for reads |

Custom behaviors via derivation:

```python
my_behavior = Behavior.DEFAULT.derive_with_changes(
    total_timeout_ms=5000,
    max_retries=3,
)
```

## DataSets

A `DataSet` represents a namespace + set pair and is a convenient key factory:

```python
from aerospike_fluent import DataSet

users = DataSet.of("test", "users")

key = users.id(42)                       # single key
keys = users.ids(1, 2, 3)               # list of keys
digest_key = users.id_from_digest(b"...")  # key from raw digest
```

Pass datasets to session methods to avoid repeating namespace/set strings:

```python
stream = await session.query(users).execute()
await session.upsert(users.id(1)).put({"name": "Alice"}).execute()
```
