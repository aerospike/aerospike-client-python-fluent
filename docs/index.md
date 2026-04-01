# Aerospike Python Fluent Client

A fluent, chainable API for the [Aerospike](https://aerospike.com/) database,
built on top of the
[Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async).

## Quick Example

=== "Async"

    ```python
    import asyncio
    from aerospike_fluent import FluentClient, DataSet, Behavior

    async def main():
        async with FluentClient("localhost:3000") as client:
            session = client.create_session(Behavior.DEFAULT)
            users = DataSet.of("test", "users")

            # Write
            await (
                session.upsert(users.id(1))
                .bin("name").set_to("Alice")
                .bin("age").set_to(30)
                .execute()
            )

            # Read
            stream = await session.query(users.id(1)).execute()
            result = await stream.first_or_raise()
            print(result.record.bins)  # {'name': 'Alice', 'age': 30}

            # Query with DSL filter
            stream = await (
                session.query(users)
                .where("$.age > 25")
                .execute()
            )
            async for result in stream:
                print(result.record.bins)
            stream.close()

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    from aerospike_fluent import SyncFluentClient, DataSet, Behavior

    with SyncFluentClient("localhost:3000") as client:
        session = client.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # Write
        session.upsert(users.id(1)).bin("name").set_to("Alice").bin("age").set_to(30).execute()

        # Read
        stream = session.query(users.id(1)).execute()
        result = stream.first_or_raise()
        print(result.record.bins)

        # Query with DSL filter
        stream = session.query(users).where("$.age > 25").execute()
        for result in stream:
            print(result.record.bins)
        stream.close()
    ```

## Installation

```bash
pip install aerospike-fluent-client-python
```

Or install from source:

```bash
git clone https://github.com/aerospike/aerospike-client-python-fluent.git
cd aerospike-client-python-fluent
pip install -e ".[dev]"
```

## Key Concepts

**FluentClient / SyncFluentClient**
:   Entry point. Connects to an Aerospike cluster and manages the connection lifecycle.

**Session**
:   Scoped to a [`Behavior`](api/behavior.md) (policy defaults for timeouts, consistency, etc.). All reads and writes go through a session.

**DataSet**
:   A namespace + set pair. Use `DataSet.of("ns", "set")` to create one, then `.id(key)` to produce keys.

**Builders**
:   Reads return a [`QueryBuilder`](api/query.md), writes return a [`WriteSegmentBuilder`](api/write-segment.md). Chain methods, then call `.execute()`.

**Expression DSL**
:   Filter records with string expressions: `"$.age > 18 and $.status == 'active'"`. See the [DSL guide](guide/expression-dsl.md).

## Requirements

- Python 3.10+
- Aerospike Server 6.0+ (7.0+ for some features)
- [aerospike-client-python-async](https://github.com/aerospike/aerospike-client-python-async)

## Next Steps

- [Connecting to a Cluster](guide/connecting.md)
- [Reading Data](guide/reads.md)
- [Writing Data](guide/writes.md)
- [API Reference](api/index.md)
