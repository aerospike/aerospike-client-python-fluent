"""Example demonstrating Session usage, matching Java fluent client API."""

import asyncio
from datetime import timedelta

from aerospike_fluent import Behavior, DataSet, FluentClient


async def main():
    """Demonstrate Session usage."""
    # Create a client (similar to Cluster in Java)
    async with FluentClient("localhost:3000") as client:
        # Create a session with default behavior (like Java: cluster.createSession(Behavior.DEFAULT))
        session = client.create_session(Behavior.DEFAULT)

        # Create a DataSet
        users = DataSet.of("test", "users")

        # Java-style operations using Session
        # 1. Upsert (create or update)
        key = users.id("user123")
        await session.upsert(key=key).put({"name": "John", "age": 30, "city": "New York"})
        print("✓ Upserted record using session.upsert()")

        # 2. Get (read)
        record = await session.query(key=key).execute()
        async for rec in record:
            print(f"✓ Read record: {rec.bins}")

        # 3. Update
        await session.update(key=key).put({"age": 31})
        print("✓ Updated record using session.update()")

        # 4. Delete
        await session.delete(key=key).delete()
        print("✓ Deleted record using session.delete()")

        # 5. Exists
        exists = await session.exists(key=key).exists()
        print(f"✓ Record exists: {exists}")

        # 6. Touch (update TTL)
        await session.upsert(key=key).put({"name": "Jane"})
        await session.touch(key=key).touch()
        print("✓ Touched record using session.touch()")

        # Create a session with custom behavior
        fast_behavior = Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5),
            max_retries=1,
        )
        fast_session = client.create_session(fast_behavior)
        print(f"✓ Created session with custom behavior: {fast_session.behavior.name}")

        # Using DataSet with Session methods
        await fast_session.upsert(dataset=users, key_value="user456").put(
            {"name": "Bob", "age": 25}
        )
        print("✓ Upserted using DataSet with session.upsert()")

        # Query using Session
        async for record in fast_session.query(dataset=users).execute():
            print(f"✓ Query result: {record.bins}")

        # Cleanup
        await session.delete(key=key).delete()
        await session.delete(dataset=users, key_value="user456").delete()


if __name__ == "__main__":
    asyncio.run(main())

