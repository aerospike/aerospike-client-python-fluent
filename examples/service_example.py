#!/usr/bin/env python3
"""
Example demonstrating the service pattern for key-value operations.

This example shows how to use KeyValueService for multiple operations
on the same namespace/set, which is more efficient than using builders.
"""

import asyncio
import os

from aerospike_fluent import FluentClient


async def main():
    """Demonstrate service pattern operations."""
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    async with FluentClient(seeds=host) as client:
        print("✓ Connected to Aerospike")

        # Use service pattern for multiple operations on same namespace/set
        async with client.key_value_service(
            namespace="test",
            set_name="users"
        ) as kv:
            print("\n--- Using KeyValueService ---")

            # Put multiple records - no need to repeat namespace/set
            await kv.put("user1", {"name": "John", "age": 30})
            await kv.put("user2", {"name": "Jane", "age": 25})
            await kv.put("user3", {"name": "Bob", "age": 35})
            print("✓ Put 3 records")

            # Get records
            record1 = await kv.get("user1")
            record2 = await kv.get("user2", bins=["name"])
            print(f"✓ Got user1: {record1.bins if record1 else None}")
            print(f"✓ Got user2 (name only): {record2.bins if record2 else None}")

            # Update records
            await kv.add("user1", {"age": 1})  # Increment age
            await kv.append("user1", {"name": " Doe"})  # Append to name
            print("✓ Updated user1")

            # Verify updates
            updated = await kv.get("user1")
            print(f"✓ Updated user1: {updated.bins if updated else None}")

            # Check existence
            exists = await kv.exists("user1")
            print(f"✓ user1 exists: {exists}")

            # Delete a record
            existed = await kv.delete("user3")
            print(f"✓ Deleted user3 (existed: {existed})")

        print("\n--- Using TransactionalSession (placeholder) ---")
        # Transactional session - currently a placeholder for future transaction support
        async with client.transaction_session() as session:
            kv = session.key_value("test", "users")
            await kv.put("user4", {"name": "Alice", "age": 28})
            await kv.put("user5", {"name": "Charlie", "age": 32})
            print("✓ Put 2 records in transaction session")
            # Transaction would be auto-committed on exit (when supported)

        print("\n✓ All service operations completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())


