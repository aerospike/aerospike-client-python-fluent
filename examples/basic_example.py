#!/usr/bin/env python3
"""
Basic example demonstrating the fluent API.

This example shows basic key-value operations using the fluent interface.
"""

import asyncio
import os

from aerospike_fluent import FluentClient


async def main():
    """Demonstrate basic fluent API operations."""
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    # Use context manager for automatic connection management
    async with FluentClient(seeds=host) as client:
        print("✓ Connected to Aerospike")

        # PUT operation with named parameters
        await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).put({"name": "John", "age": 30})
        print("✓ Put record")

        # GET operation
        record = await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).get()
        print(f"✓ Got record: {record.bins if record else None}")

        # GET with specific bins
        record = await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).bins(["name"]).get()
        print(f"✓ Got record with selected bins: {record.bins if record else None}")

        # EXISTS operation
        exists = await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).exists()
        print(f"✓ Record exists: {exists}")

        # DELETE operation
        existed = await client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).delete()
        print(f"✓ Deleted record (existed: {existed})")

        print("\n✓ All operations completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())

