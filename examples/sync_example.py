"""Example demonstrating SyncFluentClient usage (synchronous API)."""

import os

from aerospike_fluent import DataSet, SyncFluentClient


def main():
    """Demonstrate synchronous fluent API operations."""
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    # Use context manager for automatic connection management
    # No async/await needed!
    with SyncFluentClient(seeds=host) as client:
        print("✓ Connected to Aerospike")

        # Create a DataSet
        users = DataSet.of("test", "users")
        user_key = users.id("user123")

        # PUT operation - synchronous, no await
        client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).put({"name": "John", "age": 30})
        print("✓ Put record")

        # GET operation - synchronous, no await
        record = client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).get()
        print(f"✓ Got record: {record.bins if record else None}")

        # GET with specific bins
        record = client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).bins(["name"]).get()
        print(f"✓ Got record with selected bins: {record.bins if record else None}")

        # EXISTS operation
        exists = client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).exists()
        print(f"✓ Record exists: {exists}")

        # Using DataSet for key creation
        key = users.id("user456")
        client.key_value(
            namespace=users.namespace,
            set_name=users.set_name,
            key=key.value
        ).put({"name": "Jane", "age": 25})
        print("✓ Put record using DataSet")

        # DELETE operation
        existed = client.key_value(
            namespace="test",
            set_name="users",
            key="user123"
        ).delete()
        print(f"✓ Deleted record (existed: {existed})")

        # Clean up second record
        client.key_value(
            namespace="test",
            set_name="users",
            key="user456"
        ).delete()

        print("\n✓ All operations completed successfully!")


if __name__ == "__main__":
    main()

