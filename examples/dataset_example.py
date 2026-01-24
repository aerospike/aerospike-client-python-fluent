"""Example demonstrating DataSet usage."""

import asyncio
import os

from aerospike_fluent import DataSet, FluentClient


async def main():
    """Demonstrate DataSet usage patterns."""
    # Get host from environment
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    
    # Create a DataSet for users
    users = DataSet.of("test", "users")
    
    print(f"Created DataSet: {users}")
    print(f"Namespace: {users.namespace}, Set: {users.set_name}")
    
    # Create single keys
    user_key1 = users.id("user123")
    user_key2 = users.id(456)
    user_key3 = users.id(b"bytes_key")
    
    print(f"\nCreated keys:")
    print(f"  String key: {user_key1}")
    print(f"  Integer key: {user_key2}")
    print(f"  Bytes key: {user_key3}")
    
    # Create multiple keys at once
    user_keys = users.ids("user1", "user2", "user3")
    print(f"\nCreated {len(user_keys)} keys from multiple arguments")
    
    # Create keys from a list
    user_keys_from_list = users.ids(["user4", "user5", "user6"])
    print(f"Created {len(user_keys_from_list)} keys from a list")
    
    # Create keys using id_for_object (automatic type detection)
    key1 = users.id_for_object("user123")
    key2 = users.id_for_object(123)
    key3 = users.id_for_object(b"bytes")
    print(f"\nCreated keys using id_for_object:")
    print(f"  {key1.value}, {key2.value}, {key3.value}")
    
    # Example: Using DataSet with FluentClient
    async with FluentClient(seeds=host) as client:
        # Use DataSet to create keys for operations
        key = users.id("example_user")
        
        # Put a record using the key from DataSet
        await client.key_value(
            namespace=users.namespace,
            set_name=users.set_name,
            key=key.value  # Extract the value from the Key
        ).put({"name": "John Doe", "age": 30})
        
        # Get the record back
        record = await client.key_value(
            namespace=users.namespace,
            set_name=users.set_name,
            key=key.value
        ).get()
        
        if record:
            print(f"\nRetrieved record: {record.bins}")
        
        # Clean up
        await client.key_value(
            namespace=users.namespace,
            set_name=users.set_name,
            key=key.value
        ).delete()


if __name__ == "__main__":
    asyncio.run(main())

