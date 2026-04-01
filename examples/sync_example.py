#!/usr/bin/env python3
"""Example demonstrating the synchronous fluent API.

Covers: SyncFluentClient connection, put, get, exists, delete — no async/await.
"""

import _env
from aerospike_fluent import Behavior, DataSet, SyncFluentClient


def main() -> None:
    cluster = _env.sync_connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)
    users = DataSet.of("test", "users")
    key = users.id("user123")

    try:
        # PUT
        session.upsert(key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # GET
        stream = session.query(key).execute()
        first = stream.first_or_raise()
        print(f"Got record: {first.record.bins}")
        stream.close()

        # GET with selected bins
        stream = session.query(key).bins(["name"]).execute()
        first = stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")
        stream.close()

        # EXISTS
        stream = session.exists(key).execute()
        first = stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")
        stream.close()

        # DELETE
        session.delete(key).execute()
        print("Deleted record")

        print("\nAll operations completed successfully!")
    finally:
        cluster.close()


if __name__ == "__main__":
    main()
