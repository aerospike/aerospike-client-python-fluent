# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Integration tests for sync CDT write operations: list_add, list_append, remove, exists."""

import pytest
from aerospike_fluent import DataSet, SyncFluentClient


NS = "test"
SET = "cdt_write_sync"
DS = DataSet.of(NS, SET)


@pytest.fixture
def client(aerospike_host, client_policy):
    with SyncFluentClient(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        for key_id in range(1, 20):
            session.delete(DS.id(key_id)).execute()
        yield c


# ===================================================================
# list_add (ordered list)
# ===================================================================

class TestListAdd:
    """Ordered list add operations via list_add()."""

    def test_list_add_maintains_sorted_order(self, client):
        """Insert values out of order; verify they are stored sorted."""
        session = client.create_session()
        k = DS.id(1)
        session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [30, 10, 50, 20, 40]:
            session.update(k).bin("scores").list_add(v).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40, 50]

    def test_list_add_to_empty_bin(self, client):
        """Create an ordered list from scratch on a non-existent bin."""
        session = client.create_session()
        k = DS.id(2)
        session.upsert(k).put({"name": "placeholder"}).execute()

        session.update(k).bin("tags").list_add("alpha").execute()
        session.update(k).bin("tags").list_add("beta").execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["tags"] == ["alpha", "beta"]


# ===================================================================
# list_append (unordered list)
# ===================================================================

class TestListAppend:
    """Unordered list append operations via list_append()."""

    def test_list_append_adds_to_end(self, client):
        """Append a value to an existing list; verify it lands at the end."""
        session = client.create_session()
        k = DS.id(3)
        session.upsert(k).put({"items": ["a", "b"]}).execute()

        session.update(k).bin("items").list_append("c").execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["a", "b", "c"]

    def test_list_append_multiple(self, client):
        """Append multiple values sequentially; verify order is preserved."""
        session = client.create_session()
        k = DS.id(4)
        session.upsert(k).put({"nums": [1]}).execute()

        session.update(k).bin("nums").list_append(2).execute()
        session.update(k).bin("nums").list_append(3).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [1, 2, 3]


# ===================================================================
# remove (map key)
# ===================================================================

class TestMapKeyRemove:
    """Remove map entries by key or key range."""

    def test_remove_map_key(self, client):
        """Remove a single map entry by key."""
        session = client.create_session()
        k = DS.id(5)
        session.upsert(k).put({"ratings": {"a": 1, "b": 2, "c": 3}}).execute()

        session.update(k).bin("ratings").on_map_key("b").remove().execute()

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert "b" not in ratings
        assert ratings == {"a": 1, "c": 3}

    def test_remove_map_key_range(self, client):
        """Remove map entries within a key range [b, d)."""
        session = client.create_session()
        k = DS.id(6)
        session.upsert(k).put({"data": {"a": 1, "b": 2, "c": 3, "d": 4}}).execute()

        session.update(k).bin("data").on_map_key_range("b", "d").remove().execute()

        result = session.query(k).execute().first_or_raise()
        data = result.record.bins["data"]
        assert data == {"a": 1, "d": 4}


# ===================================================================
# remove (list value)
# ===================================================================

class TestListValueRemove:
    """Remove list elements by value or index."""

    def test_remove_list_value(self, client):
        """Remove a list element matching a specific value."""
        session = client.create_session()
        k = DS.id(7)
        session.upsert(k).put({"scores": [10, 20, 30, 40]}).execute()

        session.update(k).bin("scores").on_list_value(20).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 30, 40]

    def test_remove_list_by_index(self, client):
        """Remove a list element at a specific index."""
        session = client.create_session()
        k = DS.id(8)
        session.upsert(k).put({"items": ["x", "y", "z"]}).execute()

        session.update(k).bin("items").on_list_index(1).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["x", "z"]


# ===================================================================
# exists (map key)
# ===================================================================

class TestMapKeyExists:
    """Check map key existence via on_map_key().exists()."""

    def test_map_key_exists_true(self, client):
        """Existing key returns True."""
        session = client.create_session()
        k = DS.id(9)
        session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        result = session.query(k).bin("info").on_map_key("name").exists().execute().first_or_raise()
        assert result.record.bins["info"] is True

    def test_map_key_exists_false(self, client):
        """Non-existent key returns False."""
        session = client.create_session()
        k = DS.id(10)
        session.upsert(k).put({"info": {"name": "Bob"}}).execute()

        result = session.query(k).bin("info").on_map_key("missing").exists().execute().first_or_raise()
        assert result.record.bins["info"] is False


# ===================================================================
# exists (list value)
# ===================================================================

class TestListValueExists:
    """Check list value existence via on_list_value().exists()."""

    def test_list_value_exists_true(self, client):
        """Present value returns True."""
        session = client.create_session()
        k = DS.id(11)
        session.upsert(k).put({"tags": ["red", "green", "blue"]}).execute()

        result = session.query(k).bin("tags").on_list_value("green").exists().execute().first_or_raise()
        assert result.record.bins["tags"] is True

    def test_list_value_exists_false(self, client):
        """Absent value returns False."""
        session = client.create_session()
        k = DS.id(12)
        session.upsert(k).put({"tags": ["red", "green"]}).execute()

        result = session.query(k).bin("tags").on_list_value("purple").exists().execute().first_or_raise()
        assert result.record.bins["tags"] is False


# ===================================================================
# Combined operations
# ===================================================================

class TestCdtWriteCombined:
    """Multi-step CDT write scenarios combining different operations."""

    def test_list_add_then_remove(self, client):
        """Add several values to an ordered list, then remove one."""
        session = client.create_session()
        k = DS.id(13)
        session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [10, 20, 30]:
            session.update(k).bin("nums").list_add(v).execute()
        session.update(k).bin("nums").list_add(25).execute()
        session.update(k).bin("nums").on_list_value(10).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [20, 25, 30]

    def test_multi_bin_cdt_ops(self, client):
        """Multiple CDT operations on different bins in one request."""
        session = client.create_session()
        k = DS.id(14)
        session.upsert(k).put({
            "scores": [10, 20, 30],
            "meta": {"x": 1, "y": 2},
        }).execute()

        session.update(k).bin("scores").list_append(40).bin("meta").on_map_key("x").remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40]
        assert result.record.bins["meta"] == {"y": 2}
