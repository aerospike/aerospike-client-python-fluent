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
        for key_id in range(1, 40):
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

        (
            session.update(k)
                .bin("scores").list_append(40)
                .bin("meta").on_map_key("x").remove()
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40]
        assert result.record.bins["meta"] == {"y": 2}


# ===================================================================
# Nested navigation: set_to / add
# ===================================================================

class TestNestedSetTo:
    """Nested CDT navigation with set_to() write terminal."""

    def test_set_to_on_map_key(self, client):
        """Set a value at a single-level map key."""
        session = client.create_session()
        k = DS.id(15)
        session.upsert(k).put({"ratings": {"a": 1, "b": 2}}).execute()

        session.update(k).bin("ratings").on_map_key("a").set_to(10).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["ratings"] == {"a": 10, "b": 2}

    def test_set_to_creates_new_key(self, client):
        """set_to() on a non-existent key creates it."""
        session = client.create_session()
        k = DS.id(16)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        session.update(k).bin("m").on_map_key("b").set_to(2).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2}

    def test_nested_set_to_two_deep(self, client):
        """Set a value at a 2-deep nested map path."""
        session = client.create_session()
        k = DS.id(17)
        session.upsert(k).put({
            "rooms": {"room1": {"rate": 100, "avail": True}},
        }).execute()

        (
            session.update(k)
                .bin("rooms").on_map_key("room1").on_map_key("rate").set_to(150)
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rate"] == 150
        assert result.record.bins["rooms"]["room1"]["avail"] is True


class TestNestedAdd:
    """Nested CDT navigation with add() (increment) terminal."""

    def test_add_on_map_key(self, client):
        """Increment a value at a single-level map key."""
        session = client.create_session()
        k = DS.id(18)
        session.upsert(k).put({"counters": {"views": 10}}).execute()

        session.update(k).bin("counters").on_map_key("views").add(5).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["counters"]["views"] == 15

    def test_nested_add_two_deep(self, client):
        """Increment at a 2-deep nested map path."""
        session = client.create_session()
        k = DS.id(19)
        session.upsert(k).put({
            "rooms": {"room1": {"rates": {"base": 100}}},
        }).execute()

        (
            session.update(k)
                .bin("rooms").on_map_key("room1").on_map_key("rates").on_map_key("base").add(10)
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rates"]["base"] == 110


class TestNestedCombined:
    """Combined nested write operations."""

    def test_nested_set_to_and_flat_read(self, client):
        """Combine nested set_to with flat read in one execute."""
        session = client.create_session()
        k = DS.id(20)
        session.upsert(k).put({
            "meta": {"status": "draft"},
            "scores": [10, 20],
        }).execute()

        (
            session.update(k)
                .bin("meta").on_map_key("status").set_to("published")
                .bin("scores").list_append(30)
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["meta"]["status"] == "published"
        assert result.record.bins["scores"] == [10, 20, 30]

    def test_multiple_nested_writes_same_map(self, client):
        """Multiple set_to operations on different keys in the same map."""
        session = client.create_session()
        k = DS.id(21)
        session.upsert(k).put({"info": {"a": 1, "b": 2, "c": 3}}).execute()

        (
            session.update(k)
                .bin("info").on_map_key("a").set_to(10)
                .bin("info").on_map_key("c").set_to(30)
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["info"] == {"a": 10, "b": 2, "c": 30}


# ===================================================================
# Product ratings workflow (end-to-end CDT map operations)
# ===================================================================

class TestProductRatingsWorkflow:
    """End-to-end CDT map workflow: write, read by key/rank/range, update, remove."""

    def test_bulk_set_to_multiple_keys(self, client):
        """Write multiple map entries in one upsert."""
        session = client.create_session()
        k = DS.id(22)
        session.upsert(k).put({"name": "product"}).execute()

        (
            session.upsert(k)
                .bin("ratings").on_map_key("alice").set_to(5)
                .bin("ratings").on_map_key("bob").set_to(4)
                .bin("ratings").on_map_key("carol").set_to(4)
                .bin("ratings").on_map_key("dave").set_to(3)
                .bin("ratings").on_map_key("eve").set_to(5)
                .bin("ratings").on_map_key("frank").set_to(2)
                .bin("ratings").on_map_key("grace").set_to(4)
                .execute()
        )

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert len(ratings) == 7
        assert ratings["alice"] == 5
        assert ratings["frank"] == 2

    def test_read_by_map_key(self, client):
        """Read a specific map entry by key."""
        session = client.create_session()
        k = DS.id(23)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3},
        }).execute()

        result = (
            session.query(k).bin("ratings").on_map_key("alice").get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 5

    def test_read_highest_by_rank(self, client):
        """Read the highest-ranked entry (rank -1)."""
        session = client.create_session()
        k = DS.id(24)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4},
        }).execute()

        result = (
            session.query(k)
                .bin("ratings").on_map_rank(-1).get_keys_and_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == {"alice": 5}

    def test_count_by_value_range(self, client):
        """Count entries within a value range [4, 6)."""
        session = client.create_session()
        k = DS.id(25)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3, "dave": 5, "eve": 2},
        }).execute()

        result = (
            session.query(k)
                .bin("ratings").on_map_value_range(4, 6).count()
                .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 3

    def test_update_then_remove_then_verify(self, client):
        """Update a rating, remove another, then verify the map state."""
        session = client.create_session()
        k = DS.id(26)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4, "dave": 2},
        }).execute()

        session.upsert(k).bin("ratings").on_map_key("bob").set_to(5).execute()
        session.upsert(k).bin("ratings").on_map_key("dave").remove().execute()

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert ratings == {"alice": 5, "bob": 5, "carol": 4}

    def test_count_after_update_and_remove(self, client):
        """Verify value-range count changes after updates and removals."""
        session = client.create_session()
        k = DS.id(27)
        session.upsert(k).put({
            "ratings": {"a": 5, "b": 4, "c": 3, "d": 5, "e": 2, "f": 4},
        }).execute()

        result = (
            session.query(k).bin("ratings").on_map_value_range(4, 6).count()
                .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 4

        session.upsert(k).bin("ratings").on_map_key("c").set_to(5).execute()
        session.upsert(k).bin("ratings").on_map_key("e").remove().execute()

        result = (
            session.query(k).bin("ratings").on_map_value_range(4, 6).count()
                .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 5


# ===================================================================
# CDT reads within write operations
# ===================================================================

class TestCdtReadsInWriteContext:
    """CDT read operations (get_values, count) issued as part of an upsert/update."""

    def test_cdt_read_in_upsert(self, client):
        """Read a CDT value as part of an upsert; the result is returned."""
        session = client.create_session()
        k = DS.id(28)
        session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        rs = (
            session.upsert(k)
                .bin("info").on_map_key("name").get_values()
                .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["info"] == "Alice"

    def test_cdt_count_in_upsert(self, client):
        """Count CDT elements in an upsert context."""
        session = client.create_session()
        k = DS.id(29)
        session.upsert(k).put({"tags": {"a": 1, "b": 2, "c": 3}}).execute()

        rs = (
            session.upsert(k)
                .bin("tags").on_map_key_range("a", "c").count()
                .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["tags"] == 2

    def test_mixed_cdt_read_and_write_in_upsert(self, client):
        """Mix CDT reads and writes on different bins in a single upsert."""
        session = client.create_session()
        k = DS.id(30)
        session.upsert(k).put({
            "rooms": {"r1": {"rate": 100}, "r2": {"rate": 200}},
            "meta": {"ver": 1},
        }).execute()

        rs = (
            session.upsert(k)
                .bin("rooms").on_map_key("r1").get_values()
                .bin("meta").on_map_key("ver").set_to(2)
                .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["rooms"] == {"rate": 100}

        verify = session.query(k).execute().first_or_raise()
        assert verify.record.bins["meta"]["ver"] == 2

    def test_cdt_read_and_flat_write_in_upsert(self, client):
        """CDT read + flat bin write in the same upsert."""
        session = client.create_session()
        k = DS.id(31)
        session.upsert(k).put({"scores": {"math": 90, "sci": 85}}).execute()

        rs = (
            session.upsert(k)
                .bin("scores").on_map_key("math").get_values()
                .bin("status").set_to("reviewed")
                .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["scores"] == 90

        verify = session.query(k).execute().first_or_raise()
        assert verify.record.bins["status"] == "reviewed"

    def test_inverted_count_in_write_context(self, client):
        """Inverted count (count_all_others) in an upsert context."""
        session = client.create_session()
        k = DS.id(32)
        session.upsert(k).put({
            "data": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
        }).execute()

        rs = (
            session.upsert(k)
                .bin("data").on_map_key_range("b", "d").count_all_others()
                .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["data"] == 3
