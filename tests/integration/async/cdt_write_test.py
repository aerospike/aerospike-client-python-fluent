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

"""Integration tests for CDT write operations: list_add, list_append, remove, exists."""

import pytest
import pytest_asyncio
from aerospike_async import ListOrderType, ListSortFlags, MapOrder
from aerospike_async.exceptions import ResultCode

from aerospike_fluent import DataSet, FluentClient
from aerospike_fluent.exceptions import AerospikeError


NS = "test"
SET = "cdt_write"
DS = DataSet.of(NS, SET)


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        for key_id in range(1, 52):
            await session.delete(DS.id(key_id)).execute()
        yield c


# ===================================================================
# list_add (ordered list)
# ===================================================================

class TestListAdd:
    """Ordered list add operations via list_add()."""

    @pytest.mark.asyncio
    async def test_list_add_maintains_sorted_order(self, client):
        """Insert values out of order; verify they are stored sorted."""
        session = client.create_session()
        k = DS.id(1)
        await session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [30, 10, 50, 20, 40]:
            await session.update(k).bin("scores").list_add(v).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40, 50]

    @pytest.mark.asyncio
    async def test_list_add_to_empty_bin(self, client):
        """Create an ordered list from scratch on a non-existent bin."""
        session = client.create_session()
        k = DS.id(2)
        await session.upsert(k).put({"name": "placeholder"}).execute()

        await session.update(k).bin("tags").list_add("alpha").execute()
        await session.update(k).bin("tags").list_add("beta").execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["tags"] == ["alpha", "beta"]


# ===================================================================
# list_append (unordered list)
# ===================================================================

class TestListAppend:
    """Unordered list append operations via list_append()."""

    @pytest.mark.asyncio
    async def test_list_append_adds_to_end(self, client):
        """Append a value to an existing list; verify it lands at the end."""
        session = client.create_session()
        k = DS.id(3)
        await session.upsert(k).put({"items": ["a", "b"]}).execute()

        await session.update(k).bin("items").list_append("c").execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["items"] == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_list_append_multiple(self, client):
        """Append multiple values sequentially; verify order is preserved."""
        session = client.create_session()
        k = DS.id(4)
        await session.upsert(k).put({"nums": [1]}).execute()

        await session.update(k).bin("nums").list_append(2).execute()
        await session.update(k).bin("nums").list_append(3).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["nums"] == [1, 2, 3]


# ===================================================================
# remove (map key)
# ===================================================================

class TestMapKeyRemove:
    """Remove map entries by key or key range."""

    @pytest.mark.asyncio
    async def test_remove_map_key(self, client):
        """Remove a single map entry by key."""
        session = client.create_session()
        k = DS.id(5)
        await session.upsert(k).put({"ratings": {"a": 1, "b": 2, "c": 3}}).execute()

        await session.update(k).bin("ratings").on_map_key("b").remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        ratings = result.record.bins["ratings"]
        assert "b" not in ratings
        assert ratings == {"a": 1, "c": 3}

    @pytest.mark.asyncio
    async def test_remove_map_key_range(self, client):
        """Remove map entries within a key range [b, d)."""
        session = client.create_session()
        k = DS.id(6)
        await session.upsert(k).put({"data": {"a": 1, "b": 2, "c": 3, "d": 4}}).execute()

        await session.update(k).bin("data").on_map_key_range("b", "d").remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        data = result.record.bins["data"]
        assert data == {"a": 1, "d": 4}


# ===================================================================
# remove (list value)
# ===================================================================

class TestListValueRemove:
    """Remove list elements by value or index."""

    @pytest.mark.asyncio
    async def test_remove_list_value(self, client):
        """Remove a list element matching a specific value."""
        session = client.create_session()
        k = DS.id(7)
        await session.upsert(k).put({"scores": [10, 20, 30, 40]}).execute()

        await session.update(k).bin("scores").on_list_value(20).remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["scores"] == [10, 30, 40]

    @pytest.mark.asyncio
    async def test_remove_list_by_index(self, client):
        """Remove a list element at a specific index."""
        session = client.create_session()
        k = DS.id(8)
        await session.upsert(k).put({"items": ["x", "y", "z"]}).execute()

        await session.update(k).bin("items").on_list_index(1).remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["items"] == ["x", "z"]


# ===================================================================
# exists (map key)
# ===================================================================

class TestMapKeyExists:
    """Check map key existence via on_map_key().exists()."""

    @pytest.mark.asyncio
    async def test_map_key_exists_true(self, client):
        """Existing key returns True."""
        session = client.create_session()
        k = DS.id(9)
        await session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        result = await (
            await session.query(k).bin("info").on_map_key("name").exists().execute()
        ).first_or_raise()
        assert result.record.bins["info"] is True

    @pytest.mark.asyncio
    async def test_map_key_exists_false(self, client):
        """Non-existent key returns False."""
        session = client.create_session()
        k = DS.id(10)
        await session.upsert(k).put({"info": {"name": "Bob"}}).execute()

        result = await (
            await session.query(k).bin("info").on_map_key("missing").exists().execute()
        ).first_or_raise()
        assert result.record.bins["info"] is False


# ===================================================================
# exists (list value)
# ===================================================================

class TestListValueExists:
    """Check list value existence via on_list_value().exists()."""

    @pytest.mark.asyncio
    async def test_list_value_exists_true(self, client):
        """Present value returns True."""
        session = client.create_session()
        k = DS.id(11)
        await session.upsert(k).put({"tags": ["red", "green", "blue"]}).execute()

        result = await (
            await session.query(k).bin("tags").on_list_value("green").exists().execute()
        ).first_or_raise()
        assert result.record.bins["tags"] is True

    @pytest.mark.asyncio
    async def test_list_value_exists_false(self, client):
        """Absent value returns False."""
        session = client.create_session()
        k = DS.id(12)
        await session.upsert(k).put({"tags": ["red", "green"]}).execute()

        result = await (
            await session.query(k).bin("tags").on_list_value("purple").exists().execute()
        ).first_or_raise()
        assert result.record.bins["tags"] is False


# ===================================================================
# Combined operations
# ===================================================================

class TestCdtWriteCombined:
    """Multi-step CDT write scenarios combining different operations."""

    @pytest.mark.asyncio
    async def test_list_add_then_remove(self, client):
        """Add several values to an ordered list, then remove one."""
        session = client.create_session()
        k = DS.id(13)
        await session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [10, 20, 30]:
            await session.update(k).bin("nums").list_add(v).execute()
        await session.update(k).bin("nums").list_add(25).execute()
        await session.update(k).bin("nums").on_list_value(10).remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["nums"] == [20, 25, 30]

    @pytest.mark.asyncio
    async def test_multi_bin_cdt_ops(self, client):
        """Multiple CDT operations on different bins in one request."""
        session = client.create_session()
        k = DS.id(14)
        await session.upsert(k).put({
            "scores": [10, 20, 30],
            "meta": {"x": 1, "y": 2},
        }).execute()

        await (
            session.update(k)
                .bin("scores").list_append(40)
                .bin("meta").on_map_key("x").remove()
                .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40]
        assert result.record.bins["meta"] == {"y": 2}


# ===================================================================
# Nested navigation: set_to / add
# ===================================================================

class TestNestedSetTo:
    """Nested CDT navigation with set_to() write terminal."""

    @pytest.mark.asyncio
    async def test_set_to_on_map_key(self, client):
        """Set a value at a single-level map key."""
        session = client.create_session()
        k = DS.id(15)
        await session.upsert(k).put({"ratings": {"a": 1, "b": 2}}).execute()

        await session.update(k).bin("ratings").on_map_key("a").set_to(10).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["ratings"] == {"a": 10, "b": 2}

    @pytest.mark.asyncio
    async def test_set_to_creates_new_key(self, client):
        """set_to() on a non-existent key creates it."""
        session = client.create_session()
        k = DS.id(16)
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        await session.update(k).bin("m").on_map_key("b").set_to(2).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2}

    @pytest.mark.asyncio
    async def test_nested_set_to_two_deep(self, client):
        """Set a value at a 2-deep nested map path."""
        session = client.create_session()
        k = DS.id(17)
        await session.upsert(k).put({
            "rooms": {"room1": {"rate": 100, "avail": True}},
        }).execute()

        await (
            session.update(k)
                .bin("rooms").on_map_key("room1").on_map_key("rate").set_to(150)
                .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rate"] == 150
        assert result.record.bins["rooms"]["room1"]["avail"] is True


class TestNestedAdd:
    """Nested CDT navigation with add() (increment) terminal."""

    @pytest.mark.asyncio
    async def test_add_on_map_key(self, client):
        """Increment a value at a single-level map key."""
        session = client.create_session()
        k = DS.id(18)
        await session.upsert(k).put({"counters": {"views": 10}}).execute()

        await session.update(k).bin("counters").on_map_key("views").add(5).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["counters"]["views"] == 15

    @pytest.mark.asyncio
    async def test_nested_add_two_deep(self, client):
        """Increment at a 2-deep nested map path."""
        session = client.create_session()
        k = DS.id(19)
        await session.upsert(k).put({
            "rooms": {"room1": {"rates": {"base": 100}}},
        }).execute()

        await (
            session.update(k)
                .bin("rooms").on_map_key("room1").on_map_key("rates").on_map_key("base").add(10)
                .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rates"]["base"] == 110


class TestNestedCombined:
    """Combined nested write operations."""

    @pytest.mark.asyncio
    async def test_nested_set_to_and_flat_read(self, client):
        """Combine nested set_to with flat read in one execute."""
        session = client.create_session()
        k = DS.id(20)
        await session.upsert(k).put({
            "meta": {"status": "draft"},
            "scores": [10, 20],
        }).execute()

        await (
            session.update(k)
                .bin("meta").on_map_key("status").set_to("published")
                .bin("scores").list_append(30)
                .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["meta"]["status"] == "published"
        assert result.record.bins["scores"] == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_multiple_nested_writes_same_map(self, client):
        """Multiple set_to operations on different keys in the same map."""
        session = client.create_session()
        k = DS.id(21)
        await session.upsert(k).put({"info": {"a": 1, "b": 2, "c": 3}}).execute()

        await (
            session.update(k)
                .bin("info").on_map_key("a").set_to(10)
                .bin("info").on_map_key("c").set_to(30)
                .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["info"] == {"a": 10, "b": 2, "c": 30}


# ===================================================================
# Product ratings workflow (end-to-end CDT map operations)
# ===================================================================

class TestProductRatingsWorkflow:
    """End-to-end CDT map workflow: write, read by key/rank/range, update, remove."""

    @pytest.mark.asyncio
    async def test_bulk_set_to_multiple_keys(self, client):
        """Write multiple map entries in one upsert."""
        session = client.create_session()
        k = DS.id(22)
        await session.upsert(k).put({"name": "product"}).execute()

        await (
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

        result = await (await session.query(k).execute()).first_or_raise()
        ratings = result.record.bins["ratings"]
        assert len(ratings) == 7
        assert ratings["alice"] == 5
        assert ratings["frank"] == 2

    @pytest.mark.asyncio
    async def test_read_by_map_key(self, client):
        """Read a specific map entry by key."""
        session = client.create_session()
        k = DS.id(23)
        await session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3},
        }).execute()

        result = await (
            await session.query(k).bin("ratings").on_map_key("alice").get_values().execute()
        ).first_or_raise()
        assert result.record.bins["ratings"] == 5

    @pytest.mark.asyncio
    async def test_read_highest_by_rank(self, client):
        """Read the highest-ranked entry (rank -1)."""
        session = client.create_session()
        k = DS.id(24)
        await session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4},
        }).execute()

        result = await (
            await session.query(k)
                .bin("ratings").on_map_rank(-1).get_keys_and_values()
                .execute()
        ).first_or_raise()
        assert result.record.bins["ratings"] == {"alice": 5}

    @pytest.mark.asyncio
    async def test_count_by_value_range(self, client):
        """Count entries within a value range [4, 6)."""
        session = client.create_session()
        k = DS.id(25)
        await session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3, "dave": 5, "eve": 2},
        }).execute()

        result = await (
            await session.query(k)
                .bin("ratings").on_map_value_range(4, 6).count()
                .execute()
        ).first_or_raise()
        assert result.record.bins["ratings"] == 3

    @pytest.mark.asyncio
    async def test_update_then_remove_then_verify(self, client):
        """Update a rating, remove another, then verify the map state."""
        session = client.create_session()
        k = DS.id(26)
        await session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4, "dave": 2},
        }).execute()

        await session.upsert(k).bin("ratings").on_map_key("bob").set_to(5).execute()
        await session.upsert(k).bin("ratings").on_map_key("dave").remove().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        ratings = result.record.bins["ratings"]
        assert ratings == {"alice": 5, "bob": 5, "carol": 4}

    @pytest.mark.asyncio
    async def test_count_after_update_and_remove(self, client):
        """Verify value-range count changes after updates and removals."""
        session = client.create_session()
        k = DS.id(27)
        await session.upsert(k).put({
            "ratings": {"a": 5, "b": 4, "c": 3, "d": 5, "e": 2, "f": 4},
        }).execute()

        result = await (
            await session.query(k).bin("ratings").on_map_value_range(4, 6).count().execute()
        ).first_or_raise()
        assert result.record.bins["ratings"] == 4

        await session.upsert(k).bin("ratings").on_map_key("c").set_to(5).execute()
        await session.upsert(k).bin("ratings").on_map_key("e").remove().execute()

        result = await (
            await session.query(k).bin("ratings").on_map_value_range(4, 6).count().execute()
        ).first_or_raise()
        assert result.record.bins["ratings"] == 5


# ===================================================================
# CDT reads within write operations
# ===================================================================

class TestCdtReadsInWriteContext:
    """CDT read operations (get_values, count) issued as part of an upsert/update."""

    @pytest.mark.asyncio
    async def test_cdt_read_in_upsert(self, client):
        """Read a CDT value as part of an upsert; the result is returned."""
        session = client.create_session()
        k = DS.id(28)
        await session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        rs = await (
            session.upsert(k)
                .bin("info").on_map_key("name").get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["info"] == "Alice"

    @pytest.mark.asyncio
    async def test_cdt_count_in_upsert(self, client):
        """Count CDT elements in an upsert context."""
        session = client.create_session()
        k = DS.id(29)
        await session.upsert(k).put({"tags": {"a": 1, "b": 2, "c": 3}}).execute()

        rs = await (
            session.upsert(k)
                .bin("tags").on_map_key_range("a", "c").count()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["tags"] == 2

    @pytest.mark.asyncio
    async def test_mixed_cdt_read_and_write_in_upsert(self, client):
        """Mix CDT reads and writes on different bins in a single upsert."""
        session = client.create_session()
        k = DS.id(30)
        await session.upsert(k).put({
            "rooms": {"r1": {"rate": 100}, "r2": {"rate": 200}},
            "meta": {"ver": 1},
        }).execute()

        rs = await (
            session.upsert(k)
                .bin("rooms").on_map_key("r1").get_values()
                .bin("meta").on_map_key("ver").set_to(2)
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["rooms"] == {"rate": 100}

        verify = await (await session.query(k).execute()).first_or_raise()
        assert verify.record.bins["meta"]["ver"] == 2

    @pytest.mark.asyncio
    async def test_cdt_read_and_flat_write_in_upsert(self, client):
        """CDT read + flat bin write in the same upsert."""
        session = client.create_session()
        k = DS.id(31)
        await session.upsert(k).put({"scores": {"math": 90, "sci": 85}}).execute()

        rs = await (
            session.upsert(k)
                .bin("scores").on_map_key("math").get_values()
                .bin("status").set_to("reviewed")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 90

        verify = await (await session.query(k).execute()).first_or_raise()
        assert verify.record.bins["status"] == "reviewed"

    @pytest.mark.asyncio
    async def test_inverted_count_in_write_context(self, client):
        """Inverted count (count_all_others) in an upsert context."""
        session = client.create_session()
        k = DS.id(32)
        await session.upsert(k).put({
            "data": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
        }).execute()

        rs = await (
            session.upsert(k)
                .bin("data").on_map_key_range("b", "d").count_all_others()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["data"] == 3


# ===================================================================
# Collection-level map operations
# ===================================================================

class TestMapCollectionOps:
    """Top-level and nested map collection CDT operations."""

    @pytest.mark.asyncio
    async def test_map_clear_top_level(self, client):
        session = client.create_session()
        k = DS.id(33)
        await session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        await session.update(k).bin("m").map_clear().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["m"] == {}

    @pytest.mark.asyncio
    async def test_map_clear_nested(self, client):
        session = client.create_session()
        k = DS.id(34)
        await session.upsert(k).put({"outer": {"inner": {"x": 1, "y": 2}}}).execute()

        await session.update(k).bin("outer").on_map_key("inner").map_clear().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["outer"]["inner"] == {}

    @pytest.mark.asyncio
    async def test_map_upsert_items(self, client):
        session = client.create_session()
        k = DS.id(35)
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        await session.update(k).bin("m").map_upsert_items({"b": 2, "c": 3}).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2, "c": 3}

    @pytest.mark.asyncio
    async def test_map_insert_items_new_keys_only(self, client):
        session = client.create_session()
        k = DS.id(36)
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        await session.update(k).bin("m").map_insert_items({"b": 2}).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2}

    @pytest.mark.asyncio
    async def test_map_update_items_existing_keys_only(self, client):
        session = client.create_session()
        k = DS.id(37)
        await session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        await session.update(k).bin("m").map_update_items({"a": 10}).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["m"] == {"a": 10, "b": 2}

    @pytest.mark.asyncio
    async def test_map_insert_items_rejects_existing_key(self, client):
        """CREATE_ONLY refuses to overwrite an existing map key."""
        session = client.create_session()
        k = DS.id(49)
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await session.update(k).bin("m").map_insert_items({"a": 2}).execute()
        assert exc_info.value.result_code == ResultCode.ELEMENT_EXISTS

    @pytest.mark.asyncio
    async def test_map_update_items_rejects_new_key(self, client):
        """UPDATE_ONLY refuses to create a map entry for a missing key."""
        session = client.create_session()
        k = DS.id(50)
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await session.update(k).bin("m").map_update_items({"b": 2}).execute()
        assert exc_info.value.result_code == ResultCode.ELEMENT_NOT_FOUND

    @pytest.mark.asyncio
    async def test_map_create_then_upsert_key_ordered(self, client):
        session = client.create_session()
        k = DS.id(38)
        await session.upsert(k).put({"p": 1}).execute()

        await session.update(k).bin("ord").map_create(MapOrder.KEY_ORDERED).execute()
        await session.update(k).bin("ord").map_upsert_items(
            {"c": 3, "a": 1, "b": 2},
        ).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert list(result.record.bins["ord"].keys()) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_map_set_policy_key_ordered(self, client):
        session = client.create_session()
        k = DS.id(39)
        await session.upsert(k).put({"m": {"z": 1, "a": 2}}).execute()

        await session.update(k).bin("m").map_set_policy(MapOrder.KEY_ORDERED).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert list(result.record.bins["m"].keys()) == ["a", "z"]

    @pytest.mark.asyncio
    async def test_map_size_in_update_context(self, client):
        session = client.create_session()
        k = DS.id(40)
        await session.upsert(k).put({"m": {"x": 1, "y": 2, "z": 3}}).execute()

        rs = await session.update(k).bin("m").map_size().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["m"] == 3


# ===================================================================
# Collection-level list operations
# ===================================================================

class TestListCollectionOps:
    """List clear, sort, bulk append, create, and order."""

    @pytest.mark.asyncio
    async def test_list_clear(self, client):
        session = client.create_session()
        k = DS.id(41)
        await session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        await session.update(k).bin("lst").list_clear().execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["lst"] == []

    @pytest.mark.asyncio
    async def test_list_sort_descending(self, client):
        session = client.create_session()
        k = DS.id(42)
        await session.upsert(k).put({"lst": [1, 5, 3, 2]}).execute()

        await session.update(k).bin("lst").list_sort(ListSortFlags.DESCENDING).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["lst"] == [5, 3, 2, 1]

    @pytest.mark.asyncio
    async def test_list_append_items(self, client):
        session = client.create_session()
        k = DS.id(43)
        await session.upsert(k).put({"lst": [1, 2]}).execute()

        await session.update(k).bin("lst").list_append_items([3, 4]).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["lst"] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_list_add_items_ordered(self, client):
        session = client.create_session()
        k = DS.id(44)
        await session.upsert(k).put({"name": "x", "scores": []}).execute()
        await session.update(k).bin("scores").list_set_order(
            ListOrderType.ORDERED,
        ).execute()
        await session.update(k).bin("scores").list_add_items([30, 10, 20]).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_empty_list_then_append_items(self, client):
        session = client.create_session()
        k = DS.id(45)
        await session.upsert(k).put({"t": 1, "items": []}).execute()
        await session.update(k).bin("items").list_append_items(["a", "b"]).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["items"] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_list_set_order(self, client):
        session = client.create_session()
        k = DS.id(46)
        await session.upsert(k).put({"lst": [3, 1, 2]}).execute()

        await session.update(k).bin("lst").list_set_order(ListOrderType.ORDERED).execute()

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["lst"] == [1, 2, 3]


# ===================================================================
# Nested collection-level after on_map_key
# ===================================================================

class TestNestedMapCollectionOps:
    """Collection-level ops with accumulated CDT context."""

    @pytest.mark.asyncio
    async def test_nested_map_upsert_items(self, client):
        session = client.create_session()
        k = DS.id(47)
        await session.upsert(k).put({"root": {"inner": {"a": 1}}}).execute()

        await (
            session.update(k)
            .bin("root").on_map_key("inner").map_upsert_items({"b": 2})
            .execute()
        )

        result = await (await session.query(k).execute()).first_or_raise()
        assert result.record.bins["root"]["inner"] == {"a": 1, "b": 2}

    @pytest.mark.asyncio
    async def test_nested_map_size_read_path(self, client):
        session = client.create_session()
        k = DS.id(48)
        await session.upsert(k).put({"data": {"sub": {"u": 1, "v": 2}}}).execute()

        result = await (
            await session.query(k).bin("data").on_map_key("sub").map_size().execute()
        ).first_or_raise()
        assert result.record.bins["data"] == 2
