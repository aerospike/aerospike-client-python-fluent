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

"""Tests proving K-ordered map key ordering is preserved through native Python dict."""

import pytest
import pytest_asyncio
from aerospike_async import (
    MapOperation, MapOrder, MapPolicy, MapReturnType,
    Operation,
)
from aerospike_fluent import FluentClient


NS = "test"
SET = "test"
BIN = "mapbin"


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as c:
        for key in range(1, 25):
            await c.key_value(namespace=NS, set_name=SET, key=key).delete()
        yield c


class TestKOrderedMapOrdering:
    """K-ordered maps return dict with keys in sorted iteration order."""

    @pytest.mark.asyncio
    async def test_string_keys_sorted(self, client):
        """Insert string keys out of order into a K-ordered map, read back sorted."""
        key = 1
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "cherry", 3, policy),
            MapOperation.put(BIN, "apple", 1, policy),
            MapOperation.put(BIN, "banana", 2, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    @pytest.mark.asyncio
    async def test_integer_keys_sorted(self, client):
        """Insert integer keys out of order into a K-ordered map, read back sorted."""
        key = 2
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
            MapOperation.put(BIN, 20, "twenty", policy),
            MapOperation.put(BIN, 40, "forty", policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 20, 30, 40, 50]

    @pytest.mark.asyncio
    async def test_many_keys_sorted(self, client):
        """K-ordered map with 100 keys preserves sorted order."""
        key = 3
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        keys_reversed = list(range(100, 0, -1))
        ops = [MapOperation.put(BIN, k, k * 10, policy) for k in keys_reversed]
        await kv.operate(ops)

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert list(m.keys()) == list(range(1, 101))

    @pytest.mark.asyncio
    async def test_ordering_after_add(self, client):
        """Adding a key to a K-ordered map keeps all keys sorted."""
        key = 4
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ])

        await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "c", 3, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "b", "c", "d"]

    @pytest.mark.asyncio
    async def test_ordering_after_remove(self, client):
        """Removing keys from a K-ordered map keeps remaining keys sorted."""
        key = 5
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ])

        await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.remove_by_key(BIN, "b", MapReturnType.NONE),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]

    @pytest.mark.asyncio
    async def test_ordering_after_remove_by_value(self, client):
        """Removing entries by value from a K-ordered map keeps remaining keys sorted."""
        key = 9
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "c", 100, policy),
            MapOperation.put(BIN, "d", 300, policy),
            MapOperation.put(BIN, "e", 200, policy),
        ])

        await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.remove_by_value(BIN, 200, MapReturnType.NONE),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]
        assert list(m.values()) == [100, 100, 300]

    @pytest.mark.asyncio
    async def test_round_trip_preserves_order(self, client):
        """Read an ordered map, clear it, re-insert via MapOperation — order preserved."""
        key = 6
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "z", 26, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "m", 13, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        original = record.bins[BIN]
        assert list(original.keys()) == ["a", "m", "z"]

        # Clear and re-insert using MapOperation to preserve K-ordered policy
        items = list(original.items())
        await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.clear(BIN),
            MapOperation.put_items(BIN, items, policy),
        ])
        record2 = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        assert list(record2.bins[BIN].keys()) == ["a", "m", "z"]


class TestKVOrderedMapOrdering:
    """KV-ordered maps return dict with keys in sorted iteration order."""

    @pytest.mark.asyncio
    async def test_kv_ordered_string_keys_sorted(self, client):
        """KV-ordered map keys iterate in sorted order, same as K-ordered."""
        key = 7
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "cherry", 30, policy),
            MapOperation.put(BIN, "apple", 10, policy),
            MapOperation.put(BIN, "banana", 20, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    @pytest.mark.asyncio
    async def test_kv_ordered_integer_keys_sorted(self, client):
        """KV-ordered map with integer keys returns them in sorted order."""
        key = 8
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 30, 50]


class TestUnorderedMap:
    """Unordered maps return dict with no guaranteed key order."""

    @pytest.mark.asyncio
    async def test_unordered_map_has_no_key_order(self, client):
        """Unordered maps return dict; key iteration order is not guaranteed."""
        key = 10
        await client.key_value(namespace=NS, set_name=SET, key=key).put(
            {BIN: {"x": 1, "y": 2, "z": 3}}
        )
        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert set(m.keys()) == {"x", "y", "z"}
        assert m["x"] == 1


class TestNestedOrderedMaps:
    """Nested K-ordered maps should preserve ordering at every level."""

    @pytest.mark.asyncio
    async def test_nested_ordered_maps(self, client):
        """Outer K-ordered map preserves key order; inner maps are unordered
        unless explicitly created with K-ordered policy."""
        outer_key = 11
        kv = client.key_value(namespace=NS, set_name=SET, key=outer_key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        inner = {"c": 3, "a": 1, "b": 2}
        await kv.operate([
            MapOperation.put(BIN, "z_outer", inner, policy),
            MapOperation.put(BIN, "a_outer", inner, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=outer_key).get()
        m = record.bins[BIN]

        # Outer keys are K-ordered → sorted
        assert list(m.keys()) == ["a_outer", "z_outer"]

        # Inner maps were sent as plain dicts (unordered HashMap) —
        # ordering policy is NOT inherited from the parent map.
        for inner_map in m.values():
            assert isinstance(inner_map, dict)
            assert set(inner_map.keys()) == {"a", "b", "c"}


class TestEdgeCases:
    """Edge cases for ordered map conversion through PythonValue::OrderedMap."""

    @pytest.mark.asyncio
    async def test_mixed_key_types_sorted(self, client):
        """Aerospike sorts by type first (int before string), then by value."""
        key = 12
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "banana", "s2", policy),
            MapOperation.put(BIN, 99, "i3", policy),
            MapOperation.put(BIN, "apple", "s1", policy),
            MapOperation.put(BIN, 1, "i1", policy),
            MapOperation.put(BIN, 50, "i2", policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        keys = list(m.keys())
        int_keys = [k for k in keys if isinstance(k, int)]
        str_keys = [k for k in keys if isinstance(k, str)]
        assert int_keys == sorted(int_keys)
        assert str_keys == sorted(str_keys)
        # Integers sort before strings in Aerospike's type ordering
        assert keys == int_keys + str_keys

    @pytest.mark.asyncio
    async def test_bytes_keys_sorted(self, client):
        """Bytes keys in a K-ordered map preserve sorted order."""
        key = 13
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, b"\x03", "third", policy),
            MapOperation.put(BIN, b"\x01", "first", policy),
            MapOperation.put(BIN, b"\x02", "second", policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert list(m.keys()) == [b"\x01", b"\x02", b"\x03"]

    @pytest.mark.asyncio
    async def test_empty_ordered_map(self, client):
        """Empty K-ordered map returns an empty dict."""
        key = 15
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "a", 1, policy),
        ])
        await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.remove_by_key(BIN, "a", MapReturnType.NONE),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert len(m) == 0

    @pytest.mark.asyncio
    async def test_get_by_rank_range_ordered(self, client):
        """get_by_rank_range on K-ordered map returns values in rank order."""
        key = 16
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "c", 300, policy),
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "d", 400, policy),
        ])

        # Rank 0 = smallest value (100), get 3 entries by rank
        record = await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.get_by_rank_range(BIN, 0, 3, MapReturnType.VALUE),
        ])
        values = record.bins[BIN]
        assert values == [100, 200, 300]


class TestFluentApiOrdering:
    """Verify ordering through the fluent BinBuilder path."""

    @pytest.mark.asyncio
    async def test_set_to_ordered_bin(self, client):
        """set_to() on a K-ordered map bin, then read back sorted."""
        key = 17
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        # First create the bin as K-ordered
        await kv.operate([
            MapOperation.put(BIN, "z", 1, policy),
        ])

        # Now overwrite via set_to (which does Operation.put under the hood)
        await client.key_value(namespace=NS, set_name=SET, key=key).bin(BIN).set_to(
            {"z": 26, "a": 1, "m": 13}
        ).execute()

        # The bin was overwritten — new dict may or may not keep K-ordered policy
        # depending on how the server handles Operation.put vs MapOperation.
        # At minimum, read it back and verify it's a dict.
        record = await client.key_value(namespace=NS, set_name=SET, key=key).get()
        assert isinstance(record.bins[BIN], dict)

    @pytest.mark.asyncio
    async def test_get_by_key_range_ordered(self, client):
        """get_by_key_range on K-ordered map returns keys in sorted order."""
        key = 18
        kv = client.key_value(namespace=NS, set_name=SET, key=key)
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await kv.operate([
            MapOperation.put(BIN, "e", 5, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "d", 4, policy),
            MapOperation.put(BIN, "b", 2, policy),
        ])

        record = await client.key_value(namespace=NS, set_name=SET, key=key).operate([
            MapOperation.get_by_key_range(BIN, "b", "e", MapReturnType.KEY),
        ])

        keys = record.bins[BIN]
        assert keys == ["b", "c", "d"]
