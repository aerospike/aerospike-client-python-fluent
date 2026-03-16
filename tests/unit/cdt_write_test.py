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

"""Unit tests for CDT write operations.

Covers:
- CdtReadBuilder.exists() terminal
- CdtWriteBuilder / CdtWriteInvertableBuilder remove terminals
- WriteBinBuilder CDT navigation returning write-capable builders
- WriteBinBuilder list_add / list_append operations
- Nested CDT navigation (CTX accumulation)
- set_to() / add() write terminals
"""

import pytest

from aerospike_async import (
    Key,
    ListOperation,
    ListOrderType,
    ListReturnType,
    MapOperation,
    MapReturnType,
)

from aerospike_fluent.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
)
from aerospike_fluent.aio.operations.cdt_write import (
    CdtWriteBuilder,
    CdtWriteInvertableBuilder,
)
from aerospike_fluent.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _OpCollector:
    """Minimal parent that satisfies the add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


def _make_qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit")


def _make_key(digest: int = 1) -> Key:
    return Key("test", "unit", digest)


# ===================================================================
# CdtReadBuilder.exists()
# ===================================================================

class TestCdtReadBuilderExists:

    def _build(self, *, is_map: bool = True):
        parent = _OpCollector()
        captured = []
        factory = lambda rt: (captured.append(rt), f"op_{rt}")[1]
        rt_cls = MapReturnType if is_map else ListReturnType
        builder = CdtReadBuilder(parent, factory, rt_cls, is_map=is_map)
        return builder, parent, captured

    def test_exists_map(self):
        b, parent, cap = self._build(is_map=True)
        result = b.exists()
        assert result is parent
        assert cap == [MapReturnType.EXISTS]
        assert len(parent.operations) == 1

    def test_exists_list(self):
        b, parent, cap = self._build(is_map=False)
        result = b.exists()
        assert result is parent
        assert cap == [ListReturnType.EXISTS]


# ===================================================================
# CdtWriteBuilder
# ===================================================================

class TestCdtWriteBuilder:

    def _build(self, *, is_map: bool = True):
        parent = _OpCollector()
        get_captured = []
        rm_captured = []
        get_factory = lambda rt: (get_captured.append(rt), f"get_{rt}")[1]
        rm_factory = lambda rt: (rm_captured.append(rt), f"rm_{rt}")[1]
        rt_cls = MapReturnType if is_map else ListReturnType
        builder = CdtWriteBuilder(
            parent, get_factory, rm_factory, rt_cls, is_map=is_map,
        )
        return builder, parent, get_captured, rm_captured

    def test_remove_map(self):
        b, parent, get_cap, rm_cap = self._build(is_map=True)
        result = b.remove()
        assert result is parent
        assert rm_cap == [MapReturnType.NONE]
        assert get_cap == []
        assert len(parent.operations) == 1

    def test_remove_list(self):
        b, parent, _, rm_cap = self._build(is_map=False)
        b.remove()
        assert rm_cap == [ListReturnType.NONE]

    def test_exists_inherited(self):
        b, parent, get_cap, rm_cap = self._build(is_map=True)
        result = b.exists()
        assert result is parent
        assert get_cap == [MapReturnType.EXISTS]
        assert rm_cap == []

    def test_get_values_inherited(self):
        b, parent, get_cap, rm_cap = self._build()
        b.get_values()
        assert get_cap == [MapReturnType.VALUE]
        assert rm_cap == []

    def test_count_inherited(self):
        b, _, get_cap, _ = self._build()
        b.count()
        assert get_cap == [MapReturnType.COUNT]


# ===================================================================
# CdtWriteInvertableBuilder
# ===================================================================

class TestCdtWriteInvertableBuilder:

    def _build(self, *, is_map: bool = True):
        parent = _OpCollector()
        get_captured = []
        rm_captured = []
        get_factory = lambda rt: (get_captured.append(rt), f"get_{rt}")[1]
        rm_factory = lambda rt: (rm_captured.append(rt), f"rm_{rt}")[1]
        rt_cls = MapReturnType if is_map else ListReturnType
        builder = CdtWriteInvertableBuilder(
            parent, get_factory, rm_factory, rt_cls, is_map=is_map,
        )
        return builder, parent, get_captured, rm_captured

    def test_remove(self):
        b, parent, _, rm_cap = self._build()
        result = b.remove()
        assert result is parent
        assert rm_cap == [MapReturnType.NONE]

    def test_remove_all_others(self):
        b, parent, _, rm_cap = self._build()
        result = b.remove_all_others()
        assert result is parent
        assert rm_cap == [MapReturnType.NONE | MapReturnType.INVERTED]

    def test_remove_list(self):
        b, _, _, rm_cap = self._build(is_map=False)
        b.remove()
        assert rm_cap == [ListReturnType.NONE]

    def test_remove_all_others_list(self):
        b, _, _, rm_cap = self._build(is_map=False)
        b.remove_all_others()
        assert rm_cap == [ListReturnType.NONE | ListReturnType.INVERTED]

    def test_exists_inherited(self):
        b, _, get_cap, rm_cap = self._build()
        b.exists()
        assert get_cap == [MapReturnType.EXISTS]
        assert rm_cap == []

    def test_inverted_read_inherited(self):
        b, _, get_cap, _ = self._build()
        b.get_all_other_values()
        assert get_cap == [MapReturnType.VALUE | MapReturnType.INVERTED]

    def test_count_all_others_inherited(self):
        b, _, get_cap, _ = self._build()
        b.count_all_others()
        assert get_cap == [MapReturnType.COUNT | MapReturnType.INVERTED]


# ===================================================================
# WriteBinBuilder CDT navigation
# ===================================================================

class TestWriteBinBuilderCdtNavigation:
    """Verify WriteBinBuilder CDT navigation returns write-capable builders."""

    def _build(self, bin_name: str = "mybin"):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        return WriteBinBuilder(segment, bin_name), segment

    def test_on_map_key_returns_write_builder(self):
        wbb, _ = self._build()
        result = wbb.on_map_key("k")
        assert isinstance(result, CdtWriteBuilder)
        assert not isinstance(result, CdtWriteInvertableBuilder)

    def test_on_map_index_returns_write_builder(self):
        wbb, _ = self._build()
        result = wbb.on_map_index(0)
        assert isinstance(result, CdtWriteBuilder)
        assert not isinstance(result, CdtWriteInvertableBuilder)

    def test_on_map_rank_returns_write_builder(self):
        wbb, _ = self._build()
        result = wbb.on_map_rank(0)
        assert isinstance(result, CdtWriteBuilder)
        assert not isinstance(result, CdtWriteInvertableBuilder)

    def test_on_map_value_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_value("v"), CdtWriteInvertableBuilder)

    def test_on_map_key_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_key_range("a", "z"), CdtWriteInvertableBuilder)

    def test_on_map_index_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_index_range(0, 5), CdtWriteInvertableBuilder)

    def test_on_map_rank_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_rank_range(0, 3), CdtWriteInvertableBuilder)

    def test_on_map_value_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_value_range(1, 100), CdtWriteInvertableBuilder)

    def test_on_map_key_list_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_key_list(["a", "b"]), CdtWriteInvertableBuilder)

    def test_on_map_value_list_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_map_value_list([1, 2]), CdtWriteInvertableBuilder)

    def test_on_list_index_returns_write_builder(self):
        wbb, _ = self._build()
        result = wbb.on_list_index(0)
        assert isinstance(result, CdtWriteBuilder)
        assert not isinstance(result, CdtWriteInvertableBuilder)

    def test_on_list_rank_returns_write_builder(self):
        wbb, _ = self._build()
        result = wbb.on_list_rank(0)
        assert isinstance(result, CdtWriteBuilder)
        assert not isinstance(result, CdtWriteInvertableBuilder)

    def test_on_list_value_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_list_value(42), CdtWriteInvertableBuilder)

    def test_on_list_index_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_list_index_range(0, 5), CdtWriteInvertableBuilder)

    def test_on_list_rank_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_list_rank_range(0, 3), CdtWriteInvertableBuilder)

    def test_on_list_value_range_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_list_value_range(1, 100), CdtWriteInvertableBuilder)

    def test_on_list_value_list_returns_invertable_write_builder(self):
        wbb, _ = self._build()
        assert isinstance(wbb.on_list_value_list([1, 2, 3]), CdtWriteInvertableBuilder)

    def test_on_map_key_remove_adds_operation(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("k").remove()
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_on_list_value_remove_adds_operation(self):
        wbb, segment = self._build()
        result = wbb.on_list_value(42).remove()
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_on_map_key_exists_adds_operation(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("k").exists()
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_on_list_value_exists_adds_operation(self):
        wbb, segment = self._build()
        result = wbb.on_list_value(42).exists()
        assert result is segment
        assert len(segment._qb._operations) == 1


# ===================================================================
# WriteBinBuilder list_add / list_append
# ===================================================================

class TestWriteBinBuilderListOps:

    def _build(self, bin_name: str = "mylist"):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        return WriteBinBuilder(segment, bin_name), segment

    def test_list_add_returns_segment(self):
        wbb, segment = self._build()
        result = wbb.list_add(42)
        assert result is segment

    def test_list_add_produces_operation(self):
        wbb, segment = self._build()
        wbb.list_add(42)
        assert len(segment._qb._operations) == 1
        op = segment._qb._operations[0]
        assert isinstance(op, ListOperation)

    def test_list_append_returns_segment(self):
        wbb, segment = self._build()
        result = wbb.list_append("tag")
        assert result is segment

    def test_list_append_produces_operation(self):
        wbb, segment = self._build()
        wbb.list_append("tag")
        assert len(segment._qb._operations) == 1
        op = segment._qb._operations[0]
        assert isinstance(op, ListOperation)

    def test_chaining_list_add_then_bin(self):
        wbb, segment = self._build()
        result = wbb.list_add(1).bin("other").set_to("x")
        assert isinstance(result, WriteSegmentBuilder)
        assert len(segment._qb._operations) == 2


# ===================================================================
# QueryBinBuilder.exists() (read path)
# ===================================================================

class TestQueryBinBuilderExists:
    """Verify exists() is available on read-path CDT navigation."""

    def _build(self):
        parent = _OpCollector()
        return QueryBinBuilder(parent, "mybin"), parent

    def test_on_map_key_exists(self):
        qbb, parent = self._build()
        result = qbb.on_map_key("k").exists()
        assert result is parent
        assert len(parent.operations) == 1

    def test_on_list_value_exists(self):
        qbb, parent = self._build()
        result = qbb.on_list_value(42).exists()
        assert result is parent
        assert len(parent.operations) == 1

    def test_on_map_index_exists(self):
        qbb, parent = self._build()
        result = qbb.on_map_index(0).exists()
        assert result is parent
        assert len(parent.operations) == 1


# ===================================================================
# Nested CDT navigation
# ===================================================================

class TestNestedNavigation:
    """Verify nested navigation preserves builder type and accumulates CTX."""

    def _build(self, bin_name: str = "mybin"):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        return WriteBinBuilder(segment, bin_name), segment

    def test_on_map_key_returns_navigable_builder(self):
        wbb, _ = self._build()
        b1 = wbb.on_map_key("k1")
        assert isinstance(b1, CdtWriteBuilder)
        b2 = b1.on_map_key("k2")
        assert isinstance(b2, CdtWriteBuilder)

    def test_nested_navigation_produces_operation(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("outer").on_map_key("inner").get_values()
        assert result is segment
        assert len(segment._qb._operations) == 1
        op = segment._qb._operations[0]
        assert isinstance(op, MapOperation)

    def test_three_deep_navigation(self):
        wbb, segment = self._build()
        wbb.on_map_key("a").on_map_key("b").on_map_key("c").get_values()
        assert len(segment._qb._operations) == 1

    def test_nested_remove(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("outer").on_map_key("inner").remove()
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_nested_exists(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("outer").on_map_key("inner").exists()
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_map_key_then_list_index(self):
        wbb, segment = self._build()
        wbb.on_map_key("items").on_list_index(0).get_values()
        assert len(segment._qb._operations) == 1

    def test_map_key_then_map_index(self):
        wbb, segment = self._build()
        wbb.on_map_key("nested").on_map_index(0).get_values()
        assert len(segment._qb._operations) == 1

    def test_list_index_then_map_key(self):
        wbb, segment = self._build()
        wbb.on_list_index(0).on_map_key("name").get_values()
        assert len(segment._qb._operations) == 1

    def test_non_navigable_builder_raises(self):
        parent = _OpCollector()
        builder = CdtWriteBuilder(
            parent,
            lambda rt: f"get_{rt}",
            lambda rt: f"rm_{rt}",
            MapReturnType, is_map=True,
        )
        with pytest.raises(TypeError, match="does not support further navigation"):
            builder.on_map_key("x")

    def test_read_path_nested_navigation(self):
        qbb = QueryBinBuilder(_OpCollector(), "mybin")
        b1 = qbb.on_map_key("outer")
        assert isinstance(b1, CdtReadBuilder)
        b2 = b1.on_map_key("inner")
        assert isinstance(b2, CdtReadBuilder)

    def test_read_path_nested_produces_operation(self):
        parent = _OpCollector()
        qbb = QueryBinBuilder(parent, "mybin")
        result = qbb.on_map_key("outer").on_map_key("inner").get_values()
        assert result is parent
        assert len(parent.operations) == 1


# ===================================================================
# set_to() / add() write terminals
# ===================================================================

class TestWriteTerminals:
    """Verify set_to() and add() on CdtWriteBuilder."""

    def _build(self, bin_name: str = "mybin"):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        return WriteBinBuilder(segment, bin_name), segment

    def test_set_to_on_map_key(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("k").set_to(42)
        assert result is segment
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], MapOperation)

    def test_add_on_map_key(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("k").add(5)
        assert result is segment
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], MapOperation)

    def test_set_to_not_available_on_map_index(self):
        wbb, _ = self._build()
        b = wbb.on_map_index(0)
        with pytest.raises(TypeError, match="set_to.*requires on_map_key"):
            b.set_to(42)

    def test_add_not_available_on_map_rank(self):
        wbb, _ = self._build()
        b = wbb.on_map_rank(0)
        with pytest.raises(TypeError, match="add.*requires on_map_key"):
            b.add(5)

    def test_set_to_not_available_on_list_index(self):
        wbb, _ = self._build()
        b = wbb.on_list_index(0)
        with pytest.raises(TypeError, match="set_to.*requires on_map_key"):
            b.set_to(42)

    def test_nested_set_to(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("outer").on_map_key("inner").set_to(99)
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_nested_add(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("outer").on_map_key("inner").add(10)
        assert result is segment
        assert len(segment._qb._operations) == 1

    def test_set_to_chaining_then_bin(self):
        wbb, segment = self._build()
        result = wbb.on_map_key("k").set_to(42).bin("other").set_to("x")
        assert isinstance(result, WriteSegmentBuilder)
        assert len(segment._qb._operations) == 2

    def test_set_to_after_nested_nav_loses_setter_on_non_key(self):
        wbb, _ = self._build()
        b = wbb.on_map_key("outer").on_map_index(0)
        with pytest.raises(TypeError, match="set_to.*requires on_map_key"):
            b.set_to(42)
