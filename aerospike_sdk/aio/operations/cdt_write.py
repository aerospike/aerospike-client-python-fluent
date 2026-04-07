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

"""CDT action builders for write contexts.

Extends the read-only builders from ``cdt_read`` with:

- ``remove()`` / ``remove_all_others()`` terminals
- ``set_to(value)`` / ``add(value)`` write terminals (map key navigation)
- Collection-level map/list terminals (``map_clear``, ``map_upsert_items``, …)
- Nested navigation that preserves write capability through the chain
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from aerospike_async import (
    CTX,
    ListOperation,
    ListOrderType,
    ListPolicy,
    ListSortFlags,
    MapOperation,
    MapOrder,
    MapPolicy,
    MapReturnType,
    MapWriteFlags,
)

from aerospike_sdk.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
    T,
    _ReturnTypeCls,
    _map_item_pairs,
)

_DEFAULT_MAP_POLICY = MapPolicy(None, None)
_UNORDERED_LIST_POLICY = ListPolicy(None, None)
_ORDERED_LIST_POLICY = ListPolicy(ListOrderType.ORDERED, None)


class _RemoveMixin:
    """Shared remove logic for CDT write builders."""

    _remove_factory: Callable[[Any], Any]
    _parent: Any
    _rt: Any

    def _emit_remove(self, return_type: Any) -> Any:
        op = self._remove_factory(return_type)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def remove(self) -> Any:
        """Remove the selected CDT element(s).

        Returns:
            The parent builder for chaining.
        """
        return self._emit_remove(self._rt.NONE)


class CdtWriteBuilder(_RemoveMixin, CdtReadBuilder[T]):
    """CDT action builder with read, remove, and write terminals.

    Inherits all read terminals and navigation from ``CdtReadBuilder``.
    Adds ``remove()``, and on map-key paths ``set_to()`` / ``add()``.
    Navigation returns ``CdtWriteBuilder`` to preserve write capability.

    Example:
        Set a nested map value and remove another key::

            await (
                session.upsert(key)
                    .bin("settings").on_map_key("theme").set_to("dark")
                    .bin("settings").on_map_key("old_key").remove()
                    .execute()
            )
    """

    __slots__ = ("_remove_factory", "_set_to_factory", "_add_factory")

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        remove_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        bin_name: str = "",
        ctx: Sequence[Any] = (),
        to_ctx: Callable[[], Any] | None = None,
        set_to_factory: Callable[[Any], Any] | None = None,
        add_factory: Callable[[Any], Any] | None = None,
    ) -> None:
        super().__init__(
            parent, op_factory, return_type_cls, is_map=is_map,
            bin_name=bin_name, ctx=ctx, to_ctx=to_ctx,
        )
        self._remove_factory = remove_factory
        self._set_to_factory = set_to_factory
        self._add_factory = add_factory

    def _build_navigated(
        self, *, op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls, is_map: bool,
        ctx: Sequence[Any], to_ctx: Callable[[], Any],
        **extra: Any,
    ) -> CdtWriteBuilder[T]:
        return CdtWriteBuilder(
            self._parent, op_factory,
            extra.get("remove_factory", self._remove_factory),
            rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx,
            set_to_factory=extra.get("set_to_factory"),
            add_factory=extra.get("add_factory"),
        )

    def on_map_key(self, key: Any) -> CdtWriteBuilder[T]:
        """Navigate into a map element by key (supports set_to / add).

        Args:
            key: Map key to target.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_key(b, key, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_key(b, key, rt).set_context(ctx_l),
            set_to_factory=lambda v: MapOperation.put(b, key, v, _DEFAULT_MAP_POLICY).set_context(ctx_l),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _DEFAULT_MAP_POLICY).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_key(key),
        )

    # -- Write terminals ------------------------------------------------------

    def set_to(self, value: Any) -> T:
        """Set the value at the current CDT path.

        Requires prior ``on_map_key()`` navigation.

        Args:
            value: New value to store.

        Example::
            .bin("m").on_map_key("color").set_to("blue")

        Raises:
            TypeError: If not preceded by ``on_map_key()`` navigation.
        """
        if self._set_to_factory is None:
            raise TypeError(
                "set_to() requires on_map_key() navigation"
            )
        op = self._set_to_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def add(self, value: Any) -> T:
        """Increment the value at the current CDT path.

        Args:
            value: Numeric value to add.

        Returns:
            The parent builder for chaining.

        Raises:
            TypeError: If not preceded by ``on_map_key()`` navigation.
        """
        if self._add_factory is None:
            raise TypeError(
                "add() requires on_map_key() navigation"
            )
        op = self._add_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> T:
        """Remove all entries from the map at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.clear(self._bin_name).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_upsert_items(self, items: Any) -> T:
        """Put multiple map entries (create or update each key).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        op = MapOperation.put_items(
            self._bin_name, pairs, _DEFAULT_MAP_POLICY,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_insert_items(self, items: Any) -> T:
        """Put map entries only for keys that do not yet exist.

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.CREATE_ONLY)
        op = MapOperation.put_items(
            self._bin_name, pairs, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_update_items(self, items: Any) -> T:
        """Update existing map entries only (no new keys).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.UPDATE_ONLY)
        op = MapOperation.put_items(
            self._bin_name, pairs, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_create(self, order: MapOrder) -> T:
        """Create an empty map with the given key order.

        Args:
            order: Key sort order for the map.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.create(self._bin_name, order).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_set_policy(self, order: MapOrder) -> T:
        """Set map sort order policy without changing entries.

        Args:
            order: Key sort order to apply.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.set_map_policy(
            self._bin_name, MapPolicy(order, None),
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> T:
        """Remove all elements from the list at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.clear(self._bin_name).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_sort(self, flags: ListSortFlags = ListSortFlags.DEFAULT) -> T:
        """Sort the list at the current path.

        Args:
            flags: Sort behaviour flags (default ``DEFAULT``).

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.sort(self._bin_name, flags).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_append_items(self, items: Sequence[Any]) -> T:
        """Append values to an unordered list.

        Args:
            items: Values to append.

        Returns:
            The parent builder for chaining.

        Example::
            .bin("tags").on_map_key("items").list_append_items([10, 20])
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.append_items(
            self._bin_name, items, _UNORDERED_LIST_POLICY,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_add_items(self, items: Sequence[Any]) -> T:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Values to insert.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.append_items(
            self._bin_name, items, _ORDERED_LIST_POLICY,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> T:
        """Create an empty list with the given order.

        Args:
            order: Element ordering for the list.
            pad: If ``True``, allow sparse indexes.
            persist_index: If ``True``, maintain a persistent index.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.create(
            self._bin_name, order, pad, persist_index,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_set_order(self, order: ListOrderType) -> T:
        """Set list sort order without changing elements.

        Args:
            order: Sort order to apply.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.set_order(self._bin_name, order).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent


class CdtWriteInvertableBuilder(_RemoveMixin, CdtReadInvertableBuilder[T]):
    """CDT action builder with read, inverted-read, and remove terminals.

    Inherits all read and inverted-read terminals from
    ``CdtReadInvertableBuilder`` and adds ``remove()`` /
    ``remove_all_others()``.  Does not support further navigation.

    Example:
        Remove everything except the selected range::

            .bin("m").on_map_value_range(0, 100).remove_all_others()
    """

    __slots__ = ("_remove_factory",)

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        remove_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
    ) -> None:
        super().__init__(parent, op_factory, return_type_cls, is_map=is_map)
        self._remove_factory = remove_factory

    def remove_all_others(self) -> T:
        """Remove all CDT elements *except* the selected ones.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_remove(self._rt.NONE | self._rt.INVERTED)
