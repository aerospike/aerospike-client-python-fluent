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

"""Read-only CDT action builders for query bin operations.

Two builder classes provide terminal read methods on a CDT path:

- ``CdtReadBuilder``  -- non-invertable terminals (get_values, count, …)
  plus singular CDT navigation for deeper nesting.
- ``CdtReadInvertableBuilder`` -- adds inverted "all others" terminals
  (no further navigation).

Both are generic on the parent builder type ``T`` so terminal methods
return the parent for continued chaining.  The actual CDT operation is
produced by an ``op_factory`` callable injected by the navigation method
that created the builder.

Nested navigation accumulates ``CTX`` entries.  Each navigation step
pushes the current selector into the context and creates new factories
for the next selector with ``.set_context()`` applied.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable, Generic, Sequence, TypeVar, Union

from aerospike_async import (
    CTX,
    ListOperation,
    ListReturnType,
    MapOperation,
    MapReturnType,
)

T = TypeVar("T")

_ReturnTypeCls = Union[type[MapReturnType], type[ListReturnType]]


def _map_item_pairs(items: Mapping[Any, Any] | Sequence[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    """Normalize mapping or sequence of pairs for ``MapOperation.put_items``."""
    if isinstance(items, Mapping):
        return list(items.items())
    return list(items)


class CdtReadBuilder(Generic[T]):
    """Terminal read actions and singular navigation for a CDT path.

    The parent (type ``T``) must expose ``add_operation(op)`` so the
    builder can register the produced operation before returning it.

    Navigation fields (``bin_name``, ``ctx``, ``to_ctx``) are optional;
    when not provided, further navigation is not available (terminal-only).
    """

    __slots__ = (
        "_parent", "_op_factory", "_rt", "_is_map",
        "_bin_name", "_ctx", "_to_ctx",
    )

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        bin_name: str = "",
        ctx: Sequence[Any] = (),
        to_ctx: Callable[[], Any] | None = None,
    ) -> None:
        self._parent = parent
        self._op_factory = op_factory
        self._rt = return_type_cls
        self._is_map = is_map
        self._bin_name = bin_name
        self._ctx: tuple[Any, ...] = tuple(ctx)
        self._to_ctx = to_ctx

    # -- Internal helpers -----------------------------------------------------

    def _emit(self, return_type: Any) -> T:
        op = self._op_factory(return_type)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def _require_map(self, method: str) -> None:
        if not self._is_map:
            raise TypeError(f"{method}() is only supported for map operations")

    def _push_ctx(self) -> tuple[str, tuple[Any, ...], list[Any]]:
        """Extend context by pushing the current selector.

        Returns ``(bin_name, new_ctx_tuple, new_ctx_list)`` for use by
        navigation methods.  Raises if navigation is not supported.
        """
        if self._to_ctx is None:
            raise TypeError("This builder does not support further navigation")
        new_ctx = self._ctx + (self._to_ctx(),)
        return self._bin_name, new_ctx, list(new_ctx)

    def _context_list_for_nested_ops(self) -> list[Any]:
        """CDT context for collection-level ops at the current selection.

        First-hop builders from a bin store only ``_to_ctx`` until a deeper
        navigation pushes it into ``_ctx``. Terminals like ``map_clear`` or
        ``map_size`` must include both.
        """
        out = list(self._ctx)
        if self._to_ctx is not None:
            out.append(self._to_ctx())
        return out

    def _build_navigated(
        self, *, op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls, is_map: bool,
        ctx: Sequence[Any], to_ctx: Callable[[], Any],
        **_extra: Any,
    ) -> CdtReadBuilder[T]:
        """Create a navigated builder of the same flavor.

        Subclasses override to preserve their builder type and extra
        fields (e.g. ``remove_factory``, ``set_to_factory``).
        """
        return CdtReadBuilder(
            self._parent, op_factory, rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx,
        )

    # -- Singular CDT navigation (deeper nesting) ----------------------------

    def on_map_key(self, key: Any) -> CdtReadBuilder[T]:
        """Navigate into a map element by key."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_key(b, key, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_key(b, key, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_key(key),
        )

    def on_map_index(self, index: int) -> CdtReadBuilder[T]:
        """Navigate into a map element by index."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_index(b, index, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_index(b, index, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_rank(self, rank: int) -> CdtReadBuilder[T]:
        """Navigate into a map element by rank (0 = lowest value)."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_rank(b, rank, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_rank(b, rank, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_rank(rank),
        )

    def on_list_index(self, index: int) -> CdtReadBuilder[T]:
        """Navigate into a list element by index."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: ListOperation.get_by_index(b, index, rt).set_context(ctx_l),
            remove_factory=lambda rt: ListOperation.remove_by_index(b, index, rt).set_context(ctx_l),
            rt_cls=ListReturnType, is_map=False,
            ctx=new_ctx, to_ctx=lambda: CTX.list_index(index),
        )

    def on_list_rank(self, rank: int) -> CdtReadBuilder[T]:
        """Navigate into a list element by rank (0 = lowest value)."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: ListOperation.get_by_rank(b, rank, rt).set_context(ctx_l),
            remove_factory=lambda rt: ListOperation.remove_by_rank(b, rank, rt).set_context(ctx_l),
            rt_cls=ListReturnType, is_map=False,
            ctx=new_ctx, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- Terminal read methods ------------------------------------------------

    def get_values(self) -> T:
        return self._emit(self._rt.VALUE)

    def get_keys(self) -> T:
        self._require_map("get_keys")
        return self._emit(self._rt.KEY)

    def get_keys_and_values(self) -> T:
        self._require_map("get_keys_and_values")
        return self._emit(self._rt.KEY_VALUE)

    def count(self) -> T:
        return self._emit(self._rt.COUNT)

    def get_indexes(self) -> T:
        return self._emit(self._rt.INDEX)

    def get_reverse_indexes(self) -> T:
        return self._emit(self._rt.REVERSE_INDEX)

    def get_ranks(self) -> T:
        return self._emit(self._rt.RANK)

    def get_reverse_ranks(self) -> T:
        return self._emit(self._rt.REVERSE_RANK)

    def exists(self) -> T:
        """Check whether the selected CDT element(s) exist."""
        return self._emit(self._rt.EXISTS)

    def map_size(self) -> T:
        """Return the element count of the map at the current CDT path."""
        op = MapOperation.size(self._bin_name).set_context(
            self._context_list_for_nested_ops(),
        )
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_size(self) -> T:
        """Return the element count of the list at the current CDT path."""
        op = ListOperation.size(self._bin_name).set_context(
            self._context_list_for_nested_ops(),
        )
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent


class CdtReadInvertableBuilder(CdtReadBuilder[T]):
    """Terminal read actions with inverted (all-others) variants.

    Used for range and list selectors where INVERTED makes semantic sense
    (e.g. "all keys *except* those in this range").  Does not support
    further navigation.
    """

    # -- Inverted terminal methods --------------------------------------------

    def get_all_other_values(self) -> T:
        return self._emit(self._rt.VALUE | self._rt.INVERTED)

    def get_all_other_keys(self) -> T:
        self._require_map("get_all_other_keys")
        return self._emit(self._rt.KEY | self._rt.INVERTED)

    def get_all_other_keys_and_values(self) -> T:
        self._require_map("get_all_other_keys_and_values")
        return self._emit(self._rt.KEY_VALUE | self._rt.INVERTED)

    def count_all_others(self) -> T:
        return self._emit(self._rt.COUNT | self._rt.INVERTED)

    def get_all_other_indexes(self) -> T:
        return self._emit(self._rt.INDEX | self._rt.INVERTED)

    def get_all_other_reverse_indexes(self) -> T:
        return self._emit(self._rt.REVERSE_INDEX | self._rt.INVERTED)

    def get_all_other_ranks(self) -> T:
        return self._emit(self._rt.RANK | self._rt.INVERTED)

    def get_all_other_reverse_ranks(self) -> T:
        return self._emit(self._rt.REVERSE_RANK | self._rt.INVERTED)
