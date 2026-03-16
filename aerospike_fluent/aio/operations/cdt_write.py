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
- Nested navigation that preserves write capability through the chain
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from aerospike_async import (
    CTX,
    MapOperation,
    MapPolicy,
    MapReturnType,
)

from aerospike_fluent.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
    T,
    _ReturnTypeCls,
)

_DEFAULT_MAP_POLICY = MapPolicy(None, None)


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
        """Remove the selected CDT element(s)."""
        return self._emit_remove(self._rt.NONE)


class CdtWriteBuilder(_RemoveMixin, CdtReadBuilder[T]):
    """CDT action builder with read, remove, and write terminals.

    Inherits all read terminals and navigation from ``CdtReadBuilder``.
    Adds ``remove()``, and on map-key paths ``set_to()`` / ``add()``.
    Navigation returns ``CdtWriteBuilder`` to preserve write capability.
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
        """Navigate into a map element by key (supports set_to / add)."""
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
        """Set the value at the current CDT path."""
        if self._set_to_factory is None:
            raise TypeError(
                "set_to() requires on_map_key() navigation"
            )
        op = self._set_to_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def add(self, value: Any) -> T:
        """Increment the value at the current CDT path."""
        if self._add_factory is None:
            raise TypeError(
                "add() requires on_map_key() navigation"
            )
        op = self._add_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent


class CdtWriteInvertableBuilder(_RemoveMixin, CdtReadInvertableBuilder[T]):
    """CDT action builder with read, inverted-read, and remove terminals.

    Inherits all read and inverted-read terminals from
    ``CdtReadInvertableBuilder`` and adds ``remove()`` /
    ``remove_all_others()``.  Does not support further navigation.
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
        """Remove all CDT elements *except* the selected ones."""
        return self._emit_remove(self._rt.NONE | self._rt.INVERTED)
