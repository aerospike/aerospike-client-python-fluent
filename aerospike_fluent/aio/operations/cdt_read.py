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

- ``CdtReadBuilder``  -- non-invertable terminals (get_values, count, …).
- ``CdtReadInvertableBuilder`` -- adds inverted "all others" terminals.

Both are generic on the parent builder type ``T`` so terminal methods
return the parent for continued chaining.  The actual CDT operation is
produced by an ``op_factory`` callable injected by the navigation method
that created the builder (Phase 2 – QueryBinBuilder).
"""

from __future__ import annotations

from typing import Any, Callable, Generic, TypeVar, Union

from aerospike_async import ListReturnType, MapReturnType

T = TypeVar("T")

_ReturnTypeCls = Union[type[MapReturnType], type[ListReturnType]]


class CdtReadBuilder(Generic[T]):
    """Terminal read actions for a CDT path.

    The parent (type ``T``) must expose ``add_operation(op)`` so the
    builder can register the produced operation before returning it.
    """

    __slots__ = ("_parent", "_op_factory", "_rt", "_is_map")

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
    ) -> None:
        self._parent = parent
        self._op_factory = op_factory
        self._rt = return_type_cls
        self._is_map = is_map

    def _emit(self, return_type: Any) -> T:
        op = self._op_factory(return_type)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def _require_map(self, method: str) -> None:
        if not self._is_map:
            raise TypeError(f"{method}() is only supported for map operations")

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


class CdtReadInvertableBuilder(CdtReadBuilder[T]):
    """Terminal read actions with inverted (all-others) variants.

    Used for range and list selectors where INVERTED makes semantic sense
    (e.g. "all keys *except* those in this range").
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
