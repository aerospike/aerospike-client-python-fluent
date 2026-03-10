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

"""CDT action builders for write contexts (read + remove terminals).

Extends the read-only builders from ``cdt_read`` with ``remove()`` and
inverted ``remove_all_others()`` terminals.  Used by ``WriteBinBuilder``
when CDT navigation is performed inside a write segment.
"""

from __future__ import annotations

from typing import Any, Callable

from aerospike_fluent.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
    T,
    _ReturnTypeCls,
)


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
    """CDT action builder with read *and* remove terminals.

    Inherits all read terminals (get_values, count, exists, …) from
    ``CdtReadBuilder`` and adds ``remove()``.
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


class CdtWriteInvertableBuilder(_RemoveMixin, CdtReadInvertableBuilder[T]):
    """CDT action builder with read, inverted-read, and remove terminals.

    Inherits all read and inverted-read terminals from
    ``CdtReadInvertableBuilder`` and adds ``remove()`` /
    ``remove_all_others()``.
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
