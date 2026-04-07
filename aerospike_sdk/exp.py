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

"""Expression builder utilities.

Re-exports FilterExpression as Exp and provides a convenience val() function
for creating value expressions from Python values.
"""

from typing import Any, Dict, List, Optional, Union, overload

from aerospike_async import (
    CTX,
    FilterExpression as Exp,
    ListReturnType,
    MapReturnType,
)

__all__ = ["Exp", "val", "in_list", "map_keys", "map_values"]


@overload
def val(value: bool) -> Exp: ...
@overload
def val(value: int) -> Exp: ...
@overload
def val(value: float) -> Exp: ...
@overload
def val(value: str) -> Exp: ...
@overload
def val(value: bytes) -> Exp: ...
@overload
def val(value: bytearray) -> Exp: ...
@overload
def val(value: List[Any]) -> Exp: ...
@overload
def val(value: Dict[Any, Any]) -> Exp: ...
@overload
def val(value: None) -> Exp: ...


def val(value: Any) -> Exp:
    """Create a value expression from a Python value.

    Automatically dispatches to the appropriate FilterExpression method
    based on the value's type.

    Args:
        value: A Python value (bool, int, float, str, bytes, list, dict, or None)

    Returns:
        A FilterExpression representing the value.

    Raises:
        TypeError: If the value type is not supported.
    """
    if isinstance(value, bool):
        return Exp.bool_val(value)
    elif isinstance(value, int):
        return Exp.int_val(value)
    elif isinstance(value, float):
        return Exp.float_val(value)
    elif isinstance(value, str):
        return Exp.string_val(value)
    elif isinstance(value, (bytes, bytearray)):
        return Exp.blob_val(value)
    elif isinstance(value, list):
        return Exp.list_val(value)
    elif isinstance(value, dict):
        return Exp.map_val(value)
    elif value is None:
        return Exp.nil()
    raise TypeError(f"Unsupported type for val(): {type(value)}")


def in_list(
    value: Exp,
    list_exp: Exp,
    ctx: Optional[List[CTX]] = None,
) -> Exp:
    """Check whether *value* exists in *list_exp*.

    Returns a boolean expression equivalent to ``value IN list``.

    Args:
        value: The value to search for.
        list_exp: A list expression (e.g. ``Exp.list_bin("tags")``).
        ctx: Optional CDT context for nested lists.

    Example::

        # bin "role" in list bin "allowed_roles"
        in_list(Exp.string_bin("role"), Exp.list_bin("allowed_roles"))
    """
    return Exp.gt(
        Exp.list_get_by_value(
            ListReturnType.COUNT,
            value,
            list_exp,
            ctx or [],
        ),
        Exp.int_val(0),
    )


def map_keys(
    map_exp: Exp,
    ctx: Optional[List[CTX]] = None,
) -> Exp:
    """Return all keys of *map_exp* as a list expression.

    Args:
        map_exp: A map expression (e.g. ``Exp.map_bin("scores")``).
        ctx: Optional CDT context for nested maps.

    Example::

        map_keys(Exp.map_bin("scores"))
    """
    return Exp.map_get_by_index_range(
        MapReturnType.KEY,
        Exp.int_val(0),
        map_exp,
        ctx or [],
    )


def map_values(
    map_exp: Exp,
    ctx: Optional[List[CTX]] = None,
) -> Exp:
    """Return all values of *map_exp* as a list expression.

    Args:
        map_exp: A map expression (e.g. ``Exp.map_bin("scores")``).
        ctx: Optional CDT context for nested maps.

    Example::

        map_values(Exp.map_bin("scores"))
    """
    return Exp.map_get_by_index_range(
        MapReturnType.VALUE,
        Exp.int_val(0),
        map_exp,
        ctx or [],
    )
