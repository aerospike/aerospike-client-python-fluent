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
# License for the specific language governing permissions and limitations
# under the License.

"""Foreground UDF execution builders (single-key, batch, and chained specs)."""

from __future__ import annotations

from typing import Any, List, Union, overload

from aerospike_async import FilterExpression, Key

from aerospike_fluent.aio.operations.query import QueryBuilder, WriteSegmentBuilder
from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.error_strategy import OnError
from aerospike_fluent.record_stream import RecordStream


class UdfFunctionBuilder:
    """Requires :meth:`function` before arguments or execution."""

    __slots__ = ("_qb",)

    def __init__(self, qb: QueryBuilder) -> None:
        self._qb = qb

    def function(self, package: str, function_name: str) -> UdfBuilder:
        if not package:
            raise ValueError("package must be a non-empty string")
        if not function_name:
            raise ValueError("function_name must be a non-empty string")
        self._qb._udf_package = package
        self._qb._udf_function = function_name
        self._qb._udf_args = None
        self._qb._op_type = "udf"
        return UdfBuilder(self._qb)


class UdfBuilder:
    """Configure a UDF spec and chain into reads, writes, or another UDF."""

    __slots__ = ("_qb",)

    def __init__(self, qb: QueryBuilder) -> None:
        self._qb = qb

    def passing(self, *args: Any) -> UdfBuilder:
        self._qb._udf_args = list(args)
        return self

    @overload
    def where(self, expression: str) -> UdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> UdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> UdfBuilder:
        if isinstance(expression, str):
            self._qb._filter_expression = parse_dsl(expression)
        else:
            self._qb._filter_expression = expression
        return self

    def respond_all_keys(self) -> UdfBuilder:
        """Include results for missing keys in the stream.

        Returns:
            self for method chaining.
        """
        self._qb._respond_all_keys = True
        return self

    def execute_udf(self, *keys: Key) -> UdfFunctionBuilder:
        if not keys:
            raise ValueError("At least one key is required")
        self._qb._finalize_udf_spec()
        self._qb._set_current_keys_from_varargs(keys)
        return UdfFunctionBuilder(self._qb)

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> QueryBuilder:
        self._qb._finalize_udf_spec()
        self._qb._op_type = None
        self._qb._set_current_keys(arg1, *more_keys)
        return self._qb

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("exists", arg1, *more_keys)

    async def execute(self, on_error: OnError | None = None) -> RecordStream:
        if self._qb._udf_function is None:
            raise ValueError(
                "function(package, name) must be called before execute()",
            )
        self._qb._finalize_udf_spec()
        return await self._qb.execute(on_error)
