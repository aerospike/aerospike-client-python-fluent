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

"""Fluent builders for server-side background operations on datasets."""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Union, overload

from aerospike_async import (
    Client,
    ExecuteTask,
    FilterExpression,
    Operation,
    RecordExistsAction,
)

from aerospike_fluent.background_shared import (
    dataset_statement,
    make_background_write_policy,
    reject_unsupported_background_write_ops,
)
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.exceptions import convert_pac_exception

if TYPE_CHECKING:
    from aerospike_fluent.aio.session import Session


class _OpType(enum.Enum):
    UPDATE = enum.auto()
    DELETE = enum.auto()
    TOUCH = enum.auto()


_BG_UNSUPPORTED = (
    "fail_on_filtered_out and respond_all_keys apply to foreground reads; "
    "they are not supported for background tasks."
)


class BackgroundTaskSession:
    """Entry point for server-side background operations on datasets."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def update(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Background update: apply bin writes to matching records."""
        return BackgroundOperationBuilder(self._session, dataset, _OpType.UPDATE)

    def delete(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Background delete: remove matching records."""
        return BackgroundOperationBuilder(self._session, dataset, _OpType.DELETE)

    def touch(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Background touch: reset TTL on matching records."""
        return BackgroundOperationBuilder(self._session, dataset, _OpType.TOUCH)

    def execute_udf(self, dataset: DataSet) -> BackgroundUdfFunctionBuilder:
        """Background UDF: call :meth:`BackgroundUdfFunctionBuilder.function` before execute."""
        return BackgroundUdfFunctionBuilder(self._session, dataset)


class BackgroundWriteBinBuilder:
    """Scalar bin write for a background update (put / add only)."""

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: BackgroundOperationBuilder, bin_name: str) -> None:
        self._parent = parent
        self._bin = bin_name

    def set_to(self, value: Any) -> BackgroundOperationBuilder:
        self._parent._operations.append(Operation.put(self._bin, value))
        return self._parent

    def add(self, value: Any) -> BackgroundOperationBuilder:
        self._parent._operations.append(Operation.add(self._bin, value))
        return self._parent


class BackgroundOperationBuilder:
    """Configure and run a background ``query_operate`` job."""

    __slots__ = (
        "_session",
        "_dataset",
        "_op_type",
        "_operations",
        "_filter_expression",
        "_ttl_seconds",
        "_records_per_second",
    )

    def __init__(
        self,
        session: Session,
        dataset: DataSet,
        op_type: _OpType,
    ) -> None:
        self._session = session
        self._dataset = dataset
        self._op_type = op_type
        self._operations: List[Any] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._ttl_seconds: Optional[int] = None
        self._records_per_second: Optional[int] = None

    @overload
    def where(self, expression: str) -> BackgroundOperationBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundOperationBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> BackgroundOperationBuilder:
        if isinstance(expression, str):
            self._filter_expression = parse_dsl(expression)
        else:
            self._filter_expression = expression
        return self

    def bin(self, name: str) -> BackgroundWriteBinBuilder:
        return BackgroundWriteBinBuilder(self, name)

    def expire_record_after_seconds(self, seconds: int) -> BackgroundOperationBuilder:
        self._ttl_seconds = seconds
        return self

    def records_per_second(self, rps: int) -> BackgroundOperationBuilder:
        # Stored for API parity; PAC background paths currently take WritePolicy only
        # (no QueryPolicy.records_per_second on query_operate / query_execute_udf).
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundOperationBuilder:
        raise TypeError(_BG_UNSUPPORTED)

    def respond_all_keys(self) -> BackgroundOperationBuilder:
        raise TypeError(_BG_UNSUPPORTED)

    def _pac_client(self) -> Client:
        fc = self._session.client
        if fc._client is None:
            raise RuntimeError("Client is not connected")
        return fc._client

    def _final_operations(self) -> List[Any]:
        ops = list(self._operations)
        if self._op_type is _OpType.DELETE:
            if not ops:
                ops = [Operation.delete()]
        elif self._op_type is _OpType.TOUCH:
            if not ops:
                ops = [Operation.touch()]
        elif self._op_type is _OpType.UPDATE:
            if not ops:
                raise ValueError(
                    "Background update requires at least one bin operation; "
                    "use .bin(name).set_to(...) or .add(...).",
                )
        return ops

    def _record_exists_action(self) -> Optional[RecordExistsAction]:
        if self._op_type is _OpType.UPDATE:
            return RecordExistsAction.UPDATE_ONLY
        if self._op_type is _OpType.TOUCH:
            return RecordExistsAction.UPDATE_ONLY
        return None

    async def execute(self) -> ExecuteTask:
        ops = self._final_operations()
        reject_unsupported_background_write_ops(ops)
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            self._ttl_seconds,
            self._record_exists_action(),
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        client = self._pac_client()
        try:
            return await client.query_operate(wp, statement, ops)
        except Exception as e:
            raise convert_pac_exception(e) from e


class BackgroundUdfFunctionBuilder:
    """Requires :meth:`function` before building arguments or executing."""

    __slots__ = ("_session", "_dataset")

    def __init__(self, session: Session, dataset: DataSet) -> None:
        self._session = session
        self._dataset = dataset

    def function(
        self,
        package_name: str,
        function_name: str,
    ) -> BackgroundUdfBuilder:
        if not package_name:
            raise ValueError("package_name must be a non-empty string")
        if not function_name:
            raise ValueError("function_name must be a non-empty string")
        return BackgroundUdfBuilder(
            self._session,
            self._dataset,
            package_name,
            function_name,
        )


class BackgroundUdfBuilder:
    """Background ``query_execute_udf`` configuration and execution."""

    __slots__ = (
        "_session",
        "_dataset",
        "_package_name",
        "_function_name",
        "_args",
        "_filter_expression",
        "_records_per_second",
    )

    def __init__(
        self,
        session: Session,
        dataset: DataSet,
        package_name: str,
        function_name: str,
    ) -> None:
        self._session = session
        self._dataset = dataset
        self._package_name = package_name
        self._function_name = function_name
        self._args: Optional[List[Any]] = None
        self._filter_expression: Optional[FilterExpression] = None
        self._records_per_second: Optional[int] = None

    def passing(self, *args: Any) -> BackgroundUdfBuilder:
        self._args = list(args)
        return self

    @overload
    def where(self, expression: str) -> BackgroundUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> BackgroundUdfBuilder:
        if isinstance(expression, str):
            self._filter_expression = parse_dsl(expression)
        else:
            self._filter_expression = expression
        return self

    def records_per_second(self, rps: int) -> BackgroundUdfBuilder:
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundUdfBuilder:
        raise TypeError(_BG_UNSUPPORTED)

    def respond_all_keys(self) -> BackgroundUdfBuilder:
        raise TypeError(_BG_UNSUPPORTED)

    def _pac_client(self) -> Client:
        fc = self._session.client
        if fc._client is None:
            raise RuntimeError("Client is not connected")
        return fc._client

    async def execute(self) -> ExecuteTask:
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            None,
            None,
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        client = self._pac_client()
        py_args: Optional[List[Any]] = (
            list(self._args) if self._args is not None else None
        )
        try:
            return await client.query_execute_udf(
                wp,
                statement,
                self._package_name,
                self._function_name,
                py_args,
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
