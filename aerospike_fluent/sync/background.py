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

"""Synchronous wrappers for background dataset task builders."""

from __future__ import annotations

from typing import Any, Union, overload

from aerospike_async import ExecuteTask, FilterExpression

from aerospike_fluent.aio.background import (
    BackgroundOperationBuilder as AsyncBackgroundOperationBuilder,
    BackgroundTaskSession as AsyncBackgroundTaskSession,
    BackgroundUdfBuilder as AsyncBackgroundUdfBuilder,
    BackgroundUdfFunctionBuilder as AsyncBackgroundUdfFunctionBuilder,
    BackgroundWriteBinBuilder as AsyncBackgroundWriteBinBuilder,
)
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.sync.client import _EventLoopManager


class SyncBackgroundWriteBinBuilder:
    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundWriteBinBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def set_to(self, value: Any) -> SyncBackgroundOperationBuilder:
        self._inner.set_to(value)
        return SyncBackgroundOperationBuilder(
            self._inner._parent, self._loop_manager)

    def add(self, value: Any) -> SyncBackgroundOperationBuilder:
        self._inner.add(value)
        return SyncBackgroundOperationBuilder(
            self._inner._parent, self._loop_manager)


class SyncBackgroundOperationBuilder:
    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundOperationBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    @overload
    def where(self, expression: str) -> SyncBackgroundOperationBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncBackgroundOperationBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncBackgroundOperationBuilder:
        self._inner.where(expression)
        return self

    def bin(self, name: str) -> SyncBackgroundWriteBinBuilder:
        return SyncBackgroundWriteBinBuilder(
            self._inner.bin(name), self._loop_manager)

    def expire_record_after_seconds(self, seconds: int) -> SyncBackgroundOperationBuilder:
        self._inner.expire_record_after_seconds(seconds)
        return self

    def records_per_second(self, rps: int) -> SyncBackgroundOperationBuilder:
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> SyncBackgroundOperationBuilder:
        self._inner.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncBackgroundOperationBuilder:
        self._inner.respond_all_keys()
        return self

    def execute(self) -> ExecuteTask:
        inner = self._inner

        async def _run() -> ExecuteTask:
            return await inner.execute()

        return self._loop_manager.run_async(_run())


class SyncBackgroundUdfFunctionBuilder:
    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundUdfFunctionBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def function(
        self,
        package_name: str,
        function_name: str,
    ) -> SyncBackgroundUdfBuilder:
        b = self._inner.function(package_name, function_name)
        return SyncBackgroundUdfBuilder(b, self._loop_manager)


class SyncBackgroundUdfBuilder:
    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundUdfBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def passing(self, *args: Any) -> SyncBackgroundUdfBuilder:
        self._inner.passing(*args)
        return self

    @overload
    def where(self, expression: str) -> SyncBackgroundUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncBackgroundUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncBackgroundUdfBuilder:
        self._inner.where(expression)
        return self

    def records_per_second(self, rps: int) -> SyncBackgroundUdfBuilder:
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> SyncBackgroundUdfBuilder:
        self._inner.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncBackgroundUdfBuilder:
        self._inner.respond_all_keys()
        return self

    def execute(self) -> ExecuteTask:
        inner = self._inner

        async def _run() -> ExecuteTask:
            return await inner.execute()

        return self._loop_manager.run_async(_run())


class SyncBackgroundTaskSession:
    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundTaskSession,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def update(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        b = self._inner.update(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def delete(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        b = self._inner.delete(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def touch(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        b = self._inner.touch(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def execute_udf(self, dataset: DataSet) -> SyncBackgroundUdfFunctionBuilder:
        b = self._inner.execute_udf(dataset)
        return SyncBackgroundUdfFunctionBuilder(b, self._loop_manager)
