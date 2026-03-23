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

"""SyncSession - Synchronous wrapper for Session."""

from __future__ import annotations

import typing
from typing import Dict, List, Optional, overload, Union

from aerospike_async import Key

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.session import Session as AsyncSession
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.policy.behavior import Behavior
from aerospike_fluent.sync.client import _EventLoopManager

if typing.TYPE_CHECKING:
    from aerospike_fluent.sync.background import SyncBackgroundTaskSession
    from aerospike_fluent.sync.operations.query import SyncQueryBuilder, SyncWriteSegmentBuilder
    from aerospike_fluent.sync.operations.index import SyncIndexBuilder
    from aerospike_fluent.sync.info import SyncInfoCommands


class SyncSession:
    """
    Synchronous wrapper for Session.

    Provides the same API as the async Session but executes operations
    synchronously by running them on an event loop.
    """

    def __init__(self, async_session: AsyncSession, loop_manager: _EventLoopManager) -> None:
        """
        Initialize a SyncSession.

        Args:
            async_session: The async Session to wrap.
            loop_manager: The event loop manager for executing async operations.
        """
        self._async_session = async_session
        self._loop_manager = loop_manager

    @property
    def behavior(self) -> Behavior:
        """Get the behavior configuration for this session."""
        return self._async_session.behavior

    @property
    def client(self) -> FluentClient:
        """Get the underlying FluentClient."""
        return self._async_session.client

    def _build_write_segment(
        self,
        op_type: str,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *more_keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Delegate to async session's write segment builder and wrap in sync."""
        from aerospike_fluent.sync.operations.query import SyncWriteSegmentBuilder

        wsb = self._async_session._build_write_segment(
            op_type, arg1, arg2, *more_keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[str] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
    ) -> "SyncQueryBuilder":
        """Create a query builder (synchronous)."""
        from aerospike_fluent.sync.operations.query import SyncQueryBuilder

        # Delegate to async session.query() - pass positional args as positional, keyword args as keyword
        if arg1 is not None or arg2 is not None or keys:
            # Has positional arguments - pass them positionally
            async_builder = self._async_session.query(arg1, arg2, *keys)
        else:
            # Only keyword arguments
            async_builder = self._async_session.query(
                namespace=namespace,
                set_name=set_name,
                dataset=dataset,
                key=key,
            )
        return SyncQueryBuilder(
            async_client=self._async_session._client,
            namespace=async_builder._namespace,
            set_name=async_builder._set_name,
            loop_manager=self._loop_manager,
            query_builder=async_builder,
        )

    def background_task(self) -> "SyncBackgroundTaskSession":
        """Synchronous entry point for server-side background dataset operations."""
        from aerospike_fluent.sync.background import SyncBackgroundTaskSession

        inner = self._async_session.background_task()
        return SyncBackgroundTaskSession(inner, self._loop_manager)

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> "SyncIndexBuilder":
        """Create an index builder (synchronous)."""
        from aerospike_fluent.sync.operations.index import SyncIndexBuilder

        # Resolve namespace and set_name from dataset if provided
        if dataset:
            namespace = dataset.namespace
            set_name = dataset.set_name

        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")

        return SyncIndexBuilder(
            async_client=self._async_session._client,
            namespace=namespace,
            set_name=set_name,
            loop_manager=self._loop_manager,
        )

    def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """Truncate (delete all records) from a set (synchronous)."""
        async def _truncate():
            await self._async_session.truncate(dataset, before_nanos)

        self._loop_manager.run_async(_truncate())

    @overload
    def info(self) -> "SyncInfoCommands": ...

    @overload
    def info(self, command: str) -> Dict[str, str]: ...

    def info(
        self, command: Optional[str] = None
    ) -> Union["SyncInfoCommands", Dict[str, str]]:
        """
        Execute info commands or get the SyncInfoCommands helper (synchronous).

        With no argument, returns SyncInfoCommands for high-level helpers and
        info_on_all_nodes(). With a command string, runs the raw info command
        and returns its result.

        Example:
            ```python
            response = session.info("sindex-list")
            info = session.info()
            by_node = info.info_on_all_nodes("build")
            ```
        """
        if command is not None:
            async def _info():
                return await self._async_session.info(command)
            return self._loop_manager.run_async(_info())
        from aerospike_fluent.sync.info import SyncInfoCommands
        return SyncInfoCommands(self._async_session.info(), self._loop_manager)

    def upsert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create an upsert write segment (synchronous)."""
        return self._build_write_segment(
            "upsert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def insert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create an insert write segment (synchronous)."""
        return self._build_write_segment(
            "insert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def update(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create an update write segment (synchronous)."""
        return self._build_write_segment(
            "update", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create a replace write segment (synchronous)."""
        return self._build_write_segment(
            "replace", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace_if_exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create a replace-if-exists write segment (synchronous)."""
        return self._build_write_segment(
            "replace_if_exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def delete(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create a delete write segment (synchronous)."""
        return self._build_write_segment(
            "delete", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def touch(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create a touch write segment (synchronous)."""
        return self._build_write_segment(
            "touch", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncWriteSegmentBuilder":
        """Create an exists-check write segment (synchronous)."""
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
