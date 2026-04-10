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

"""Synchronous :class:`~aerospike_sdk.aio.session.Session` wrapper."""

from __future__ import annotations

from typing import Dict, List, Optional, overload, Union

from aerospike_async import Key

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.session import Session as AsyncSession
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.background import SyncBackgroundTaskSession
from aerospike_sdk.sync.client import _EventLoopManager
from aerospike_sdk.sync.info import SyncInfoCommands
from aerospike_sdk.sync.operations.batch import SyncBatchOperationBuilder
from aerospike_sdk.sync.operations.index import SyncIndexBuilder
from aerospike_sdk.sync.operations.query import SyncQueryBuilder, SyncWriteSegmentBuilder
from aerospike_sdk.sync.operations.udf import SyncUdfFunctionBuilder


class SyncSession:
    """Run session-scoped reads and writes without ``async``/``await``.

    Constructed by :meth:`SyncClient.create_session
    <aerospike_sdk.sync.client.SyncClient.create_session>`, not by
    calling ``SyncSession(...)`` directly. Each method delegates to
    :class:`~aerospike_sdk.aio.session.Session` on a shared per-thread loop;
    return types are sync wrappers where the async API would return a coroutine
    or async stream.

    See Also:
        :class:`~aerospike_sdk.aio.session.Session`: Async API and behavior
            semantics.
    """

    def __init__(self, async_session: AsyncSession, loop_manager: _EventLoopManager) -> None:
        """Wrap ``async_session``; use :meth:`SyncClient.create_session` instead.

        Args:
            async_session: Connected async session (same behavior binding).
            loop_manager: Loop manager shared with the parent
                :class:`~aerospike_sdk.sync.client.SyncClient`.
        """
        self._async_session = async_session
        self._loop_manager = loop_manager

    @property
    def behavior(self) -> Behavior:
        """Get the behavior configuration for this session."""
        return self._async_session.behavior

    @property
    def client(self) -> Client:
        """Get the underlying Client."""
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
        wsb = self._async_session._build_write_segment(
            op_type, arg1, arg2, *more_keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[Union[str, Key]] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
        behavior: Optional[Behavior] = None,
    ) -> "SyncQueryBuilder":
        """Start a read or secondary-index query (synchronous session).

        Same shapes as :meth:`aerospike_sdk.aio.session.Session.query`, with
        this session's behavior applied on the underlying async builder.

        Args:
            arg1: Positional dataset, key, list of keys, or namespace string
                (when paired with ``arg2`` as set name).
            arg2: When ``arg1`` is a namespace, the set name; otherwise may be
                a second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional argument is a key.
            namespace: Keyword namespace (with ``set_name``) when not using a
                dataset.
            set_name: Keyword set name (with ``namespace``).
            dataset: Keyword :class:`~aerospike_sdk.dataset.DataSet`.
            key: Keyword single key.
            keys_list: Keyword list of keys for batch read.
            behavior: Optional per-query behavior override (same as async session).

        Returns:
            A :class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder`.
        """
        # Delegate to async session.query() - pass positional args as positional, keyword args as keyword
        if arg1 is not None or arg2 is not None or keys:
            # Has positional arguments - pass them positionally
            async_builder = self._async_session.query(  # type: ignore[call-overload]
                arg1, arg2, *keys, behavior=behavior,
            )
        else:
            # Only keyword arguments
            async_builder = self._async_session.query(  # type: ignore[call-overload]
                namespace=namespace,
                set_name=set_name,
                dataset=dataset,
                key=key,
                keys_list=keys_list,
                behavior=behavior,
            )
        return SyncQueryBuilder(
            async_client=self._async_session._client,
            namespace=async_builder._namespace,
            set_name=async_builder._set_name,
            loop_manager=self._loop_manager,
            query_builder=async_builder,
        )

    def batch(self) -> "SyncBatchOperationBuilder":
        """Start a multi-key batch of mixed write operations (synchronous).

        Chain ``insert``, ``update``, ``upsert``, ``replace``, ``delete``, and bin
        builders, then :meth:`~aerospike_sdk.sync.operations.batch.SyncBatchOperationBuilder.execute`
        for a :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

        Returns:
            A :class:`~aerospike_sdk.sync.operations.batch.SyncBatchOperationBuilder`.

        Raises:
            RuntimeError: If the client is not connected (from the async session).

        Example::

            stream = (
                session.batch()
                .insert(key1).put({"name": "Alice", "age": 25})
                .update(key2).bin("counter").add(1)
                .execute()
            )
            for row in stream:
                print(row.key, row.result_code)

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.batch`
        """
        inner = self._async_session.batch()
        return SyncBatchOperationBuilder(inner, self._loop_manager)

    def background_task(self) -> "SyncBackgroundTaskSession":
        """Start a background dataset task chain (synchronous).

        Returns:
            :class:`~aerospike_sdk.sync.background.SyncBackgroundTaskSession`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.background_task`.
        """
        inner = self._async_session.background_task()
        return SyncBackgroundTaskSession(inner, self._loop_manager)

    def execute_udf(self, *keys: Key) -> "SyncUdfFunctionBuilder":
        """Begin a foreground UDF invocation on the given keys (synchronous).

        Returns:
            :class:`~aerospike_sdk.sync.operations.udf.SyncUdfFunctionBuilder`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.execute_udf`.
        """
        inner = self._async_session.execute_udf(*keys)
        return SyncUdfFunctionBuilder(
            inner, self._loop_manager, self._async_session.client)

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> "SyncIndexBuilder":
        """Create a secondary-index builder for this namespace/set (synchronous).

        Raises:
            ValueError: If ``namespace`` and ``set_name`` are missing and no
                ``dataset`` is provided.

        Returns:
            :class:`~aerospike_sdk.sync.operations.index.SyncIndexBuilder`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.index`.
        """
        _ = behavior
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

        Example::

                response = session.info("sindex-list")
                info = session.info()
                by_node = info.info_on_all_nodes("build")
        """
        if command is not None:
            async def _info():
                return await self._async_session.info(command)
            return self._loop_manager.run_async(_info())
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
