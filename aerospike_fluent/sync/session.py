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
    from aerospike_fluent.sync.operations.batch_delete import SyncBatchDeleteOperation
    from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation
    from aerospike_fluent.sync.operations.query import SyncQueryBuilder
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

    def key_value(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key: Optional[Union[str, int, bytes, Key]] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> "SyncKeyValueOperation":
        """Create a key-value operation builder (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        # Support key_value as alias for key
        if key_value is not None and key is None:
            key = key_value

        # Handle Key object - extract namespace, set_name, and key value
        if isinstance(key, Key):
            namespace = key.namespace
            set_name = key.set_name
            key = key.value
        elif dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        
        if namespace is None or set_name is None or key is None:
            raise ValueError("namespace, set_name, and key must be provided")
        
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=namespace,
            set_name=set_name,
            key=key,
            loop_manager=self._loop_manager,
        )

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

    # Convenience methods
    @typing.overload
    def upsert(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create an upsert operation builder for a single Key."""
        ...

    @typing.overload
    def upsert(
        self,
        keys: List[Key],
    ) -> "SyncKeyValueOperation":
        """Create an upsert operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def upsert(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncKeyValueOperation":
        """Create an upsert operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

    def upsert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for upsert operations (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_op = self._async_session.upsert(
            arg1=arg1,
            arg2=arg2,
            *keys,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=async_op._namespace,
            set_name=async_op._set_name,
            key=async_op._key,
            loop_manager=self._loop_manager,
            write_policy=async_op._write_policy,
        )

    @typing.overload
    def insert(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create an insert operation builder for a single Key."""
        ...

    @typing.overload
    def insert(
        self,
        keys: List[Key],
    ) -> "SyncKeyValueOperation":
        """Create an insert operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def insert(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncKeyValueOperation":
        """Create an insert operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

    def insert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for insert operations (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_op = self._async_session.insert(
            arg1=arg1,
            arg2=arg2,
            *keys,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=async_op._namespace,
            set_name=async_op._set_name,
            key=async_op._key,
            loop_manager=self._loop_manager,
            write_policy=async_op._write_policy,
        )

    @typing.overload
    def update(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create an update operation builder for a single Key."""
        ...

    @typing.overload
    def update(
        self,
        keys: List[Key],
    ) -> "SyncKeyValueOperation":
        """Create an update operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def update(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncKeyValueOperation":
        """Create an update operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

    def update(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for update operations (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_op = self._async_session.update(
            arg1=arg1,
            arg2=arg2,
            *keys,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=async_op._namespace,
            set_name=async_op._set_name,
            key=async_op._key,
            loop_manager=self._loop_manager,
            write_policy=async_op._write_policy,
        )

    @typing.overload
    def replace(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create a replace operation builder for a single Key."""
        ...

    @typing.overload
    def replace(
        self,
        keys: List[Key],
    ) -> "SyncKeyValueOperation":
        """Create a replace operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def replace(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncKeyValueOperation":
        """Create a replace operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

    def replace(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for replace operations (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_op = self._async_session.replace(
            arg1=arg1,
            arg2=arg2,
            *keys,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=async_op._namespace,
            set_name=async_op._set_name,
            key=async_op._key,
            loop_manager=self._loop_manager,
            write_policy=async_op._write_policy,
        )

    @typing.overload
    def replace_if_exists(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create a replace-if-exists operation builder for a single Key."""
        ...

    @typing.overload
    def replace_if_exists(
        self,
        keys: List[Key],
    ) -> "SyncKeyValueOperation":
        """Create a replace-if-exists operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def replace_if_exists(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncKeyValueOperation":
        """Create a replace-if-exists operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

    def replace_if_exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for replace-if-exists operations (synchronous)."""
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_op = self._async_session.replace_if_exists(
            arg1=arg1,
            arg2=arg2,
            *keys,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )
        return SyncKeyValueOperation(
            async_client=self._async_session._client,
            namespace=async_op._namespace,
            set_name=async_op._set_name,
            key=async_op._key,
            loop_manager=self._loop_manager,
            write_policy=async_op._write_policy,
        )

    @typing.overload
    def delete(
        self,
        key: Key,
    ) -> "SyncKeyValueOperation":
        """Create a delete operation builder for a single Key."""
        ...

    @typing.overload
    def delete(
        self,
        keys: List[Key],
    ) -> "SyncBatchDeleteOperation":
        """Create a batch delete operation builder for multiple Keys."""
        ...

    @typing.overload
    def delete(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "SyncBatchDeleteOperation":
        """Create a batch delete operation builder for multiple Keys (varargs)."""
        ...

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
    ) -> Union["SyncKeyValueOperation", "SyncBatchDeleteOperation"]:
        """Convenience method for delete operations (synchronous)."""
        from aerospike_fluent.sync.operations.batch_delete import SyncBatchDeleteOperation
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        async_result = self._async_session.delete(
            arg1=arg1,
            arg2=arg2,
            *keys,
            key=key,
            dataset=dataset,
            namespace=namespace,
            set_name=set_name,
            key_value=key_value,
        )

        # Check if it's a BatchDeleteOperation
        if hasattr(async_result, '_keys'):  # BatchDeleteOperation has _keys
            return SyncBatchDeleteOperation(
                async_client=self._async_session._client,
                keys=async_result._keys,
                loop_manager=self._loop_manager,
            )
        else:  # KeyValueOperation
            return SyncKeyValueOperation(
                async_client=self._async_session._client,
                namespace=async_result._namespace,
                set_name=async_result._set_name,
                key=async_result._key,
                loop_manager=self._loop_manager,
                write_policy=async_result._write_policy,
            )

    def touch(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key: Optional[Union[str, int, bytes]] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> "SyncKeyValueOperation":
        """Convenience method for touch operations (synchronous)."""
        return self.key_value(
            namespace=namespace,
            set_name=set_name,
            key=key,
            dataset=dataset,
        )

    def exists(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key: Optional[Union[str, int, bytes]] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> bool:
        """Check if a record exists (synchronous)."""
        async def _exists():
            return await self._async_session.exists(
                namespace=namespace,
                set_name=set_name,
                key=key,
                dataset=dataset,
            )

        return self._loop_manager.run_async(_exists())

