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

"""Session - Main interface for database operations with Behavior configuration."""

from __future__ import annotations

import typing
from typing import Awaitable, Dict, List, Optional, overload, Union

from aerospike_async import Key

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.operations.batch import BatchOperationBuilder
    from aerospike_fluent.aio.operations.query import QueryBuilder, WriteSegmentBuilder
    from aerospike_fluent.aio.operations.index import IndexBuilder
    from aerospike_fluent.aio.transactional_session import TransactionalSession
    from aerospike_fluent.aio.info import InfoCommands


class Session:
    """
    Main interface for performing database operations.

    A Session is created from a FluentClient (Cluster) and is configured
    with a Behavior that defines the policies (timeouts, retries) for all
    operations performed within that session.

    Sessions are lightweight and can be created multiple times from the
    same client, each with different behaviors for different use cases.

    Example::

        async with FluentClient("localhost:3000") as client:
            session = client.create_session(Behavior.DEFAULT)

            rs = await session.query(users.id(1)).execute()
            await session.upsert(users.id(2)).bin("name").set_to("Tim").execute()
    """

    def __init__(self, client: FluentClient, behavior: Behavior) -> None:
        """
        Initialize a Session.

        Args:
            client: The FluentClient (Cluster) to use for operations.
            behavior: The Behavior configuration for this session.

        Note:
            Sessions should be created via FluentClient.create_session(),
            not directly.
        """
        self._client = client
        self._behavior = behavior

    @property
    def behavior(self) -> Behavior:
        """Get the behavior configuration for this session."""
        return self._behavior

    @property
    def client(self) -> FluentClient:
        """Get the underlying FluentClient."""
        return self._client

    # Delegate all FluentClient operations to maintain same API

    def batch(self) -> "BatchOperationBuilder":
        """
        Create a batch operation builder for chaining operations across multiple keys.
        
        This enables fluent chaining of insert, update, upsert, replace, and delete
        operations on different keys, which are executed as a single batch.
        
        Returns:
            A BatchOperationBuilder for chaining operations.
        
        Example:
            ```python
            # Chain multiple operations across different keys
            results = await session.batch() \\
                .insert(key1).bin("name").set_to("Alice").bin("age").set_to(25) \\
                .update(key2).bin("counter").add(1) \\
                .upsert(key3).put({"status": "active"}) \\
                .delete(key4) \\
                .execute()
            
            # Check results
            for result in results:
                print(f"Key: {result.key}, Result: {result.result_code}")
            ```
        """
        from aerospike_fluent.aio.operations.batch import BatchOperationBuilder

        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        return BatchOperationBuilder(self._client._client, self._behavior)

    # -- Internal helpers -----------------------------------------------------

    def _resolve_keys(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *more_keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> List[Key]:
        """Resolve mixed positional/keyword arguments into a flat list of Keys."""
        all_keys: List[Key] = []

        if arg1 is not None:
            if isinstance(arg1, Key):
                all_keys.append(arg1)
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                all_keys.extend(more_keys)
            elif isinstance(arg1, list):
                if not arg1:
                    raise ValueError("keys list cannot be empty")
                all_keys.extend(arg1)
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            all_keys.append(key)
        elif key_value is not None:
            if dataset is not None:
                all_keys.append(dataset.id(key_value))
            elif namespace is not None and set_name is not None:
                all_keys.append(Key(namespace, set_name, key_value))
            else:
                raise ValueError(
                    "Either dataset or (namespace and set_name) must be provided with key_value"
                )

        if not all_keys:
            raise ValueError("At least one key must be provided")
        return all_keys

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
    ) -> WriteSegmentBuilder:
        """Resolve keys and create a :class:`WriteSegmentBuilder`."""
        from aerospike_fluent.aio.operations.query import QueryBuilder

        all_keys = self._resolve_keys(
            arg1, arg2, *more_keys,
            key=key, dataset=dataset,
            namespace=namespace, set_name=set_name, key_value=key_value,
        )
        first = all_keys[0]
        qb = QueryBuilder(
            client=self._client._client,
            namespace=first.namespace,
            set_name=first.set_name,
            behavior=self._behavior,
        )
        target: Union[Key, List[Key]] = all_keys[0] if len(all_keys) == 1 else all_keys
        return qb._start_write_verb(op_type, target)

    # -- Read entry point -----------------------------------------------------

    @typing.overload
    def query(
        self,
        dataset: DataSet,
    ) -> QueryBuilder:
        """Create a query builder from a DataSet."""
        ...

    @typing.overload
    def query(
        self,
        key: Key,
    ) -> QueryBuilder:
        """Create a query builder for a single Key (point read)."""
        ...

    @typing.overload
    def query(
        self,
        keys: List[Key],
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @typing.overload
    def query(
        self,
        *keys: Key,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @typing.overload
    def query(
        self,
        namespace: str,
        set_name: str,
    ) -> QueryBuilder:
        """Create a query builder with explicit namespace/set."""
        ...

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
    ) -> QueryBuilder:
        """
        Create a query builder.

        Supports multiple calling styles:

        1. Positional DataSet: `query(users)`
        2. Positional Key: `query(users.id(1))`
        3. Positional List[Key]: `query(users.ids(1,2,3))`
        4. Varargs Keys: `query(key1, key2, key3)`
        5. Explicit namespace/set: `query("test", "users")`
        6. Keyword arguments (legacy): `query(dataset=users)`, `query(key=key)`, etc.

        See FluentClient.query() for full documentation.
        """
        b = self._behavior
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, DataSet):
                return self._client.query(dataset=arg1, behavior=b)
            elif isinstance(arg1, Key):
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                else:
                    return self._client.query(key=arg1, behavior=b)
                return self._client.query(keys=all_keys, behavior=b)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                return self._client.query(keys=arg1, behavior=b)
            elif isinstance(arg1, str) and arg2 is not None:
                return self._client.query(namespace=arg1, set_name=arg2, behavior=b)

        if keys:
            keys_list = list(keys)
            if arg1 is not None and isinstance(arg1, Key):
                keys_list.insert(0, arg1)
            if arg2 is not None and isinstance(arg2, Key):
                keys_list.insert(1 if arg1 is not None and isinstance(arg1, Key) else 0, arg2)
            return self._client.query(keys=keys_list, behavior=b)

        return self._client.query(
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            key=key,
            behavior=b,
        )

    @typing.overload
    def index(
        self,
        *,
        dataset: DataSet,
    ) -> IndexBuilder:
        """Create an index builder from a DataSet."""
        ...

    @typing.overload
    def index(
        self,
        namespace: str,
        set_name: str,
    ) -> IndexBuilder:
        """Create an index builder with explicit namespace/set."""
        ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> IndexBuilder:
        """
        Create an index builder.

        See FluentClient.index() for full documentation.
        """
        if dataset is not None:
            return self._client.index(dataset=dataset)
        elif namespace is not None and set_name is not None:
            return self._client.index(namespace, set_name)
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - index(dataset=DataSet(...))\n"
                "  - index(namespace=..., set_name=...)"
            )

    def transaction_session(self) -> TransactionalSession:
        """
        Create a transactional session.

        See FluentClient.transaction_session() for full documentation.
        """
        return self._client.transaction_session()

    @overload
    def info(self) -> InfoCommands: ...

    @overload
    def info(self, command: str) -> Awaitable[Dict[str, str]]: ...

    def info(
        self, command: Optional[str] = None
    ) -> Union[InfoCommands, Awaitable[Dict[str, str]]]:
        """
        Execute info commands or get the InfoCommands helper.

        With no argument, returns an InfoCommands instance for high-level
        helpers (namespaces(), namespace_details(), etc.) and for
        info_on_all_nodes().

        With a command string, runs the raw info command and returns its
        result (awaitable).

        Args:
            command: Optional. If given, the raw info command to run
                (e.g. "sindex-list", "build").

        Returns:
            If command is None: InfoCommands instance.
            If command is given: awaitable dict (node -> response).

        Example:
            ```python
            # Raw command (no double .info)
            response = await session.info("sindex-list")

            # High-level helpers
            info = session.info()
            namespaces = await info.namespaces()
            by_node = await info.info_on_all_nodes("build")
            ```
        """
        if command is not None:
            return self._client._async_client.info(command)
        from aerospike_fluent.aio.info import InfoCommands
        return InfoCommands(self)

    async def is_namespace_sc(self, namespace: str) -> bool:
        """
        Check if a namespace is in strong consistency (SC) mode.

        Strong consistency mode provides linearizable reads and writes
        at the cost of availability during network partitions.

        Args:
            namespace: The namespace name to check.

        Returns:
            True if the namespace is in strong consistency mode, False otherwise.

        Raises:
            ValueError: If the namespace is unknown or the info command fails.

        Example:
            ```python
            if await session.is_namespace_sc("test"):
                print("Namespace 'test' is in strong consistency mode")
            else:
                print("Namespace 'test' is in AP (availability) mode")
            ```
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        try:
            # Query namespace configuration via info command
            result = await self._client._client.info(f"namespace/{namespace}")

            # Parse the result - it's a dict with node addresses as keys
            for node_result in result.values():
                # Parse semicolon-separated key=value pairs
                for pair in node_result.split(";"):
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        if key == "strong-consistency":
                            return value.lower() == "true"

            # If we didn't find the strong-consistency key, default to False (AP mode)
            return False

        except Exception as e:
            raise ValueError(f"Failed to check namespace '{namespace}': {e}") from e

    async def do_in_transaction(
        self,
        operation: typing.Callable[[TransactionalSession], typing.Awaitable[typing.Any]],
    ) -> typing.Any:
        """
        Execute an operation within a transaction.

        This is a convenience method that creates a TransactionalSession,
        executes the operation, and handles commit/rollback automatically.

        Args:
            operation: An async function that takes a TransactionalSession
                      and performs operations within it.

        Returns:
            The result of the operation function.

        Example:
            ```python
            async def transfer_funds(tx_session):
                await tx_session.upsert(accounts.id("acc1")).put({"balance": 100}).execute()
                await tx_session.upsert(accounts.id("acc2")).put({"balance": 200}).execute()

            await session.do_in_transaction(transfer_funds)
            ```
        """
        # This will be implemented when TransactionalSession is fully functional
        # For now, delegate to the client's transaction_session
        async with self.transaction_session() as tx_session:
            return await operation(tx_session)

    # -- Write entry points ---------------------------------------------------

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
    ) -> WriteSegmentBuilder:
        """Create an upsert (create or update) write segment.

        Returns a :class:`WriteSegmentBuilder` for chaining bin writes,
        policies, and ``execute()``.

        Example::

            await session.upsert(users.id(1)).bin("name").set_to("Tim").execute()
        """
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
    ) -> WriteSegmentBuilder:
        """Create an insert (create-only) write segment."""
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
    ) -> WriteSegmentBuilder:
        """Create an update (update-only) write segment."""
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
    ) -> WriteSegmentBuilder:
        """Create a replace write segment."""
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
    ) -> WriteSegmentBuilder:
        """Create a replace-if-exists write segment."""
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
    ) -> WriteSegmentBuilder:
        """Create a delete write segment.

        Example::

            await session.delete(users.id(1)).execute()
            await session.delete(users.ids(1, 2, 3)).execute()
        """
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
    ) -> WriteSegmentBuilder:
        """Create a touch (reset TTL) write segment."""
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
    ) -> WriteSegmentBuilder:
        """Create an exists-check write segment.

        Example::

            rs = await session.exists(users.id(1)).execute()
            found = (await rs.first()).as_bool()
        """
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    async def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set.

        This method deletes all records in the specified set.
        This operation cannot be undone.

        Args:
            dataset: The DataSet to truncate.
            before_nanos: Optional timestamp in nanoseconds. Only records with
                         last update time (LUT) less than this value will be
                         truncated. If None, all records in the set are truncated.

        Example:
            ```python
            users = DataSet.of("test", "users")
            await session.truncate(users)

            # Truncate only records older than a specific time
            cutoff_time = time.time_ns() - (24 * 60 * 60 * 10**9)  # 24 hours ago
            await session.truncate(users, before_nanos=cutoff_time)
            ```
        """
        # Access the underlying async client and call its truncate method
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        await self._client._client.truncate(
            dataset.namespace,
            dataset.set_name,
            before_nanos
        )

    def __repr__(self) -> str:
        """String representation of the session."""
        return f"Session(behavior={self._behavior.name!r})"

