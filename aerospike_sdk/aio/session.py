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

from aerospike_sdk.aio.background import BackgroundTaskSession
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.info import InfoCommands
from aerospike_sdk.aio.operations.batch import BatchOperationBuilder
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import QueryBuilder, WriteSegmentBuilder
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.aio.transactional_session import TransactionalSession
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior


class Session:
    """Perform reads and writes against Aerospike with a fixed :class:`~aerospike_sdk.policy.behavior.Behavior`.

    A session binds a connected :class:`Client` to policy defaults (timeouts,
    retries, replica preferences) for every operation started from it. Create
    sessions with :meth:`Client.create_session`; do not construct
    ``Session`` directly.

    Example:
        async with Client("localhost:3000") as client:
            session = client.create_session(Behavior.DEFAULT)
            users = DataSet.of("test", "users")
            stream = await session.query(users.id(1)).execute()
            first = await stream.first_or_raise()
            await session.upsert(users.id(2)).put({"name": "Tim"}).execute()

    See Also:
        :meth:`Client.create_session`: How to obtain a session.
        :meth:`query`: Point reads, batch reads, and secondary-index queries.
        :meth:`upsert`: Create-or-update writes.
    """

    def __init__(self, client: Client, behavior: Behavior) -> None:
        """Attach a client and behavior; prefer :meth:`Client.create_session`.

        Args:
            client: Connected (or not yet connected) :class:`Client`.
            behavior: Policy bundle for operations from this session.

        Note:
            Application code should not call ``Session(...)`` directly.

        See Also:
            :meth:`Client.create_session`.
        """
        self._client = client
        self._behavior = behavior

    @property
    def behavior(self) -> Behavior:
        """Policy bundle applied to operations created from this session.

        Returns:
            The :class:`~aerospike_sdk.policy.behavior.Behavior` passed to
            :meth:`Client.create_session`.
        """
        return self._behavior

    @property
    def client(self) -> Client:
        """SDK client that owns the connection used by this session.

        Returns:
            The parent :class:`Client`.
        """
        return self._client

    # Delegate all Client operations to maintain same API

    def batch(self) -> "BatchOperationBuilder":
        """Start a multi-key batch of mixed write operations executed in one server round trip.

        Chain ``insert``, ``update``, ``upsert``, ``replace``, ``delete``, and related
        bin builders, then ``await ...execute()`` to obtain per-key outcomes.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.batch.BatchOperationBuilder`
            for chaining operations.

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            results = await (
                session.batch()
                .insert(key1).put({"name": "Alice", "age": 25})
                .update(key2).bin("counter").add(1)
                .upsert(key3).put({"status": "active"})
                .delete(key4)
                .execute()
            )
            for row in results:
                print(row.key, row.result_code)

        See Also:
            :meth:`upsert`: Single-record writes without batching.
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        return BatchOperationBuilder(self._client._client, self._behavior)

    def background_task(self) -> "BackgroundTaskSession":
        """Configure a server-side background job (query + scan scope) on a dataset.

        Call ``update``, ``delete``, ``touch``, or ``execute_udf`` on the returned
        object, add optional filters (for example ``where`` on supported builders),
        then ``await ...execute()`` to start work and receive an async task handle.

        Returns:
            A :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`
            for chaining the operation type and execution.

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            task = await (
                session.background_task()
                .delete(DataSet.of("test", "scratch"))
                .where("$.flag == 1")
                .execute()
            )
            await task.wait_till_complete(sleep_time=0.2, max_attempts=50)

        See Also:
            :meth:`execute_udf`: Foreground UDF on explicit keys.
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        return BackgroundTaskSession(self)

    def execute_udf(self, *keys: Key) -> "UdfFunctionBuilder":
        """Run a registered server-side UDF on one or more keys (foreground).

        Chain ``function(package, name)`` (package is the registered module name
        without ``.lua``), optional ``passing(*args)`` for Lua parameters, optional
        ``where`` for a filter expression, then ``await ...execute()`` to obtain a
        :class:`~aerospike_sdk.record_stream.RecordStream`. Multiple keys use a
        batch UDF; results preserve per-key order where applicable.

        Args:
            *keys: One or more :class:`~aerospike_async.Key` targets in the same
                namespace and set.

        Returns:
            :class:`~aerospike_sdk.aio.operations.udf.UdfFunctionBuilder` —
            call ``function`` next.

        Raises:
            ValueError: If no keys are given.
            RuntimeError: If the client is not connected.

        Example::

            users = DataSet.of("test", "users")
            stream = await (
                session.execute_udf(users.id("a"))
                .function("my_module", "my_fn")
                .passing("binName", 42)
                .execute()
            )
            value = await stream.first_udf_result()

        See Also:
            :meth:`query`: Read bins without UDF.
            :meth:`background_task`: Dataset-scoped background UDF.
        """
        if not keys:
            raise ValueError("At least one key is required")
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        first = keys[0]
        qb = QueryBuilder(
            self._client._client,
            first.namespace,
            first.set_name,
            self._behavior,
            indexes_monitor=self._client._indexes_monitor,
        )
        qb._set_current_keys_from_varargs(keys)
        return UdfFunctionBuilder(qb)

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
            indexes_monitor=self._client._indexes_monitor,
        )
        target: Union[Key, List[Key]] = all_keys[0] if len(all_keys) == 1 else all_keys
        return qb._start_write_verb(op_type, target)

    # -- Read entry point -----------------------------------------------------

    @typing.overload
    def query(
        self,
        dataset: DataSet,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder from a DataSet."""
        ...

    @typing.overload
    def query(
        self,
        key: Key,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for a single Key (point read)."""
        ...

    @typing.overload
    def query(
        self,
        keys: List[Key],
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @typing.overload
    def query(
        self,
        *keys: Key,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @typing.overload
    def query(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder with explicit namespace/set."""
        ...

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
    ) -> QueryBuilder:
        """Start a read or secondary-index query for keys or a whole set.

        This session's :attr:`behavior` is applied to the underlying
        :class:`~aerospike_sdk.aio.operations.query.QueryBuilder`. Supported
        shapes include a :class:`~aerospike_sdk.dataset.DataSet` (set-wide
        query), a single :class:`~aerospike_async.Key`, multiple keys (list or
        varargs), or explicit ``namespace`` / ``set_name`` for index scans.

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
            keys_list: Keyword list of keys when not using ``arg1`` or varargs;
                forwarded to the client as ``keys``.
            behavior: Optional override for this query; defaults to the session's
                :attr:`behavior`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.QueryBuilder` to
            chain ``where``, ``bins``, ``execute``, etc.

        Raises:
            TypeError: If positional types do not match the supported overloads.
            ValueError: If a key list is empty or arguments are inconsistent.

        Example:
            users = DataSet.of("test", "users")
            rs = await session.query(users.id(1)).bins(["name"]).execute()
            row = await rs.first_or_raise()

        Example:
            users = DataSet.of("test", "users")
            rs = await session.query(users.ids(1, 2, 3)).bins(["name"]).execute()
            rows = await rs.collect()

        See Also:
            :meth:`Client.query`: Same shapes without session behavior.
            :meth:`upsert`: Writes for the same keys.
        """
        b = self._behavior if behavior is None else behavior
        # Handle positional arguments (SDK API)
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

        return self._client.query(  # type: ignore[call-overload]
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            key=key,
            keys=keys_list,
            behavior=b,
        )

    @typing.overload
    def index(
        self,
        *,
        dataset: DataSet,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create an index builder from a DataSet."""
        ...

    @typing.overload
    def index(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create an index builder with explicit namespace/set."""
        ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """
        Create a secondary index builder for a namespace and set.

        Args:
            namespace: Namespace name when not using ``dataset``.
            set_name: Set name when not using ``dataset``.
            dataset: Optional :class:`~aerospike_sdk.dataset.DataSet` that
                supplies namespace and set.
            behavior: Reserved for symmetry with :meth:`query`; forwarded to
                :meth:`Client.index` but not used by index operations yet.

        Returns:
            :class:`~aerospike_sdk.aio.operations.index.IndexBuilder` for
                chaining index definition and creation.

        Raises:
            ValueError: If ``dataset`` is not given and ``namespace`` or
                ``set_name`` is missing.

        Example::

            users = DataSet.of("test", "users")
            await session.index(dataset=users).on_bin("age").named("age_idx").numeric().create()

        See Also:
            :meth:`Client.index`
        """
        if dataset is not None:
            return self._client.index(dataset=dataset, behavior=behavior)
        elif namespace is not None and set_name is not None:
            return self._client.index(
                namespace, set_name, behavior=behavior,
            )
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - index(dataset=DataSet(...))\n"
                "  - index(namespace=..., set_name=...)"
            )

    def transaction_session(self) -> TransactionalSession:
        """
        Create a transactional session for multi-record atomic operations.

        Returns:
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`.

        See Also:
            :meth:`Client.transaction_session`
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

        Example::

                # Raw command (no double .info)
                response = await session.info("sindex-list")

                # High-level helpers
                info = session.info()
                namespaces = await info.namespaces()
                by_node = await info.info_on_all_nodes("build")
        """
        if command is not None:
            return self._client._async_client.info(command)
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

        Example::

                if await session.is_namespace_sc("test"):
                    print("Namespace 'test' is in strong consistency mode")
                else:
                    print("Namespace 'test' is in AP (availability) mode")
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

        Example::

                async def transfer_funds(tx_session):
                    await tx_session.upsert(accounts.id("acc1")).put({"balance": 100}).execute()
                    await tx_session.upsert(accounts.id("acc2")).put({"balance": 200}).execute()

                await session.do_in_transaction(transfer_funds)
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
        """Start a create-or-replace write for one or more keys.

        If the record exists, bins are merged according to the chained operations;
        if it does not exist, it is created. Use :meth:`insert` when the record
        must not already exist.

        Args:
            arg1: A single :class:`~aerospike_async.Key`, a list of keys, or omit
                and pass ``key`` / ``dataset`` + ``key_value`` / ``namespace`` +
                ``set_name`` + ``key_value``.
            arg2: Optional second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional is a key.
            key: Single key (keyword form).
            dataset: Dataset used with ``key_value`` to build a key.
            namespace: Namespace used with ``set_name`` and ``key_value``.
            set_name: Set name used with ``namespace`` and ``key_value``.
            key_value: User key value with ``dataset`` or ``namespace``/``set_name``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`
            for ``put``, ``bin``, ``where``, ``execute``, etc.

        Raises:
            ValueError: If no keys are resolved or lists are empty.
            TypeError: If positional arguments are not keys or lists of keys.

        Example:
            users = DataSet.of("test", "users")
            await session.upsert(users.id(1)).put({"name": "Tim", "age": 30}).execute()

        See Also:
            :meth:`insert`: Fails if the record already exists.
            :meth:`update`: Fails if the record does not exist.
            :meth:`replace`: Replace-entire-record semantics when configured.
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
        """Start a create-only write; fails on execute if the record already exists.

        Key resolution matches :meth:`upsert`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            await session.insert(users.id(99)).put({"name": "new"}).execute()

        See Also:
            :meth:`upsert`: Create or update.
        """
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
        """Start an update-only write; fails on execute if the record is missing.

        Key resolution matches :meth:`upsert`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`upsert`: Create if missing.
            :meth:`replace_if_exists`: Replace semantics when the record exists.
        """
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
        """Start a full-record replace write (bins replaced per builder rules).

        Key resolution matches :meth:`upsert`. Prefer :meth:`replace_if_exists`
        when the record must already exist.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`replace_if_exists`: Replace only when the record exists.
        """
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
        """Start a replace write that requires an existing record.

        Key resolution matches :meth:`upsert`. On execute, missing keys surface
        as errors according to error strategy (default may raise).

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`replace`: Unconditional replace semantics.
        """
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
        """Start a delete for one or more keys.

        Key resolution matches :meth:`upsert`. Chain filters or durable-delete
        options on the builder, then ``await ...execute()``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            await session.delete(users.id(1)).execute()
            await session.delete(users.ids(10, 11)).execute()

        See Also:
            :meth:`background_task`: Delete many records via a server job.
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
        """Start a touch to refresh TTL without changing bins.

        Key resolution matches :meth:`upsert`. Use the builder to set TTL or
        related policy, then ``await ...execute()``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`upsert`: Writes that can also set expiration via the builder.
        """
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
        """Start an existence check for one or more keys.

        Key resolution matches :meth:`upsert`. After ``execute``, use
        :meth:`~aerospike_sdk.record_result.RecordResult.as_bool` on each
        :class:`~aerospike_sdk.record_result.RecordResult` or inspect
        ``result_code``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            rs = await session.exists(users.id(1)).execute()
            exists = (await rs.first()).as_bool()

        See Also:
            :meth:`query`: Read record data when the key is known to exist.
        """
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    async def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set; this cannot be undone.

        Args:
            dataset: The DataSet to truncate.
            before_nanos: Optional timestamp in nanoseconds. Only records with
                last update time (LUT) less than this value are truncated.
                If None, all records in the set are truncated.

        Returns:
            None

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            users = DataSet.of("test", "users")
            await session.truncate(users)

            cutoff_time = time.time_ns() - (24 * 60 * 60 * 10**9)  # 24 hours ago
            await session.truncate(users, before_nanos=cutoff_time)
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

