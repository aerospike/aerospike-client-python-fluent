"""Session - Main interface for database operations with Behavior configuration."""

from __future__ import annotations

import typing
from typing import Awaitable, Dict, List, Optional, overload, Union

from aerospike_async import Key, RecordExistsAction, WritePolicy

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.operations.batch import BatchOperationBuilder
    from aerospike_fluent.aio.operations.batch_delete import BatchDeleteOperation
    from aerospike_fluent.aio.operations.batch_exists import BatchExistsOperation
    from aerospike_fluent.aio.operations.key_value import KeyValueOperation
    from aerospike_fluent.aio.operations.query import QueryBuilder
    from aerospike_fluent.aio.operations.index import IndexBuilder
    from aerospike_fluent.aio.services.key_value_service import KeyValueService
    from aerospike_fluent.aio.services.transactional_session import TransactionalSession
    from aerospike_fluent.aio.info import InfoCommands


class Session:
    """
    Main interface for performing database operations.

    A Session is created from a FluentClient (Cluster) and is configured
    with a Behavior that defines the policies (timeouts, retries) for all
    operations performed within that session.

    Sessions are lightweight and can be created multiple times from the
    same client, each with different behaviors for different use cases.

    Example:
        ```python
        # Create a session with default behavior
        async with FluentClient("localhost:3000") as client:
            session = client.create_session(Behavior.DEFAULT)

            # Use session for operations
            record = await session.key_value(
                namespace="test",
                set_name="users",
                key="user123"
            ).get()

        # Create different sessions for different use cases
        fast_session = client.create_session(Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5)
        ))
        durable_session = client.create_session(Behavior.DEFAULT.derive_with_changes(
            name="durable",
            max_retries=5
        ))
        ```
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
                .update(key2).bin("counter").increment_by(1) \\
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
        
        return BatchOperationBuilder(self._client._client)

    @typing.overload
    def key_value(
        self,
        *,
        key: Key,
    ) -> KeyValueOperation:
        """Create a key-value operation builder from a Key object."""
        ...

    @typing.overload
    def key_value(
        self,
        *,
        dataset: DataSet,
        key: Union[str, int, bytes],
    ) -> KeyValueOperation:
        """Create a key-value operation builder using a DataSet."""
        ...

    @typing.overload
    def key_value(
        self,
        namespace: str,
        set_name: str,
        key: Union[str, int],
    ) -> KeyValueOperation:
        """Create a key-value operation builder with explicit namespace/set."""
        ...

    def key_value(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key: Optional[Union[str, int, bytes, Key]] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> KeyValueOperation:
        """
        Create a key-value operation builder.

        See FluentClient.key_value() for full documentation.
        """
        if isinstance(key, Key):
            return self._client.key_value(key=key)
        elif dataset is not None and key is not None:
            return self._client.key_value(dataset=dataset, key=key)
        elif namespace is not None and set_name is not None and key is not None:
            return self._client.key_value(namespace, set_name, key)
        else:
            raise ValueError(
                "Invalid arguments. Use one of:\n"
                "  - key_value(key=Key(...))\n"
                "  - key_value(dataset=DataSet(...), key=...)\n"
                "  - key_value(namespace=..., set_name=..., key=...)"
            )

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
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            # Check if it's a DataSet
            if isinstance(arg1, DataSet):
                return self._client.query(dataset=arg1)
            # Check if it's a Key
            elif isinstance(arg1, Key):
                # If arg2 is also a Key, or there are varargs, treat as multiple keys
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                else:
                    # Single key
                    return self._client.query(key=arg1)
                # Multiple keys - pass as keyword argument
                return self._client.query(keys=all_keys)
            # Check if it's a List[Key]
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                return self._client.query(keys=arg1)
            # Check if it's a string (namespace) with second arg as set_name
            elif isinstance(arg1, str) and arg2 is not None:
                return self._client.query(namespace=arg1, set_name=arg2)

        # Handle varargs (multiple Keys)
        if keys:
            # Convert to list and pass as keyword argument
            keys_list = list(keys)
            if arg1 is not None and isinstance(arg1, Key):
                keys_list.insert(0, arg1)
            if arg2 is not None and isinstance(arg2, Key):
                keys_list.insert(1 if arg1 is not None and isinstance(arg1, Key) else 0, arg2)
            return self._client.query(keys=keys_list)

        # Fall back to keyword arguments (legacy style)
        return self._client.query(
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            key=key,
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

    @typing.overload
    def key_value_service(
        self,
        *,
        dataset: DataSet,
    ) -> KeyValueService:
        """Create a key-value service from a DataSet."""
        ...

    @typing.overload
    def key_value_service(
        self,
        namespace: str,
        set_name: str,
    ) -> KeyValueService:
        """Create a key-value service with explicit namespace/set."""
        ...

    def key_value_service(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
    ) -> KeyValueService:
        """
        Create a key-value service with shared namespace and set context.

        See FluentClient.key_value_service() for full documentation.
        """
        if dataset is not None:
            return self._client.key_value_service(dataset=dataset)
        elif namespace is not None and set_name is not None:
            return self._client.key_value_service(namespace, set_name)
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - key_value_service(dataset=DataSet(...))\n"
                "  - key_value_service(namespace=..., set_name=...)"
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
                await tx_session.key_value("test", "accounts", "acc1").put({"balance": 100})
                await tx_session.key_value("test", "accounts", "acc2").put({"balance": 200})

            await session.do_in_transaction(transfer_funds)
            ```
        """
        # This will be implemented when TransactionalSession is fully functional
        # For now, delegate to the client's transaction_session
        async with self.transaction_session() as tx_session:
            return await operation(tx_session)

    # Convenience methods for write operations
    @typing.overload
    def upsert(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create an upsert operation builder for a single Key."""
        ...

    @typing.overload
    def upsert(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create an upsert operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def upsert(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create an upsert operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create an upsert (create or update) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `upsert(key)`
        2. Positional List[Key]: `upsert([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `upsert(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `upsert(key=key)`, `upsert(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.

        Example:
            ```python
            users = DataSet.of("test", "users")
            key = users.id("user123")

            # Single key (positional)
            await session.upsert(key).put({"name": "John", "age": 30})

            # Keyword arguments (legacy)
            await session.upsert(key=key).put({"name": "John", "age": 30})
            await session.upsert(dataset=users, key_value="user123").put({"name": "John"})
            ```
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            # Check if it's a single Key
            if isinstance(arg1, Key):
                # If arg2 is also a Key, or there are varargs, treat as multiple keys
                # For now, multiple keys not fully supported - return first key's operation
                # TODO: Add batch upsert support
                return self.key_value(key=arg1)
            # Check if it's a List[Key]
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                # For now, return first key's operation
                # TODO: Add batch upsert support
                return self.key_value(key=arg1[0])

        # Fall back to keyword arguments (legacy style)
        if key is not None:
            return self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                return self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                return self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

    @typing.overload
    def insert(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create an insert operation builder for a single Key."""
        ...

    @typing.overload
    def insert(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create an insert operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def insert(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create an insert operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create an insert (create only) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `insert(key)`
        2. Positional List[Key]: `insert([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `insert(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `insert(key=key)`, `insert(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, Key):
                op = self.key_value(key=arg1)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                op = self.key_value(key=arg1[0])
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            op = self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                op = self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                op = self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

        # Set write policy to CreateOnly for insert semantics
        if op._write_policy is None:
            op._write_policy = WritePolicy()
        op._write_policy.record_exists_action = RecordExistsAction.CREATE_ONLY
        return op

    @typing.overload
    def update(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create an update operation builder for a single Key."""
        ...

    @typing.overload
    def update(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create an update operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def update(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create an update operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create an update (update only) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `update(key)`
        2. Positional List[Key]: `update([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `update(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `update(key=key)`, `update(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, Key):
                op = self.key_value(key=arg1)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                op = self.key_value(key=arg1[0])
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            op = self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                op = self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                op = self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

        # Set write policy to UpdateOnly for update semantics
        if op._write_policy is None:
            op._write_policy = WritePolicy()
        op._write_policy.record_exists_action = RecordExistsAction.UPDATE_ONLY
        return op

    @typing.overload
    def replace(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create a replace operation builder for a single Key."""
        ...

    @typing.overload
    def replace(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create a replace operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def replace(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create a replace operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create a replace (replace only) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `replace(key)`
        2. Positional List[Key]: `replace([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `replace(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `replace(key=key)`, `replace(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, Key):
                op = self.key_value(key=arg1)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                op = self.key_value(key=arg1[0])
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            op = self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                op = self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                op = self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

        # Set write policy to Replace for replace semantics (create or replace)
        if op._write_policy is None:
            op._write_policy = WritePolicy()
        op._write_policy.record_exists_action = RecordExistsAction.REPLACE
        return op

    @typing.overload
    def replace_if_exists(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create a replace-if-exists operation builder for a single Key."""
        ...

    @typing.overload
    def replace_if_exists(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create a replace-if-exists operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def replace_if_exists(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create a replace-if-exists operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create a replace-if-exists (replace only if record exists) operation builder.

        This operation will fail if the record does not exist.
        All bins not in the write command will be deleted.

        Supports multiple calling styles:

        1. Positional single Key: `replace_if_exists(key)`
        2. Positional List[Key]: `replace_if_exists([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `replace_if_exists(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `replace_if_exists(key=key)`, `replace_if_exists(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.

        Example:
            ```python
            users = DataSet.of("test", "users")
            key = users.id("user123")

            # Replace only if exists - fails if record doesn't exist
            await session.replace_if_exists(key).put({"name": "New Name", "status": "updated"})
            ```
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, Key):
                op = self.key_value(key=arg1)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                op = self.key_value(key=arg1[0])
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            op = self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                op = self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                op = self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

        # Set write policy to ReplaceOnly - fail if record doesn't exist
        if op._write_policy is None:
            op._write_policy = WritePolicy()
        op._write_policy.record_exists_action = RecordExistsAction.REPLACE_ONLY
        return op

    @typing.overload
    def delete(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create a delete operation builder for a single Key."""
        ...

    @typing.overload
    def delete(
        self,
        keys: List[Key],
    ) -> "BatchDeleteOperation":
        """Create a batch delete operation builder for multiple Keys."""
        ...

    @typing.overload
    def delete(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "BatchDeleteOperation":
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
    ) -> Union[KeyValueOperation, "BatchDeleteOperation"]:
        """
        Create a delete operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `delete(key)`
        2. Positional List[Key]: `delete([key1, key2])`
        3. Varargs Keys: `delete(key1, key2, key3)`
        4. Keyword arguments (legacy): `delete(key=key)`, `delete(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            KeyValueOperation for single key, BatchDeleteOperation for multiple keys.

        Example:
            ```python
            users = DataSet.of("test", "users")
            key = users.id("user123")

            # Single key (positional)
            await session.delete(key).delete()

            # Multiple keys (List)
            await session.delete([key1, key2, key3]).execute()

            # Multiple keys (varargs)
            await session.delete(key1, key2, key3).execute()

            # Keyword arguments (legacy)
            await session.delete(key=key).delete()
            await session.delete(dataset=users, key_value="user123").delete()
            ```
        """
        from aerospike_fluent.aio.operations.batch_delete import BatchDeleteOperation

        # Handle positional arguments (fluent API)
        if arg1 is not None:
            # Check if it's a single Key
            if isinstance(arg1, Key):
                # If arg2 is also a Key, or there are varargs, treat as multiple keys
                if isinstance(arg2, Key) or keys:
                    all_keys = [arg1]
                    if isinstance(arg2, Key):
                        all_keys.append(arg2)
                    if keys:
                        all_keys.extend(keys)
                    # Multiple keys - return BatchDeleteOperation
                    return BatchDeleteOperation(self._client._async_client, all_keys)
                else:
                    # Single key - return KeyValueOperation
                    return self.key_value(key=arg1)
            # Check if it's a List[Key]
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                # Multiple keys - return BatchDeleteOperation
                return BatchDeleteOperation(self._client._async_client, arg1)

        # Fall back to keyword arguments (legacy style)
        if key is not None:
            return self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                return self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                return self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

    @typing.overload
    def touch(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create a touch operation builder for a single Key."""
        ...

    @typing.overload
    def touch(
        self,
        keys: List[Key],
    ) -> KeyValueOperation:
        """Create a touch operation builder for multiple Keys (returns first key's operation)."""
        ...

    @typing.overload
    def touch(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> KeyValueOperation:
        """Create a touch operation builder for multiple Keys (varargs, returns first key's operation)."""
        ...

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
    ) -> KeyValueOperation:
        """
        Create a touch (update TTL) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `touch(key)`
        2. Positional List[Key]: `touch([key1, key2])` (returns first key's operation)
        3. Varargs Keys: `touch(key1, key2, key3)` (returns first key's operation)
        4. Keyword arguments (legacy): `touch(key=key)`, `touch(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            A KeyValueOperation builder for chaining operations.

        Example:
            ```python
            users = DataSet.of("test", "users")
            key = users.id("user123")

            # Single key (positional)
            await session.touch(key).touch()

            # Keyword arguments (legacy)
            await session.touch(key=key).touch()
            ```
        """
        # Handle positional arguments (fluent API)
        if arg1 is not None:
            if isinstance(arg1, Key):
                return self.key_value(key=arg1)
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                return self.key_value(key=arg1[0])
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            return self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                return self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                return self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

    @typing.overload
    def exists(
        self,
        key: Key,
    ) -> KeyValueOperation:
        """Create an exists operation builder for a single Key."""
        ...

    @typing.overload
    def exists(
        self,
        keys: List[Key],
    ) -> "BatchExistsOperation":
        """Create a batch exists operation builder for multiple Keys."""
        ...

    @typing.overload
    def exists(
        self,
        key1: Key,
        key2: Key,
        *keys: Key,
    ) -> "BatchExistsOperation":
        """Create a batch exists operation builder for multiple Keys (varargs)."""
        ...

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
    ) -> Union[KeyValueOperation, "BatchExistsOperation"]:
        """
        Create an exists (check existence) operation builder.

        Supports multiple calling styles:

        1. Positional single Key: `exists(key)`
        2. Positional List[Key]: `exists([key1, key2])`
        3. Varargs Keys: `exists(key1, key2, key3)`
        4. Keyword arguments (legacy): `exists(key=key)`, `exists(dataset=..., key_value=...)`

        Args:
            arg1: A Key object, List[Key], or None (for keyword args).
            arg2: Second Key (for varargs).
            *keys: Additional Keys (for varargs).
            key: A Key object (keyword arg, preferred).
            dataset: A DataSet to use for namespace/set.
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key_value: The key value (if not using Key object).

        Returns:
            KeyValueOperation for single key, BatchExistsOperation for multiple keys.

        Example:
            ```python
            users = DataSet.of("test", "users")
            key = users.id("user123")

            # Single key (positional)
            exists = await session.exists(key).exists()

            # Multiple keys - batch operation
            results = await session.exists(users.ids("user1", "user2", "user3")).execute()
            for exists in results:
                print(f"exists: {exists}")
            ```
        """
        from aerospike_fluent.aio.operations.batch_exists import BatchExistsOperation

        # Handle positional arguments (fluent API)
        if arg1 is not None:
            # Check if it's a single Key
            if isinstance(arg1, Key):
                # If arg2 is also a Key, or there are varargs, treat as multiple keys
                if isinstance(arg2, Key) or keys:
                    all_keys = [arg1]
                    if isinstance(arg2, Key):
                        all_keys.append(arg2)
                    if keys:
                        all_keys.extend(keys)
                    # Multiple keys - return BatchExistsOperation
                    return BatchExistsOperation(self._client._client, all_keys)
                else:
                    # Single key - return KeyValueOperation
                    return self.key_value(key=arg1)
            # Check if it's a List[Key]
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                # Multiple keys - return BatchExistsOperation
                return BatchExistsOperation(self._client._client, arg1)
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")

        # Fall back to keyword arguments (legacy style)
        if key is not None:
            return self.key_value(key=key)
        elif key_value is not None:
            if dataset is not None:
                return self.key_value(dataset=dataset, key=key_value)
            elif namespace is not None and set_name is not None:
                return self.key_value(namespace, set_name, key_value)
            else:
                raise ValueError("Either dataset or (namespace and set_name) must be provided with key_value")
        else:
            raise ValueError("Either key (Key object) or key_value must be provided")

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

