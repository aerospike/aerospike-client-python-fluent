"""FluentClient - Main entry point for the Aerospike Fluent API."""

from __future__ import annotations

import typing
from typing import List, Optional, Union, overload

from aerospike_async import (
    Client as AsyncClient,
    ClientPolicy,
    Key,
    new_client,
)

from aerospike_fluent.dataset import DataSet
from aerospike_fluent.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.operations.key_value import KeyValueOperation
    from aerospike_fluent.aio.operations.query import QueryBuilder
    from aerospike_fluent.aio.operations.index import IndexBuilder
    from aerospike_fluent.aio.services.key_value_service import KeyValueService
    from aerospike_fluent.aio.services.transactional_session import TransactionalSession
    from aerospike_fluent.aio.session import Session


class FluentClient:
    """
    Fluent API wrapper for the Aerospike async Python client.

    Provides a more intuitive and chainable interface for database operations.

    Example:
        ```python
        async with FluentClient("localhost:3000") as client:
            record = await client.key_value(
                namespace="test", 
                set_name="users", 
                key="user123"
            ).get()
        ```
    """

    def __init__(
        self,
        seeds: str,
        policy: Optional[ClientPolicy] = None,
    ) -> None:
        """
        Initialize a FluentClient.

        Args:
            seeds: Aerospike cluster seed addresses (e.g., "localhost:3000")
            policy: Optional client policy. If None, a default policy is used.

        Note:
            The client is not connected until `connect()` is called or
            the client is used as a context manager.
        """
        self._seeds = seeds
        if policy is None:
            policy = ClientPolicy()
            policy.use_services_alternate = True
        self._policy = policy
        self._client: Optional[AsyncClient] = None
        self._connected = False

    async def connect(self) -> None:
        """
        Connect to the Aerospike cluster.

        Raises:
            ConnectionError: If connection fails.
        """
        if self._connected and self._client is not None:
            return

        self._client = await new_client(self._policy, self._seeds)
        self._connected = True

    async def close(self) -> None:
        """Close the connection to the Aerospike cluster."""
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._connected = False

    async def __aenter__(self) -> FluentClient:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[typing.TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    @property
    def _async_client(self) -> AsyncClient:
        """
        Get the underlying async client.

        Raises:
            RuntimeError: If the client is not connected.
        """
        if not self._connected or self._client is None:
            raise RuntimeError("Client is not connected. Call connect() first or use async with.")
        return self._client

    @overload
    def key_value(
        self,
        *,
        key: Key,
    ) -> KeyValueOperation:
        """Create a key-value operation builder from a Key object."""
        ...

    @overload
    def key_value(
        self,
        *,
        dataset: DataSet,
        key: Union[str, int, bytes],
    ) -> KeyValueOperation:
        """Create a key-value operation builder using a DataSet."""
        ...

    @overload
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

        Supports multiple calling styles:

        1. Using a Key object:
           ```python
           users = DataSet.of("test", "users")
           key = users.id("user123")
           await client.key_value(key=key).put({"name": "John"})
           ```

        2. Using a DataSet:
           ```python
           users = DataSet.of("test", "users")
           await client.key_value(dataset=users, key="user123").put({"name": "John"})
           ```

        3. Explicit namespace/set (original style):
           ```python
           await client.key_value(
               namespace="test",
               set_name="users",
               key="user123"
           ).put({"name": "John"})
           ```

        Args:
            namespace: The namespace name (if not using Key or DataSet).
            set_name: The set name (if not using Key or DataSet).
            key: The record key. Can be a Key object, or string/int/bytes identifier.
            dataset: Optional DataSet to use for namespace/set.

        Returns:
            A KeyValueOperation builder for chaining operations.
        """
        from aerospike_fluent.aio.operations.key_value import KeyValueOperation

        # Handle Key object
        if isinstance(key, Key):
            namespace = key.namespace
            set_name = key.set_name
            key_value = key.value
        # Handle DataSet
        elif dataset is not None:
            if key is None:
                raise ValueError("key is required when using dataset parameter")
            namespace = dataset.namespace
            set_name = dataset.set_name
            key_value = key
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None and key is not None:
            key_value = key
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - key_value(key=Key(...))\n"
                "  - key_value(dataset=DataSet(...), key=...)\n"
                "  - key_value(namespace=..., set_name=..., key=...)"
            )

        return KeyValueOperation(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
            key=key_value,
        )

    @overload
    def query(
        self,
        *,
        dataset: DataSet,
    ) -> QueryBuilder:
        """Create a query builder from a DataSet."""
        ...

    @overload
    def query(
        self,
        *,
        key: Key,
    ) -> QueryBuilder:
        """Create a query builder for a single Key (point read)."""
        ...

    @overload
    def query(
        self,
        *,
        keys: List[Key],
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @overload
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
        set_name: Optional[str] = None,
        namespace: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys: Optional[List[Key]] = None,
    ) -> QueryBuilder:
        """
        Create a query builder.

        Supports multiple calling styles:

        1. Using a DataSet (positional or keyword):
           ```python
           users = DataSet.of("test", "users")
           async for record in client.query(users).execute():
           # or
           async for record in client.query(dataset=users).execute():
               print(record.bins)
           ```

        2. Using a single Key (positional or keyword):
           ```python
           users = DataSet.of("test", "users")
           key = users.id("user123")
           recordset = await client.query(key).execute()
           # or
           recordset = await client.query(key=key).execute()
           ```

        3. Using multiple Keys (positional or keyword):
           ```python
           users = DataSet.of("test", "users")
           keys = users.ids("user1", "user2", "user3")
           recordset = await client.query(keys).execute()
           # or
           recordset = await client.query(keys=keys).execute()
           ```

        4. Explicit namespace/set (original style):
           ```python
           async for record in client.query(
               namespace="test",
               set_name="users"
           ).execute():
               print(record.bins)
           ```

        Args:
            arg1: Optional positional argument (DataSet, Key, or List[Key]).
            namespace: The namespace name (if not using DataSet or Key).
            set_name: The set name (if not using DataSet or Key).
            dataset: Optional DataSet to use for namespace/set.
            key: Optional single Key for point read.
            keys: Optional list of Keys for batch read.

        Returns:
            A QueryBuilder for chaining query operations.
        """
        from aerospike_fluent.aio.operations.query import QueryBuilder

        # Handle positional arguments
        # Check if arg1 and arg2 are both strings (namespace, set_name pattern)
        if isinstance(arg1, str) and set_name is not None:
            # This is the namespace, set_name pattern - use them directly
            namespace = arg1
            # set_name is already set from the parameter
        elif arg1 is not None:
            # Handle single positional argument (DataSet, Key, or List[Key])
            if isinstance(arg1, DataSet):
                dataset = arg1
            elif isinstance(arg1, Key):
                key = arg1
            elif isinstance(arg1, list):
                keys = arg1
            else:
                raise TypeError(f"Expected DataSet, Key, or List[Key], got {type(arg1)}")

        # Handle single Key
        if key is not None:
            namespace = key.namespace
            set_name = key.set_name
            # For single key queries, we'll need to handle this in QueryBuilder
            # For now, create a query builder and store the key
            builder = QueryBuilder(
                client=self._async_client,
                namespace=namespace,
                set_name=set_name,
            )
            builder._single_key = key  # Store for later use
            return builder

        # Handle multiple Keys
        if keys is not None:
            if not keys:
                raise ValueError("keys list cannot be empty")
            # Use first key's namespace/set (all should be same)
            namespace = keys[0].namespace
            set_name = keys[0].set_name
            builder = QueryBuilder(
                client=self._async_client,
                namespace=namespace,
                set_name=set_name,
            )
            builder._keys = keys  # Store for later use
            return builder

        # Handle DataSet
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None:
            pass
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - query(dataset=DataSet(...))\n"
                "  - query(key=Key(...))\n"
                "  - query(keys=[Key(...), ...])\n"
                "  - query(namespace=..., set_name=...)"
            )

        return QueryBuilder(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
        )

    @overload
    def index(
        self,
        *,
        dataset: DataSet,
    ) -> IndexBuilder:
        """Create an index builder from a DataSet."""
        ...

    @overload
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

        Supports multiple calling styles:

        1. Using a DataSet:
           ```python
           users = DataSet.of("test", "users")
           await client.index(dataset=users).on_bin("age").named("age_idx").numeric().create()
           ```

        2. Explicit namespace/set (original style):
           ```python
           await client.index(
               namespace="test",
               set_name="users"
           ).on_bin("age").named("age_idx").numeric().create()
           ```

        Args:
            namespace: The namespace name (if not using DataSet).
            set_name: The set name (if not using DataSet).
            dataset: Optional DataSet to use for namespace/set.

        Returns:
            An IndexBuilder for chaining index operations.
        """
        from aerospike_fluent.aio.operations.index import IndexBuilder

        # Handle DataSet
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None:
            pass
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - index(dataset=DataSet(...))\n"
                "  - index(namespace=..., set_name=...)"
            )

        return IndexBuilder(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
        )

    @overload
    def key_value_service(
        self,
        *,
        dataset: DataSet,
    ) -> KeyValueService:
        """Create a key-value service from a DataSet."""
        ...

    @overload
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

        This service is more efficient than builders when performing many
        operations on the same namespace/set, as it holds the context
        and doesn't require repeating namespace/set for each operation.

        Supports multiple calling styles:

        1. Using a DataSet:
           ```python
           users = DataSet.of("test", "users")
           async with client.key_value_service(dataset=users) as kv:
               await kv.put("user1", {"name": "John"})
               record = await kv.get("user1")
           ```

        2. Explicit namespace/set (original style):
           ```python
           async with client.key_value_service(
               namespace="test",
               set_name="users"
           ) as kv:
               await kv.put("user1", {"name": "John"})
               record = await kv.get("user1")
           ```

        Args:
            namespace: The namespace name (if not using DataSet).
            set_name: The set name (if not using DataSet).
            dataset: Optional DataSet to use for namespace/set.

        Returns:
            A KeyValueService for performing multiple operations.
        """
        from aerospike_fluent.aio.services.key_value_service import KeyValueService

        # Handle DataSet
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None:
            pass
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - key_value_service(dataset=DataSet(...))\n"
                "  - key_value_service(namespace=..., set_name=...)"
            )

        return KeyValueService(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
        )

    def transaction_session(self) -> TransactionalSession:
        """
        Create a transactional session.

        All operations within this session will be part of the same
        transaction. The transaction is automatically committed when
        the session exits successfully, or rolled back on error.

        Note: Full transaction support depends on Aerospike server
        capabilities. This is a placeholder for future transaction support.

        Returns:
            A TransactionalSession for transactional operations.

        Example:
            ```python
            async with client.transaction_session() as session:
                kv = session.key_value("test", "users")
                await kv.put("user1", {"name": "John"})
                await kv.put("user2", {"name": "Jane"})
                # Transaction auto-committed on exit
            ```
        """
        from aerospike_fluent.aio.services.transactional_session import (
            TransactionalSession,
        )

        return TransactionalSession(client=self._async_client)

    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """
        Create a session with the specified behavior.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: The behavior configuration for the session.
                     If None, uses Behavior.DEFAULT.

        Returns:
            A new Session instance.

        Example:
            ```python
            # Create a session with default behavior
            session = client.create_session()

            # Create a session with custom behavior
            fast_behavior = Behavior.DEFAULT.derive_with_changes(
                name="fast",
                total_timeout=timedelta(seconds=5)
            )
            session = client.create_session(fast_behavior)
            ```
        """
        from aerospike_fluent.aio.session import Session

        if behavior is None:
            behavior = Behavior.DEFAULT

        return Session(client=self, behavior=behavior)

