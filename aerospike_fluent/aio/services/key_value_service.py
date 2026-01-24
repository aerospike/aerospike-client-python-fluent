"""KeyValueService - Service for key-value operations with shared context."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from aerospike_async import Client, ReadPolicy, Record, WritePolicy

from aerospike_fluent.aio.operations.key_value import KeyValueOperation


class KeyValueService:
    """
    Service for key-value operations with shared namespace and set context.

    This service holds the namespace and set, allowing multiple operations
    without repeating them. More efficient than builders when performing
    many operations on the same namespace/set.

    Example:
        ```python
        async with client.key_value_service("test", "users") as kv:
            await kv.put("user1", {"name": "John"})
            await kv.put("user2", {"name": "Jane"})
            record = await kv.get("user1")
        ```
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
        read_policy: Optional[ReadPolicy] = None,
        write_policy: Optional[WritePolicy] = None,
    ) -> None:
        """
        Initialize a KeyValueService.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            read_policy: Optional default read policy for all operations.
            write_policy: Optional default write policy for all operations.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._default_read_policy = read_policy
        self._default_write_policy = write_policy

    def _create_operation(self, key: Union[str, int]) -> KeyValueOperation:
        """Create a KeyValueOperation with service context."""
        op = KeyValueOperation(
            client=self._client,
            namespace=self._namespace,
            set_name=self._set_name,
            key=key,
        )
        if self._default_read_policy:
            op.with_read_policy(self._default_read_policy)
        if self._default_write_policy:
            op.with_write_policy(self._default_write_policy)
        return op

    async def get(
        self,
        key: Union[str, int],
        bins: Optional[List[str]] = None,
    ) -> Optional[Record]:
        """
        Get a record.

        Args:
            key: The record key (string or integer).
            bins: Optional list of bin names to retrieve.

        Returns:
            The record if found, None otherwise.
        """
        op = self._create_operation(key)
        if bins:
            op.bins(bins)
        return await op.get()

    async def put(
        self,
        key: Union[str, int],
        bins: Dict[str, Any],
    ) -> None:
        """
        Put (create or update) a record.

        Args:
            key: The record key (string or integer).
            bins: Dictionary of bin name to value mappings.
        """
        op = self._create_operation(key)
        await op.put(bins)

    async def delete(self, key: Union[str, int]) -> bool:
        """
        Delete a record.

        Args:
            key: The record key (string or integer).

        Returns:
            True if the record existed, False otherwise.
        """
        op = self._create_operation(key)
        return await op.delete()

    async def exists(self, key: Union[str, int]) -> bool:
        """
        Check if a record exists.

        Args:
            key: The record key (string or integer).

        Returns:
            True if the record exists, False otherwise.
        """
        op = self._create_operation(key)
        return await op.exists()

    async def add(
        self,
        key: Union[str, int],
        bins: Dict[str, int],
    ) -> None:
        """
        Add (increment) integer values in bins.

        Args:
            key: The record key (string or integer).
            bins: Dictionary of bin name to integer increment values.
        """
        op = self._create_operation(key)
        await op.add(bins)

    async def append(
        self,
        key: Union[str, int],
        bins: Dict[str, str],
    ) -> None:
        """
        Append strings to string bins.

        Args:
            key: The record key (string or integer).
            bins: Dictionary of bin name to string values to append.
        """
        op = self._create_operation(key)
        await op.append(bins)

    async def prepend(
        self,
        key: Union[str, int],
        bins: Dict[str, str],
    ) -> None:
        """
        Prepend strings to string bins.

        Args:
            key: The record key (string or integer).
            bins: Dictionary of bin name to string values to prepend.
        """
        op = self._create_operation(key)
        await op.prepend(bins)

    async def touch(self, key: Union[str, int]) -> None:
        """
        Touch (update TTL) a record without modifying its data.

        Args:
            key: The record key (string or integer).
        """
        op = self._create_operation(key)
        await op.touch()

    async def __aenter__(self) -> KeyValueService:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Async context manager exit."""
        # No cleanup needed for KeyValueService
        pass


