"""KeyValueOperation - Builder for key-value operations."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from aerospike_async import (
    BitOperation,
    Client,
    GenerationPolicy,
    Key,
    ListOperation,
    MapOperation,
    Operation,
    ReadPolicy,
    Record,
    WritePolicy,
)
from aerospike_async.exceptions import ServerError


class BinBuilder:
    """
    Builder for chaining bin operations (e.g., .bin("name").set_to("Tim")).

    This class enables fluent chaining of bin operations.
    """

    def __init__(self, operation: KeyValueOperation, bin_name: Optional[str] = None) -> None:
        """
        Initialize a BinBuilder.

        Args:
            operation: The parent KeyValueOperation instance.
            bin_name: Optional initial bin name (for chaining).
        """
        self._operation = operation
        self._bins: Dict[str, Any] = {}
        self._increments: Dict[str, int] = {}
        self._remove_other_bins: bool = False
        self._current_bin: Optional[str] = bin_name

    def bin(self, bin_name: str) -> BinBuilder:
        """
        Start a bin operation chain.
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            self for method chaining.
        """
        self._current_bin = bin_name
        return self

    def set_to(self, value: Any) -> BinBuilder:
        """
        Set a bin value (used after .bin(name)).
        
        Args:
            value: The value to set.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .set_to(value)")
        self._bins[self._current_bin] = value
        self._current_bin = None
        return self

    def increment_by(self, value: int) -> BinBuilder:
        """
        Increment a bin value (used after .bin(name)).
        
        Alias: add().
        
        Args:
            value: The amount to increment by.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .increment_by(value)")
        self._increments[self._current_bin] = self._increments.get(self._current_bin, 0) + value
        self._current_bin = None
        return self

    def add(self, value: int) -> BinBuilder:
        """
        Add (increment) a numeric bin value (used after .bin(name)).
        
        This is an alias for increment_by().
        
        Args:
            value: The amount to add.
        
        Returns:
            self for method chaining.
        """
        return self.increment_by(value)

    def append(self, value: str) -> BinBuilder:
        """
        Append a string to a bin value (used after .bin(name)).
        
        Args:
            value: The string to append.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .append(value)")
        if "_appends" not in self.__dict__:
            self._appends: Dict[str, str] = {}
        self._appends[self._current_bin] = value
        self._current_bin = None
        return self

    def prepend(self, value: str) -> BinBuilder:
        """
        Prepend a string to a bin value (used after .bin(name)).
        
        Args:
            value: The string to prepend.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .prepend(value)")
        if "_prepends" not in self.__dict__:
            self._prepends: Dict[str, str] = {}
        self._prepends[self._current_bin] = value
        self._current_bin = None
        return self

    def remove(self) -> BinBuilder:
        """
        Remove (delete) a bin (used after .bin(name)).
        
        This sets the bin to None, which deletes it from the record.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .remove()")
        if "_removes" not in self.__dict__:
            self._removes: List[str] = []
        self._removes.append(self._current_bin)
        self._current_bin = None
        return self

    def get(self) -> BinBuilder:
        """
        Mark a bin for reading (used after .bin(name)).
        
        The bin value will be included in the result when execute() is called.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .get()")
        if "_gets" not in self.__dict__:
            self._gets: List[str] = []
        self._gets.append(self._current_bin)
        self._current_bin = None
        return self

    def and_remove_other_bins(self) -> BinBuilder:
        """
        Mark that all bins not explicitly set should be removed.
        
        Returns:
            self for method chaining.
        """
        self._remove_other_bins = True
        return self

    def ensure_generation_is(self, generation: int) -> BinBuilder:
        """
        Set expected generation for optimistic locking.
        
        The operation will fail if the record's current generation
        doesn't match the expected generation.
        
        Args:
            generation: The expected generation number.
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            record = await session.key_value(key=key).get()
            await session.upsert(key) \\
                .ensure_generation_is(record.generation) \\
                .bin("counter").set_to(new_value) \\
                .execute()
            ```
        """
        self._expected_generation = generation
        return self

    async def execute(self) -> Optional[Record]:
        """
        Execute the accumulated bin operations.
        
        This will:
        1. Put all bins set via .set_to()
        2. Add all increments via .increment_by() / .add()
        3. Append strings via .append()
        4. Prepend strings via .prepend()
        5. Remove bins via .remove()
        6. Read bins via .get()
        7. Remove other bins if .and_remove_other_bins() was called
        8. Apply generation check if .ensure_generation_is() was called
        
        Returns:
            Record if any .get() operations were included, None otherwise.
        """
        operations: List[Union[Operation, ListOperation, MapOperation, BitOperation]] = []
        
        # Put operations
        for bin_name, value in self._bins.items():
            operations.append(Operation.put(bin_name, value))
        
        # Add (increment) operations
        for bin_name, increment in self._increments.items():
            operations.append(Operation.add(bin_name, increment))
        
        # Append operations
        if hasattr(self, "_appends"):
            for bin_name, value in self._appends.items():
                operations.append(Operation.append(bin_name, value))
        
        # Prepend operations
        if hasattr(self, "_prepends"):
            for bin_name, value in self._prepends.items():
                operations.append(Operation.prepend(bin_name, value))
        
        # Remove bin operations (set to None)
        if hasattr(self, "_removes"):
            for bin_name in self._removes:
                operations.append(Operation.put(bin_name, None))
        
        # Get operations
        if hasattr(self, "_gets"):
            for bin_name in self._gets:
                operations.append(Operation.get_bin(bin_name))
        
        # Handle remove_other_bins
        if self._remove_other_bins:
            record = await self._operation.get()
            if record and record.bins:
                existing_bins = set(record.bins.keys())
                set_bins = set(self._bins.keys())
                bins_to_remove = existing_bins - set_bins
                for bin_name in bins_to_remove:
                    operations.append(Operation.put(bin_name, None))
        
        # Set up generation policy if specified
        if hasattr(self, "_expected_generation"):
            if self._operation._write_policy is None:
                self._operation._write_policy = WritePolicy()
            self._operation._write_policy.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            self._operation._write_policy.generation = self._expected_generation
        
        if operations:
            return await self._operation.operate(operations)
        elif self._bins:
            await self._operation.put(self._bins)
            return None
        return None


class KeyValueOperation:
    """
    Builder for key-value operations (get, put, delete, etc.).
    
    This class provides a fluent interface for building and executing
    key-value operations on a single record.
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
        key: Union[str, int],
    ) -> None:
        """
        Initialize a KeyValueOperation builder.
        
        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            key: The record key (string or integer).
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._key = key
        self._read_policy: Optional[ReadPolicy] = None
        self._write_policy: Optional[WritePolicy] = None
        self._bins: Optional[List[str]] = None
        self._durable_delete: Optional[bool] = None

    def _get_key(self) -> Key:
        """Create a Key object from the namespace, set, and key."""
        return Key(self._namespace, self._set_name, self._key)

    def with_read_policy(self, policy: ReadPolicy) -> KeyValueOperation:
        """
        Set the read policy for this operation.
        
        Args:
            policy: The read policy to use.
        
        Returns:
            self for method chaining.
        """
        self._read_policy = policy
        return self

    def with_write_policy(self, policy: WritePolicy) -> KeyValueOperation:
        """
        Set the write policy for this operation.
        
        Args:
            policy: The write policy to use.
        
        Returns:
            self for method chaining.
        """
        self._write_policy = policy
        return self

    def bins(self, bin_names: List[str]) -> KeyValueOperation:
        """
        Specify which bins to retrieve for get operations.
        
        Args:
            bin_names: List of bin names to retrieve.
        
        Returns:
            self for method chaining.
        """
        self._bins = bin_names
        return self

    def ensure_generation_is(self, generation: int) -> KeyValueOperation:
        """
        Set expected generation for optimistic locking.
        
        The operation will fail if the record's current generation
        doesn't match the expected generation.
        
        Args:
            generation: The expected generation number.
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            record = await session.key_value(key=key).get()
            await session.upsert(key) \\
                .ensure_generation_is(record.generation) \\
                .bin("counter").set_to(new_value) \\
                .execute()
            ```
        """
        if self._write_policy is None:
            self._write_policy = WritePolicy()
        self._write_policy.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
        self._write_policy.generation = generation
        return self

    def bin(self, bin_name: str) -> BinBuilder:
        """
        Start a bin operation chain (e.g., .bin("name").set_to("Tim")).
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            A BinBuilder for chaining operations.
        
        Example:
            await session.key_value(key="mykey").bin("name").set_to("Tim").bin("age").set_to(25).execute()
        """
        return BinBuilder(self, bin_name)

    async def get(self) -> Optional[Record]:
        """
        Get a record.
        
        Returns:
            The record if found, None otherwise.
        """
        policy = self._read_policy or ReadPolicy()
        key = self._get_key()
        try:
            return await self._client.get(policy, key, self._bins)
        except ServerError as e:
            # Return None if key not found, re-raise other server errors
            if "KeyNotFoundError" in str(e):
                return None
            raise

    async def put(self, bins: Dict[str, Any]) -> None:
        """
        Put (create or update) a record.
        
        Args:
            bins: Dictionary of bin name to value mappings.
        """
        policy = self._write_policy or WritePolicy()
        if self._durable_delete is not None:
            policy.durable_delete = self._durable_delete
        key = self._get_key()
        await self._client.put(policy, key, bins)

    def set_bins(self, bins: Dict[str, Any]) -> KeyValueOperation:
        """
        Set bins for a put operation (alias for put, but returns self for chaining).
        
        Args:
            bins: Dictionary of bin name to value mappings.
        
        Returns:
            self for method chaining.
        
        Note:
            This method stores the bins but does not execute the operation.
            Call .execute() to perform the put.
        """
        self._pending_bins = bins
        return self

    async def execute(self) -> Optional[bool]:
        """
        Execute a pending operation (put, delete, or exists).
        
        For put operations (used with .set_bins()):
            await session.key_value(key="mykey").set_bins({"name": "Tim"}).execute()
        
        For delete operations:
            await session.delete(key="mykey").execute()  # Returns bool
        
        For exists operations:
            await session.exists(key="mykey").execute()  # Returns bool
        """
        if hasattr(self, '_pending_bins'):
            await self.put(self._pending_bins)
            delattr(self, '_pending_bins')
            return None
        elif hasattr(self, '_pending_delete'):
            result = await self._delete_impl()
            delattr(self, '_pending_delete')
            return result
        else:
            raise ValueError("No pending operation to execute. Use .set_bins() or .delete() first.")

    def durably(self, durable: bool = True) -> KeyValueOperation:
        """
        Set durable delete flag for delete operations.
        
        Args:
            durable: If True, the delete will be durable (default: True).
        
        Returns:
            self for method chaining.
        
        Example:
            await session.delete(key="mykey").durably(False).delete()
        """
        self._durable_delete = durable
        return self

    async def delete(self) -> bool:
        """
        Delete a record.
        
        Returns:
            True if the record existed, False otherwise.
        """
        policy = self._write_policy or WritePolicy()
        if self._durable_delete is not None:
            policy.durable_delete = self._durable_delete
        key = self._get_key()
        return await self._client.delete(policy, key)

    async def exists(self) -> bool:
        """
        Check if a record exists.
        
        Returns:
            True if the record exists, False otherwise.
        """
        policy = self._read_policy or ReadPolicy()
        key = self._get_key()
        return await self._client.exists(policy, key)

    async def add(self, bins: Dict[str, int]) -> None:
        """
        Add (increment) integer values in bins.
        
        Args:
            bins: Dictionary of bin name to integer increment values.
        """
        policy = self._write_policy or WritePolicy()
        key = self._get_key()
        await self._client.add(policy, key, bins)

    async def append(self, bins: Dict[str, str]) -> None:
        """
        Append strings to string bins.
        
        Args:
            bins: Dictionary of bin name to string values to append.
        """
        policy = self._write_policy or WritePolicy()
        key = self._get_key()
        await self._client.append(policy, key, bins)

    async def prepend(self, bins: Dict[str, str]) -> None:
        """
        Prepend strings to string bins.
        
        Args:
            bins: Dictionary of bin name to string values to prepend.
        """
        policy = self._write_policy or WritePolicy()
        key = self._get_key()
        await self._client.prepend(policy, key, bins)

    async def touch(self) -> None:
        """
        Touch (update TTL) a record without modifying its data.
        """
        policy = self._write_policy or WritePolicy()
        key = self._get_key()
        await self._client.touch(policy, key)

    async def operate(
        self,
        operations: List[Union[Operation, ListOperation, MapOperation, BitOperation]],
    ) -> Optional[Record]:
        """
        Execute multiple operations atomically on a record.
        
        This method supports:
        - Basic operations: Operation.put(), Operation.get(), etc.
        - List operations: ListOperation.append(), ListOperation.get(), etc.
        - Map operations: MapOperation.put(), MapOperation.get_by_key(), etc.
        - Bit operations: BitOperation.set(), BitOperation.get(), etc.
        
        Args:
            operations: List of operations to execute atomically.
        
        Returns:
            The record with results of the operations, or None if the record
            doesn't exist and no read operations were performed.
        
        Example:
            # Put a value and get the record
            record = await session.key_value(key="mykey").operate([
                Operation.put("bin1", "value1"),
                Operation.get()
            ])
            
            # List append and get
            record = await session.key_value(key="mykey").operate([
                ListOperation.append("list_bin", "new_item", ListPolicy()),
                ListOperation.size("list_bin")
            ])
            
            # Map put and get
            record = await session.key_value(key="mykey").operate([
                MapOperation.put("map_bin", "key1", "value1", MapPolicy()),
                MapOperation.get_by_key("map_bin", "key1", MapReturnType.VALUE)
            ])
        """
        policy = self._write_policy or WritePolicy()
        key = self._get_key()
        try:
            return await self._client.operate(policy, key, operations)
        except ServerError as e:
            # Return None if key not found, re-raise other server errors
            if "KeyNotFoundError" in str(e):
                return None
            raise