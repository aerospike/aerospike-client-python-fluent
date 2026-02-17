"""BatchOperationBuilder - Builder for chaining operations across multiple keys."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from aerospike_async import (
    Client,
    Key,
    Operation,
    WritePolicy,
    RecordExistsAction,
)

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.record_stream import RecordStream


class BatchOpType(Enum):
    """Type of batch operation."""
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"
    REPLACE = "replace"
    REPLACE_IF_EXISTS = "replace_if_exists"
    DELETE = "delete"


class BatchBinBuilder:
    """
    Builder for chaining bin operations within a batch key operation.
    
    Example:
        batch.insert(key).bin("name").set_to("Alice").bin("age").set_to(25)
    """
    
    def __init__(self, key_op: BatchKeyOperationBuilder, bin_name: str) -> None:
        self._key_op = key_op
        self._bin_name = bin_name
    
    def set_to(self, value: Any) -> BatchKeyOperationBuilder:
        """
        Set a bin value.
        
        Args:
            value: The value to set.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._bins[self._bin_name] = value
        self._key_op._operations.append(Operation.put(self._bin_name, value))
        return self._key_op
    
    def increment_by(self, value: int) -> BatchKeyOperationBuilder:
        """
        Increment a bin value.
        
        Args:
            value: The amount to increment by.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._operations.append(Operation.add(self._bin_name, value))
        return self._key_op
    
    def append(self, value: str) -> BatchKeyOperationBuilder:
        """
        Append a string to a bin value.
        
        Args:
            value: The string to append.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._operations.append(Operation.append(self._bin_name, value))
        return self._key_op
    
    def prepend(self, value: str) -> BatchKeyOperationBuilder:
        """
        Prepend a string to a bin value.
        
        Args:
            value: The string to prepend.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._operations.append(Operation.prepend(self._bin_name, value))
        return self._key_op


class BatchKeyOperationBuilder:
    """
    Builder for a single key's operation within a batch.
    
    This class allows chaining bin operations and then continuing
    to add more keys to the batch.
    
    Example:
        batch.insert(key1).bin("name").set_to("Alice") \\
             .update(key2).bin("counter").increment_by(1)
    """
    
    def __init__(
        self,
        batch: BatchOperationBuilder,
        key: Key,
        op_type: BatchOpType,
    ) -> None:
        self._batch = batch
        self._key = key
        self._op_type = op_type
        self._bins: Dict[str, Any] = {}
        self._operations: List[Operation] = []
    
    def bin(self, bin_name: str) -> BatchBinBuilder:
        """
        Start a bin operation chain.
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            A BatchBinBuilder for chaining bin operations.
        
        Example:
            batch.insert(key).bin("name").set_to("Alice").bin("age").set_to(25)
        """
        return BatchBinBuilder(self, bin_name)
    
    def put(self, bins: Dict[str, Any]) -> BatchKeyOperationBuilder:
        """
        Set multiple bins at once.
        
        Args:
            bins: Dictionary of bin name to value mappings.
        
        Returns:
            self for method chaining.
        
        Example:
            batch.insert(key).put({"name": "Alice", "age": 25})
        """
        self._bins.update(bins)
        for bin_name, value in bins.items():
            self._operations.append(Operation.put(bin_name, value))
        return self
    
    # Methods to continue chaining to more keys (delegate to batch)
    
    def insert(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an insert operation for another key."""
        return self._batch.insert(key)
    
    def update(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an update operation for another key."""
        return self._batch.update(key)
    
    def upsert(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an upsert operation for another key."""
        return self._batch.upsert(key)
    
    def replace(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a replace operation for another key."""
        return self._batch.replace(key)
    
    def replace_if_exists(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a replace-if-exists operation for another key."""
        return self._batch.replace_if_exists(key)
    
    def delete(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a delete operation for another key."""
        return self._batch.delete(key)
    
    async def execute(self) -> RecordStream:
        """Execute all batch operations."""
        return await self._batch.execute()


class BatchOperationBuilder:
    """
    Builder for chaining operations across multiple keys.
    
    This class enables fluent chaining of operations on different keys,
    which are then executed as a single batch operation.
    
    Example:
        ```python
        results = await session.batch() \\
            .insert(key1).bin("name").set_to("Alice").bin("age").set_to(25) \\
            .update(key2).bin("counter").increment_by(1) \\
            .delete(key3) \\
            .execute()
        ```
    
    The operations are collected and executed together using the async
    client's batch_operate method for optimal performance.
    """
    
    def __init__(self, client: Client) -> None:
        """
        Initialize a BatchOperationBuilder.
        
        Args:
            client: The underlying async client.
        """
        self._client = client
        self._key_operations: List[BatchKeyOperationBuilder] = []
    
    def insert(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an insert (create only) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.insert(key).bin("name").set_to("Alice")
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.INSERT)
        self._key_operations.append(op)
        return op
    
    def update(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an update (update only) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.update(key).bin("counter").increment_by(1)
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.UPDATE)
        self._key_operations.append(op)
        return op
    
    def upsert(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an upsert (create or update) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.upsert(key).bin("name").set_to("Bob")
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.UPSERT)
        self._key_operations.append(op)
        return op
    
    def replace(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a replace (create or replace) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.replace(key).put({"name": "Charlie", "age": 35})
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.REPLACE)
        self._key_operations.append(op)
        return op
    
    def replace_if_exists(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a replace-if-exists operation for a key.
        
        This operation will fail if the record does not exist.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.replace_if_exists(key).put({"name": "Updated", "status": "active"})
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.REPLACE_IF_EXISTS)
        self._key_operations.append(op)
        return op
    
    def delete(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a delete operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for continuing the chain.
        
        Example:
            batch.delete(key1).delete(key2).execute()
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.DELETE)
        self._key_operations.append(op)
        return op
    
    async def execute(self) -> RecordStream:
        """Execute all batch operations.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.

        Raises:
            ValueError: If no operations have been added.
        """
        if not self._key_operations:
            raise ValueError("No operations to execute. Add operations with insert(), update(), etc.")

        # Separate delete operations from others (they use batch_delete)
        delete_keys: List[Key] = []
        operate_keys: List[Key] = []
        operate_ops: List[List[Operation]] = []

        for key_op in self._key_operations:
            if key_op._op_type == BatchOpType.DELETE:
                delete_keys.append(key_op._key)
            else:
                operate_keys.append(key_op._key)
                # Build operations list for this key
                ops = key_op._operations.copy()

                # If no operations but we have bins, convert to put operations
                if not ops and key_op._bins:
                    for bin_name, value in key_op._bins.items():
                        ops.append(Operation.put(bin_name, value))

                # If still no operations, add a touch to make it valid
                if not ops:
                    ops.append(Operation.touch())

                operate_ops.append(ops)

        raw_results: list = []

        try:
            if delete_keys:
                delete_results = await self._client.batch_delete(
                    None,  # batch_policy
                    None,  # delete_policy
                    delete_keys,
                )
                raw_results.extend(delete_results)

            if operate_keys:
                operate_results = await self._client.batch_operate(
                    None,  # batch_policy
                    None,  # write_policy
                    operate_keys,
                    operate_ops,
                )
                raw_results.extend(operate_results)
        except Exception as e:
            raise convert_pac_exception(e) from e

        return RecordStream.from_batch_records(raw_results)
