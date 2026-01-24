"""SyncBatchDeleteOperation - Synchronous wrapper for batch delete operations."""

from __future__ import annotations

from typing import List, Optional

from aerospike_async import WritePolicy

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.operations.batch_delete import BatchDeleteOperation
from aerospike_fluent.sync.client import _EventLoopManager


class SyncBatchDeleteOperation:
    """
    Synchronous wrapper for BatchDeleteOperation.
    
    Provides the same fluent interface as BatchDeleteOperation but with
    synchronous methods that hide async/await.
    """

    def __init__(
        self,
        async_client: FluentClient,
        keys: List,
        loop_manager: _EventLoopManager,
    ) -> None:
        """
        Initialize a SyncBatchDeleteOperation.
        
        Args:
            async_client: The async FluentClient instance.
            keys: List of keys to delete.
            loop_manager: The event loop manager for running async operations.
        """
        self._async_client = async_client
        self._keys = keys
        self._loop_manager = loop_manager
        self._write_policy: Optional[WritePolicy] = None

    def _get_async_operation(self) -> BatchDeleteOperation:
        """Get the underlying async operation builder."""
        op = BatchDeleteOperation(self._async_client._async_client, self._keys)
        if self._write_policy:
            op.with_write_policy(self._write_policy)
        return op

    def with_write_policy(self, policy: WritePolicy) -> SyncBatchDeleteOperation:
        """Set the write policy for this operation."""
        self._write_policy = policy
        return self

    def durably(self, durable: bool = True) -> SyncBatchDeleteOperation:
        """
        Set whether the delete operations should be durable.
        
        Args:
            durable: If True, the deletes will be durable. If False, the deletes
                    will not be durable (default: True).
        
        Returns:
            self for method chaining.
        """
        if self._write_policy is None:
            self._write_policy = WritePolicy()
        self._write_policy.durable_delete = durable
        return self

    def execute(self) -> None:
        """
        Execute the batch delete operation.
        
        This deletes all keys in the batch synchronously.
        """
        op = self._get_async_operation()
        self._loop_manager.run_async(op.execute())


