"""BatchDeleteOperation - Builder for batch delete operations."""

from __future__ import annotations

from typing import List, Optional

from aerospike_async import Client, Key, WritePolicy

from aerospike_fluent.aio.operations.key_value import KeyValueOperation


class BatchDeleteOperation:
    """
    Builder for batch delete operations.
    
    This class handles batch deletes for multiple keys:
    session.delete(customerDataSet.ids(1,2,3)).execute()
    """

    def __init__(
        self,
        client: Client,
        keys: List[Key],
    ) -> None:
        """
        Initialize a BatchDeleteOperation.
        
        Args:
            client: The underlying async client.
            keys: List of keys to delete.
        """
        self._client = client
        self._keys = keys
        self._write_policy: Optional[WritePolicy] = None

    def with_write_policy(self, policy: WritePolicy) -> BatchDeleteOperation:
        """
        Set the write policy for this operation.
        
        Args:
            policy: The write policy to use.
        
        Returns:
            self for method chaining.
        """
        self._write_policy = policy
        return self

    def durably(self, durable: bool = True) -> BatchDeleteOperation:
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

    async def execute(self) -> None:
        """
        Execute the batch delete operation.
        
        This deletes all keys in the batch.
        """
        policy = self._write_policy or WritePolicy()
        # Delete all keys concurrently using the underlying async client
        import asyncio
        # self._client is the underlying async client (Client from aerospike_async)
        tasks = [self._client.delete(policy, key) for key in self._keys]
        await asyncio.gather(*tasks, return_exceptions=True)

