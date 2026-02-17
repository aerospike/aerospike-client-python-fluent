"""BatchDeleteOperation - Builder for batch delete operations."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from aerospike_async import Client, Key, WritePolicy

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.record_stream import RecordStream


class BatchDeleteOperation:
    """
    Builder for batch delete operations.
    
    This class handles batch deletes for multiple keys using the
    underlying async client's batch_delete for optimal performance.
    
    Example:
        ```python
        # Delete multiple keys
        results = await session.delete(users.ids("user1", "user2", "user3")).execute()
        
        # Check results
        for i, result in enumerate(results):
            print(f"Key {i}: deleted={result.result_code == 0}")
        ```
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
        self._respond_all_keys: bool = False

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

    def respond_all_keys(self) -> BatchDeleteOperation:
        """
        Request that results be returned for all keys, not just failures.
        
        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    async def execute(self) -> RecordStream:
        """Execute the batch delete operation.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.
        """
        try:
            results = await self._client.batch_delete(
                None,  # batch_policy
                None,  # delete_policy
                self._keys,
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_batch_records(results)

