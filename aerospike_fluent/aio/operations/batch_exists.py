"""BatchExistsOperation - Builder for batch exists operations."""

from __future__ import annotations

from typing import List

from aerospike_async import Client, Key

from aerospike_fluent.exceptions import convert_pac_exception


class BatchExistsOperation:
    """
    Builder for batch exists operations.
    
    This class checks if multiple keys exist using the
    underlying async client's batch_exists for optimal performance.
    
    Example:
        ```python
        # Check if multiple keys exist
        results = await session.exists(users.ids("user1", "user2", "user3")).execute()
        
        # Check results
        for i, exists in enumerate(results):
            print(f"Key {i}: exists={exists}")
        ```
    """

    def __init__(
        self,
        client: Client,
        keys: List[Key],
    ) -> None:
        """
        Initialize a BatchExistsOperation.
        
        Args:
            client: The underlying async client.
            keys: List of keys to check.
        """
        self._client = client
        self._keys = keys
        self._respond_all_keys: bool = False

    def respond_all_keys(self) -> BatchExistsOperation:
        """
        Request that results be returned for all keys.
        
        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    async def execute(self) -> List[bool]:
        """
        Execute the batch exists operation.
        
        Returns:
            List of boolean values indicating if each key exists.
        """
        try:
            results = await self._client.batch_exists(
                None,  # batch_policy
                None,  # read_policy
                self._keys,
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
        return results
