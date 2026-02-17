"""BatchExistsOperation - Builder for batch exists operations."""

from __future__ import annotations

from typing import List

from aerospike_async import Client, Key

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.record_stream import RecordStream


class BatchExistsOperation:
    """Builder for batch exists operations.

    Uses ``batch_read`` under the hood so that each result carries a full
    ``result_code`` (enabling callers to distinguish "not found" from errors
    via :meth:`RecordResult.as_bool`).

    Example::

        stream = await session.exists(users.ids("user1", "user2")).execute()
        async for result in stream:
            print(f"{result.key}: exists={result.as_bool()}")
    """

    def __init__(
        self,
        client: Client,
        keys: List[Key],
    ) -> None:
        self._client = client
        self._keys = keys
        self._respond_all_keys: bool = False

    def respond_all_keys(self) -> BatchExistsOperation:
        """Request that results be returned for all keys.

        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    async def execute(self) -> RecordStream:
        """Execute the batch exists operation.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.
            Use :meth:`RecordResult.as_bool` for a simple existence check.
        """
        try:
            results = await self._client.batch_read(
                None,  # batch_policy
                None,  # read_policy
                self._keys,
                [],    # empty bins list = header only
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_batch_records(results)
