"""Recordset wrappers for single and batch key queries."""

from __future__ import annotations

from typing import List, Optional

from aerospike_async import Key, Record


class SingleRecordRecordset:
    """
    A Recordset wrapper for a single record (from a single key query).

    This allows single key queries to return a Recordset that can be
    iterated like a regular query result.
    """

    def __init__(self, record: Optional[Record], key: Optional[Key] = None) -> None:
        """
        Initialize a SingleRecordRecordset.

        Args:
            record: The single record, or None if not found.
            key: Optional Key object for the record.
        """
        self._record = record
        self._key = key
        self._iterated = False
        self._closed = False

    def __aiter__(self) -> SingleRecordRecordset:
        """Return self as an async iterator."""
        return self

    async def __anext__(self) -> Record:
        """
        Get the next record.

        Returns:
            A Record object.

        Raises:
            StopAsyncIteration: If already iterated or closed.
        """
        if self._closed:
            raise StopAsyncIteration
        if self._iterated:
            raise StopAsyncIteration
        if self._record is None:
            raise StopAsyncIteration

        self._iterated = True
        return self._record

    def close(self) -> None:
        """Close the recordset."""
        self._closed = True


class BatchRecordset:
    """
    A Recordset wrapper for multiple records (from a batch key query).

    This allows batch key queries to return a Recordset that can be
    iterated like a regular query result.
    """

    def __init__(self, records: List[tuple[Key, Optional[Record]]]) -> None:
        """
        Initialize a BatchRecordset.

        Args:
            records: List of (Key, Record) tuples from get_many().
        """
        self._records = records
        self._index = 0
        self._closed = False

    def __aiter__(self) -> BatchRecordset:
        """Return self as an async iterator."""
        return self

    async def __anext__(self) -> Record:
        """
        Get the next record.

        Returns:
            A Record object.

        Raises:
            StopAsyncIteration: If all records have been iterated or closed.
        """
        if self._closed:
            raise StopAsyncIteration

        while self._index < len(self._records):
            key, record = self._records[self._index]
            self._index += 1
            # Skip None records (keys that don't exist)
            if record is not None:
                return record

        raise StopAsyncIteration

    def close(self) -> None:
        """Close the recordset."""
        self._closed = True

