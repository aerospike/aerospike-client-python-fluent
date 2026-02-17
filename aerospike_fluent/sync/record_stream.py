"""SyncRecordStream — synchronous wrapper around :class:`RecordStream`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aerospike_fluent.record_result import RecordResult

if TYPE_CHECKING:
    from aerospike_fluent.record_stream import RecordStream
    from aerospike_fluent.sync.client import _EventLoopManager


class SyncRecordStream:
    """Synchronous iterable of :class:`RecordResult`.

    Wraps an async :class:`RecordStream` and delegates to the event-loop
    manager for each blocking call, following the existing PFC sync pattern.
    """

    def __init__(self, stream: RecordStream, loop_manager: _EventLoopManager) -> None:
        self._stream = stream
        self._loop_manager = loop_manager

    # -- sync iteration ------------------------------------------------------

    def __iter__(self) -> SyncRecordStream:
        return self

    def __next__(self) -> RecordResult:
        async def _next() -> RecordResult:
            return await self._stream.__anext__()

        try:
            return self._loop_manager.run_async(_next())
        except StopAsyncIteration:
            raise StopIteration

    # -- convenience methods -------------------------------------------------

    def first(self) -> RecordResult | None:
        """Return the first result, or ``None`` if the stream is empty."""
        return self._loop_manager.run_async(self._stream.first())

    def first_or_raise(self) -> RecordResult:
        """Return the first result (raising if not OK or empty)."""
        return self._loop_manager.run_async(self._stream.first_or_raise())

    def collect(self) -> list[RecordResult]:
        """Materialise the entire stream into a list."""
        return self._loop_manager.run_async(self._stream.collect())

    def failures(self) -> list[RecordResult]:
        """Materialise and return only non-OK results."""
        return self._loop_manager.run_async(self._stream.failures())

    def close(self) -> None:
        """Close the underlying stream."""
        self._stream.close()
