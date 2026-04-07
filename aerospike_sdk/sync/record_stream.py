# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""SyncRecordStream — synchronous wrapper around :class:`RecordStream`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aerospike_sdk.record_result import RecordResult

if TYPE_CHECKING:
    from aerospike_sdk.record_stream import RecordStream
    from aerospike_sdk.sync.client import _EventLoopManager


class SyncRecordStream:
    """Blocking view of :class:`~aerospike_sdk.record_stream.RecordStream`.

    Iterator protocol and helpers (``first``, ``collect``, …) call into the
    underlying async stream via the parent client's event-loop manager, so
    each step may block the calling thread until data is ready.

    **Behavior vs async:** Ordering and per-row semantics match
    :class:`~aerospike_sdk.record_stream.RecordStream`; only the API surface
    is synchronous. ``close()`` closes the async stream directly without crossing
    the loop (releases resources; pending async iteration should stop).

    Example::

        with SyncClient("localhost:3000") as client:
            session = client.create_session()
            for row in session.query(ns, set).bins(["name"]).execute():
                if row.record:
                    print(row.record.bins)

    See Also:
        :class:`~aerospike_sdk.record_stream.RecordStream`
    """

    def __init__(self, stream: RecordStream, loop_manager: _EventLoopManager) -> None:
        """Wrap ``stream`` for synchronous consumption (internal use)."""
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
        """Return the first row, or ``None`` if the stream is empty.

        See Also:
            :meth:`~aerospike_sdk.record_stream.RecordStream.first`
        """
        return self._loop_manager.run_async(self._stream.first())

    def first_or_raise(self) -> RecordResult:
        """Return the first row or raise if the stream is empty or not OK.

        Raises:
            AerospikeError: On a failed row or empty-stream error from the
                async layer. See
                :meth:`~aerospike_sdk.record_stream.RecordStream.first_or_raise`.
        """
        return self._loop_manager.run_async(self._stream.first_or_raise())

    def first_udf_result(self) -> Any | None:
        """Return the first non-``None`` :attr:`~RecordResult.udf_result`."""
        return self._loop_manager.run_async(self._stream.first_udf_result())

    def collect(self) -> list[RecordResult]:
        """Drain the stream into a list (blocks until complete)."""
        return self._loop_manager.run_async(self._stream.collect())

    def failures(self) -> list[RecordResult]:
        """Collect rows whose :attr:`~RecordResult.result_code` is not OK."""
        return self._loop_manager.run_async(self._stream.failures())

    def close(self) -> None:
        """Release the underlying async stream."""
        self._stream.close()
