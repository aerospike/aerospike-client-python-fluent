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

"""RecordStream — async iterable of RecordResult for batch and query operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from aerospike_async import Key, Record
from aerospike_async.exceptions import ResultCode

from aerospike_fluent.record_result import RecordResult, batch_records_to_results

if TYPE_CHECKING:
    from aerospike_fluent.exceptions import AerospikeError


class RecordStream:
    """Async iterator of :class:`~aerospike_fluent.record_result.RecordResult` rows.

    Produced by ``await session.query(...).execute()`` and similar APIs. Prefer
    ``async for row in stream``, or helpers such as :meth:`collect` and
    :meth:`first`. Do not call ``RecordStream(...)`` directly; use factories
    like :meth:`from_list` or :meth:`from_batch_records`.

    Example:
        Typical consumption with ``async for``::

            stream = await session.query(key).bins(["name"]).execute()
            async for row in stream:
                if row.is_ok and row.record:
                    print(row.record.bins)

    See Also:
        :meth:`first_or_raise`: Assert a single OK row.
    """

    def __init__(self, source: AsyncIterator[RecordResult]) -> None:
        self._source = source
        self._closed = False

    # -- factory constructors ------------------------------------------------

    @classmethod
    def from_list(cls, results: Sequence[RecordResult]) -> RecordStream:
        """Wrap an already-materialised list of results.

        Example::
            stream = RecordStream.from_list([row1, row2])
            rows = await stream.collect()
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            for r in results:
                yield r
        return cls(_iter())

    @classmethod
    def chain(cls, streams: Sequence[RecordStream]) -> RecordStream:
        """Yield all results from each stream in order.

        Example::
            combined = RecordStream.chain([stream_a, stream_b])
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            for st in streams:
                async for r in st:
                    yield r
        return cls(_iter())

    @classmethod
    def from_batch_records(cls, batch_records: Sequence) -> RecordStream:
        """Wrap a sequence of async-client ``BatchRecord`` objects.

        Example::
            stream = RecordStream.from_batch_records(batch_records)
        """
        return cls.from_list(batch_records_to_results(list(batch_records)))

    @classmethod
    def from_recordset(cls, recordset) -> RecordStream:
        """Wrap a ``Recordset`` (async iterable of ``Record``).

        Each yielded ``Record`` is converted to a :class:`RecordResult` with
        ``result_code=OK`` and ``index=-1`` (queries have no positional index).

        Example::
            stream = RecordStream.from_recordset(recordset)
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            async for record in recordset:
                key = record.key if hasattr(record, "key") and record.key is not None else Key("", "", 0)
                yield RecordResult(
                    key=key,
                    record=record,
                    result_code=ResultCode.OK,
                )
        return cls(_iter())

    @classmethod
    def from_single(cls, key: Key, record: Record | None) -> RecordStream:
        """Wrap a single-key result.

        Example::
            stream = RecordStream.from_single(key, record)
        """
        if record is None:
            results = [RecordResult(
                key=key,
                record=None,
                result_code=ResultCode.KEY_NOT_FOUND_ERROR,
                index=0,
            )]
        else:
            results = [RecordResult(
                key=key,
                record=record,
                result_code=ResultCode.OK,
                index=0,
            )]
        return cls.from_list(results)

    @classmethod
    def from_error(
        cls,
        key: Key,
        result_code: ResultCode,
        in_doubt: bool = False,
        exception: AerospikeError | None = None,
    ) -> RecordStream:
        """Wrap a single-key error as a one-element stream.

        Example::
            stream = RecordStream.from_error(key, ResultCode.TIMEOUT)
        """
        return cls.from_list([RecordResult(
            key=key,
            record=None,
            result_code=result_code,
            in_doubt=in_doubt,
            index=0,
            exception=exception,
        )])

    # -- async iteration -----------------------------------------------------

    def __aiter__(self) -> RecordStream:
        """Return ``self`` for ``async for`` iteration."""
        return self

    async def __anext__(self) -> RecordResult:
        """Yield the next :class:`~aerospike_fluent.record_result.RecordResult`.

        Raises:
            StopAsyncIteration: When the stream is exhausted or :meth:`close` was
                called before more rows were read.
        """
        if self._closed:
            raise StopAsyncIteration
        return await self._source.__anext__()

    # -- convenience methods -------------------------------------------------

    async def first(self) -> RecordResult | None:
        """Consume and return the first row, or ``None`` if there are no rows.

        Returns:
            The first :class:`~aerospike_fluent.record_result.RecordResult`, or
            ``None`` when the stream is empty.

        Note:
            This advances the iterator; remaining rows are left for further
            ``async for`` or other helpers only if the underlying source allows
            partial consumption (most fluent streams are single-pass).

        Example::
            stream = await session.query(key).execute()
            row = await stream.first()
            if row is None:
                ...
        """
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def first_or_raise(self) -> RecordResult:
        """Return the first row and require success (see :meth:`RecordResult.or_raise`).

        Returns:
            The first OK :class:`~aerospike_fluent.record_result.RecordResult`.

        Raises:
            StopAsyncIteration: If the stream yields no rows (empty).
            AerospikeError: If the first row is not OK (from :meth:`RecordResult.or_raise`).

        Example:
            rec = (await stream.first_or_raise()).record_or_raise()
        """
        result = await self.first()
        if result is None:
            raise StopAsyncIteration("RecordStream is empty")
        return result.or_raise()

    async def first_udf_result(self) -> Any | None:
        """Scan forward for the first non-``None`` :attr:`~aerospike_fluent.record_result.RecordResult.udf_result`.

        Returns:
            The UDF return value, or ``None`` if no row carries a UDF result.

        Example::
            value = await stream.first_udf_result()

        See Also:
            :meth:`Session.execute_udf`: Produces streams with UDF results.
        """
        async for r in self:
            if r.udf_result is not None:
                return r.udf_result
        return None

    async def collect(self) -> list[RecordResult]:
        """Drain the stream into a list (order preserved).

        Returns:
            All remaining :class:`~aerospike_fluent.record_result.RecordResult`
            instances.

        Example:
            rows = await stream.collect()
            oks = [r for r in rows if r.is_ok]
        """
        results: list[RecordResult] = []
        async for r in self:
            results.append(r)
        return results

    async def failures(self) -> list[RecordResult]:
        """Drain the stream and return rows where :attr:`~aerospike_fluent.record_result.RecordResult.is_ok` is false.

        Returns:
            Only error or non-OK rows.

        Note:
            Like :meth:`collect`, this consumes the entire stream.
        """
        return [r async for r in self if not r.is_ok]

    def close(self) -> None:
        """Mark the stream closed; further :meth:`__anext__` calls stop iteration.

        Idempotent. Use when abandoning a stream early to cooperate with
        resource cleanup where supported.
        """
        self._closed = True
