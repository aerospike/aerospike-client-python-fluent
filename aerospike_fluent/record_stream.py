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

from typing import AsyncIterator, Sequence

from aerospike_async import Key, Record
from aerospike_async.exceptions import ResultCode

from aerospike_fluent.record_result import RecordResult, batch_records_to_results


class RecordStream:
    """Async iterable stream of :class:`RecordResult`.

    Construct via the class methods :meth:`from_list` or :meth:`from_recordset`
    rather than calling ``__init__`` directly.
    """

    def __init__(self, source: AsyncIterator[RecordResult]) -> None:
        self._source = source
        self._closed = False

    # -- factory constructors ------------------------------------------------

    @classmethod
    def from_list(cls, results: Sequence[RecordResult]) -> RecordStream:
        """Wrap an already-materialised list of results."""
        async def _iter() -> AsyncIterator[RecordResult]:
            for r in results:
                yield r
        return cls(_iter())

    @classmethod
    def from_batch_records(cls, batch_records: Sequence) -> RecordStream:
        """Wrap a sequence of PAC ``BatchRecord`` objects."""
        return cls.from_list(batch_records_to_results(list(batch_records)))

    @classmethod
    def from_recordset(cls, recordset) -> RecordStream:
        """Wrap a PAC ``Recordset`` (async iterable of ``Record``).

        Each yielded ``Record`` is converted to a :class:`RecordResult` with
        ``result_code=OK`` and ``index=-1`` (queries have no positional index).
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
        """Wrap a single-key result."""
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
    ) -> RecordStream:
        """Wrap a single-key error as a one-element stream."""
        return cls.from_list([RecordResult(
            key=key,
            record=None,
            result_code=result_code,
            in_doubt=in_doubt,
            index=0,
        )])

    # -- async iteration -----------------------------------------------------

    def __aiter__(self) -> RecordStream:
        return self

    async def __anext__(self) -> RecordResult:
        if self._closed:
            raise StopAsyncIteration
        return await self._source.__anext__()

    # -- convenience methods -------------------------------------------------

    async def first(self) -> RecordResult | None:
        """Return the first result, or ``None`` if the stream is empty."""
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def first_or_raise(self) -> RecordResult:
        """Return the first result (calling :meth:`RecordResult.or_raise`).

        Raises if the stream is empty or the first result is not OK.
        """
        result = await self.first()
        if result is None:
            raise StopAsyncIteration("RecordStream is empty")
        return result.or_raise()

    async def collect(self) -> list[RecordResult]:
        """Materialise the entire stream into a list."""
        results: list[RecordResult] = []
        async for r in self:
            results.append(r)
        return results

    async def failures(self) -> list[RecordResult]:
        """Materialise and return only non-OK results."""
        return [r async for r in self if not r.is_ok]

    def close(self) -> None:
        """Close the stream; further iteration will stop."""
        self._closed = True
