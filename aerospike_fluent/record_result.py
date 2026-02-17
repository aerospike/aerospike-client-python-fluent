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

"""RecordResult — per-record outcome for batch and query operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aerospike_async import Key, Record
from aerospike_async.exceptions import ResultCode

if TYPE_CHECKING:
    from aerospike_async import BatchRecord


@dataclass(frozen=True, slots=True)
class RecordResult:
    """Result of a single record operation within a batch or query.

    Attributes:
        key: The record key (always present).
        record: The record data, or ``None`` on failure / not found.
        result_code: Server result code (``ResultCode.OK`` on success).
        in_doubt: ``True`` when a write may have completed despite the error.
        index: Position in the batch (``-1`` for non-batch results).
    """

    key: Key
    record: Record | None
    result_code: ResultCode
    in_doubt: bool = False
    index: int = -1

    @property
    def is_ok(self) -> bool:
        """``True`` when the operation succeeded."""
        return self.result_code == ResultCode.OK

    def or_raise(self) -> RecordResult:
        """Return *self* if the result is OK, otherwise raise a typed exception."""
        if not self.is_ok:
            from aerospike_fluent.exceptions import result_code_to_exception

            raise result_code_to_exception(
                self.result_code, str(self.result_code), self.in_doubt
            )
        return self

    def record_or_raise(self) -> Record:
        """Return the record if OK and non-``None``, otherwise raise."""
        self.or_raise()
        if self.record is None:
            raise ValueError("Record is None despite ResultCode.OK")
        return self.record

    def as_bool(self) -> bool:
        """Existence check: ``True`` if OK, ``False`` if key not found, else raise."""
        if self.is_ok:
            return True
        if self.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            return False
        self.or_raise()
        return False  # unreachable


def batch_records_to_results(
    batch_records: list[BatchRecord] | tuple[BatchRecord, ...],
) -> list[RecordResult]:
    """Convert a sequence of PAC ``BatchRecord`` to ``RecordResult`` list."""
    return [
        RecordResult(
            key=br.key,
            record=br.record,
            result_code=br.result_code if br.result_code is not None else ResultCode.OK,
            in_doubt=br.in_doubt,
            index=i,
        )
        for i, br in enumerate(batch_records)
    ]
