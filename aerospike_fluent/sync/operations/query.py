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

"""SyncQueryBuilder - Synchronous delegation wrapper for query operations."""

from __future__ import annotations

from typing import Any, List, Optional, overload, Union

from aerospike_async import (
    BasePolicy,
    Filter,
    FilterExpression,
    Key,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
)

from aerospike_fluent.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)
from aerospike_fluent.sync.client import _EventLoopManager
from aerospike_fluent.sync.record_stream import SyncRecordStream


class SyncQueryBuilder:
    """Synchronous wrapper for :class:`QueryBuilder`.

    All builder methods delegate directly to the underlying async
    ``QueryBuilder``.  ``execute()`` runs the async execute on the
    event loop and returns a :class:`SyncRecordStream`.
    """

    def __init__(
        self,
        async_client: Any,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
        query_builder: Optional[QueryBuilder] = None,
    ) -> None:
        self._loop_manager = loop_manager
        self._qb: QueryBuilder = query_builder if query_builder is not None else QueryBuilder(
            client=async_client, namespace=namespace, set_name=set_name,
        )

    # -- Bin projection / selection -------------------------------------------

    def bins(self, bin_names: List[str]) -> SyncQueryBuilder:
        """Specify which bins to retrieve."""
        self._qb.bins(bin_names)
        return self

    def bin(self, bin_name: str) -> QueryBinBuilder[SyncQueryBuilder]:
        """Start a bin-level read operation."""
        return QueryBinBuilder(self, bin_name)

    def add_operation(self, op: Any) -> None:
        """Append a read operation produced by a bin or CDT builder."""
        self._qb.add_operation(op)

    def with_no_bins(self) -> SyncQueryBuilder:
        """Specify that no bins should be read (header-only query)."""
        self._qb.with_no_bins()
        return self

    # -- Filtering ------------------------------------------------------------

    def filter(self, filter_obj: Filter) -> SyncQueryBuilder:
        """Add a filter to the query."""
        self._qb.filter(filter_obj)
        return self

    def filter_expression(self, expression: FilterExpression) -> SyncQueryBuilder:
        """Set a FilterExpression for server-side filtering."""
        self._qb.filter_expression(expression)
        return self

    @overload
    def where(self, expression: str) -> SyncQueryBuilder: ...

    @overload
    def where(self, expression: str, *params: Any) -> SyncQueryBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> SyncQueryBuilder:
        """Set the query filter from a DSL string or FilterExpression."""
        self._qb.where(expression, *params)
        return self

    # -- Policy / options -----------------------------------------------------

    def with_policy(self, policy: QueryPolicy) -> SyncQueryBuilder:
        """Set the query policy."""
        self._qb.with_policy(policy)
        return self

    def with_read_policy(self, policy: ReadPolicy) -> SyncQueryBuilder:
        """Set the read policy (for single key or batch key queries)."""
        self._qb.with_read_policy(policy)
        return self

    def partition(self, partition_filter: PartitionFilter) -> SyncQueryBuilder:
        """Set the partition filter."""
        self._qb.partition(partition_filter)
        return self

    def on_partitions(self, *partition_ids: int) -> SyncQueryBuilder:
        """Set partitions to query by partition IDs."""
        self._qb.on_partitions(*partition_ids)
        return self

    def on_partition(self, part_id: int) -> SyncQueryBuilder:
        """Target a specific partition for the query."""
        self._qb.on_partition(part_id)
        return self

    def on_partition_range(self, start_incl: int, end_excl: int) -> SyncQueryBuilder:
        """Target a range of partitions for the query."""
        self._qb.on_partition_range(start_incl, end_excl)
        return self

    def chunk_size(self, chunk_size: int) -> SyncQueryBuilder:
        """Set the chunk size for server-side streaming."""
        self._qb.chunk_size(chunk_size)
        return self

    def records_per_second(self, rps: int) -> SyncQueryBuilder:
        """Set the maximum records per second for the query."""
        self._qb.records_per_second(rps)
        return self

    def max_records(self, max_records: int) -> SyncQueryBuilder:
        """Set the maximum number of records to return."""
        self._qb.max_records(max_records)
        return self

    def limit(self, limit: int) -> SyncQueryBuilder:
        """Set the maximum number of records to return (alias for max_records)."""
        self._qb.limit(limit)
        return self

    def expected_duration(self, duration: QueryDuration) -> SyncQueryBuilder:
        """Set the expected duration of the query."""
        self._qb.expected_duration(duration)
        return self

    def replica(self, replica: Replica) -> SyncQueryBuilder:
        """Set the replica preference for the query."""
        self._qb.replica(replica)
        return self

    def base_policy(self, base_policy: BasePolicy) -> SyncQueryBuilder:
        """Set the base policy for the query."""
        self._qb.base_policy(base_policy)
        return self

    def fail_on_filtered_out(self) -> SyncQueryBuilder:
        """Include filtered-out records in the stream with FILTERED_OUT code."""
        self._qb.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncQueryBuilder:
        """Return null for missing keys instead of omitting them."""
        self._qb.respond_all_keys()
        return self

    # -- Chain-level defaults -------------------------------------------------

    @overload
    def default_where(self, expression: str) -> SyncQueryBuilder: ...

    @overload
    def default_where(self, expression: str, *params: Any) -> SyncQueryBuilder: ...

    @overload
    def default_where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> SyncQueryBuilder:
        """Set a default filter expression for all specs that lack their own."""
        self._qb.default_where(expression, *params)
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> SyncQueryBuilder:
        """Set a default TTL applied to specs that lack their own."""
        self._qb.default_expire_record_after_seconds(seconds)
        return self

    # -- Query stacking -------------------------------------------------------

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Chain another query with new key(s) for batch/point stacking."""
        self._qb.query(arg1, *more_keys)
        return self

    # -- Write transitions ----------------------------------------------------

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an upsert write segment."""
        wsb = self._qb.upsert(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an insert segment."""
        wsb = self._qb.insert(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an update segment."""
        wsb = self._qb.update(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a replace segment."""
        wsb = self._qb.replace(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a replace-if-exists segment."""
        wsb = self._qb.replace_if_exists(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a delete segment."""
        wsb = self._qb.delete(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    # -- Execute --------------------------------------------------------------

    def execute(self) -> SyncRecordStream:
        """Execute the query synchronously."""
        qb = self._qb

        async def _run():
            return await qb.execute()

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteSegmentBuilder:
    """Synchronous wrapper for :class:`WriteSegmentBuilder`."""

    __slots__ = ("_wsb", "_loop_manager")

    def __init__(
        self, wsb: WriteSegmentBuilder, loop_manager: _EventLoopManager,
    ) -> None:
        self._wsb = wsb
        self._loop_manager = loop_manager

    # -- Bin operations -------------------------------------------------------

    def bin(self, bin_name: str) -> SyncWriteBinBuilder:
        """Start a bin-level write operation."""
        return SyncWriteBinBuilder(self, bin_name)

    def put(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Set multiple bins at once."""
        self._wsb.put(bins)
        return self

    def set_bins(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`put`."""
        return self.put(bins)

    # -- Transition methods ---------------------------------------------------

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Finalize current write segment and start a read segment."""
        qb = self._wsb.query(arg1, *more_keys)
        return SyncQueryBuilder(
            None, "", "", self._loop_manager, query_builder=qb,
        )

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an upsert segment."""
        self._wsb.upsert(arg1, *more_keys)
        return self

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an insert segment."""
        self._wsb.insert(arg1, *more_keys)
        return self

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start an update segment."""
        self._wsb.update(arg1, *more_keys)
        return self

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a replace segment."""
        self._wsb.replace(arg1, *more_keys)
        return self

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a replace-if-exists segment."""
        self._wsb.replace_if_exists(arg1, *more_keys)
        return self

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize current segment and start a delete segment."""
        self._wsb.delete(arg1, *more_keys)
        return self

    # -- Per-spec settings ----------------------------------------------------

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> SyncWriteSegmentBuilder:
        """Set a filter expression on the current write segment."""
        self._wsb.where(expression, *params)
        return self

    def expire_record_after_seconds(self, seconds: int) -> SyncWriteSegmentBuilder:
        """Set the TTL on the current write segment."""
        self._wsb.expire_record_after_seconds(seconds)
        return self

    def ensure_generation_is(self, generation: int) -> SyncWriteSegmentBuilder:
        """Set expected generation for optimistic locking."""
        self._wsb.ensure_generation_is(generation)
        return self

    def durably_delete(self) -> SyncWriteSegmentBuilder:
        """Enable durable delete on the current segment."""
        self._wsb.durably_delete()
        return self

    # -- Execution ------------------------------------------------------------

    def execute(self) -> SyncRecordStream:
        """Execute all accumulated specs synchronously."""
        wsb = self._wsb

        async def _run():
            return await wsb.execute()

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteBinBuilder:
    """Synchronous wrapper for bin-level write operations."""

    __slots__ = ("_sync_segment", "_wbb")

    def __init__(
        self, sync_segment: SyncWriteSegmentBuilder, bin_name: str,
    ) -> None:
        self._sync_segment = sync_segment
        self._wbb = WriteBinBuilder(sync_segment._wsb, bin_name)

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> SyncWriteSegmentBuilder:
        """Set the bin to *value*."""
        self._wbb.set_to(value)
        return self._sync_segment

    def increment_by(self, value: Any) -> SyncWriteSegmentBuilder:
        """Increment the bin by *value*."""
        self._wbb.increment_by(value)
        return self._sync_segment

    def append(self, value: str) -> SyncWriteSegmentBuilder:
        """Append a string to the bin."""
        self._wbb.append(value)
        return self._sync_segment

    def prepend(self, value: str) -> SyncWriteSegmentBuilder:
        """Prepend a string to the bin."""
        self._wbb.prepend(value)
        return self._sync_segment

    def remove(self) -> SyncWriteSegmentBuilder:
        """Delete the bin from the record."""
        self._wbb.remove()
        return self._sync_segment

    # -- Expression operations ------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Read a computed value into this bin using a DSL expression."""
        self._wbb.select_from(expression, ignore_eval_failure=ignore_eval_failure)
        return self._sync_segment

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        self._wbb.insert_from(
            expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self._sync_segment

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        self._wbb.update_from(
            expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self._sync_segment

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        self._wbb.upsert_from(
            expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self._sync_segment

    # -- Convenience transitions (delegate to segment) ------------------------

    def bin(self, bin_name: str) -> SyncWriteBinBuilder:
        """Start the next bin operation."""
        return SyncWriteBinBuilder(self._sync_segment, bin_name)

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Shortcut: finalize write segment and start a read segment."""
        return self._sync_segment.query(arg1, *more_keys)

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start an upsert segment."""
        return self._sync_segment.upsert(arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start an insert segment."""
        return self._sync_segment.insert(arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start an update segment."""
        return self._sync_segment.update(arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start a replace segment."""
        return self._sync_segment.replace(arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start a replace-if-exists segment."""
        return self._sync_segment.replace_if_exists(arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Shortcut: finalize and start a delete segment."""
        return self._sync_segment.delete(arg1, *more_keys)

    def execute(self) -> SyncRecordStream:
        """Shortcut: execute all accumulated specs."""
        return self._sync_segment.execute()
