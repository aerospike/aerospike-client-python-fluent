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

"""Synchronous query and write-verb builders delegating to ``aio.operations.query``."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union, overload

from aerospike_async import (
    BasePolicy,
    CTX,
    ExecuteTask,
    Filter,
    FilterExpression,
    Key,
    ListOperation,
    ListOrderType,
    ListPolicy,
    ListReturnType,
    ListSortFlags,
    MapOperation,
    MapOrder,
    MapPolicy,
    MapReturnType,
    MapWriteFlags,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
)

from aerospike_fluent.aio.operations.cdt_read import _map_item_pairs
from aerospike_fluent.aio.operations.cdt_write import (
    CdtWriteBuilder,
    CdtWriteInvertableBuilder,
)
from aerospike_fluent.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteSegmentBuilder,
)
from aerospike_fluent.error_strategy import OnError
from aerospike_fluent.sync.client import _EventLoopManager
from aerospike_fluent.sync.record_stream import SyncRecordStream


class _SyncWriteVerbs:
    """Mixin mirroring async write-verb entry points on :class:`QueryBuilder`.

    Subclasses implement ``_start_write_verb`` to open a
    :class:`SyncWriteSegmentBuilder`. Semantics match
    :class:`~aerospike_fluent.aio.operations.query.QueryBuilder` /
    :class:`~aerospike_fluent.aio.session.Session`.
    """

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        raise NotImplementedError

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an upsert write segment."""
        return self._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an insert (create-only) segment."""
        return self._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an update (update-only) segment."""
        return self._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a replace segment."""
        return self._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a replace-if-exists segment."""
        return self._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a delete segment."""
        return self._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a touch segment (reset TTL)."""
        return self._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an exists-check segment."""
        return self._start_write_verb("exists", arg1, *more_keys)


class SyncQueryBuilder(_SyncWriteVerbs):
    """Configure and run reads, queries, and write segments synchronously.

    Every chain method forwards to :class:`~aerospike_fluent.aio.operations.query.QueryBuilder`.
    :meth:`execute` blocks on the owning loop manager and returns
    :class:`~aerospike_fluent.sync.record_stream.SyncRecordStream`. Detailed
    parameter semantics (filters, policies, CDT, ``on_error``) are documented
    on the async builder.

    See Also:
        :class:`~aerospike_fluent.aio.operations.query.QueryBuilder`
    """

    def __init__(
        self,
        async_client: Any,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
        query_builder: Optional[QueryBuilder] = None,
    ) -> None:
        """Attach or create an async :class:`QueryBuilder` and the sync loop."""
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

    def with_write_operations(
        self, operations: Sequence[Any],
    ) -> SyncQueryBuilder:
        """Attach scalar write operations for a background dataset task."""
        self._qb.with_write_operations(operations)
        return self

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
    def where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncQueryBuilder:
        """Set the query filter from a DSL string or FilterExpression."""
        self._qb.where(expression)
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
    def default_where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncQueryBuilder:
        """Set a default filter for all chained operations that lack their own."""
        self._qb.default_where(expression)
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> SyncQueryBuilder:
        """Set a default TTL for all chained operations that lack their own."""
        self._qb.default_expire_record_after_seconds(seconds)
        return self

    def default_never_expire(self) -> SyncQueryBuilder:
        """Set the default TTL to never expire (TTL = -1)."""
        self._qb.default_never_expire()
        return self

    def default_with_no_change_in_expiration(self) -> SyncQueryBuilder:
        """Set the default to preserve each record's existing TTL (TTL = -2)."""
        self._qb.default_with_no_change_in_expiration()
        return self

    def default_expiry_from_server_default(self) -> SyncQueryBuilder:
        """Set the default TTL to the namespace's server default (TTL = 0)."""
        self._qb.default_expiry_from_server_default()
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

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        wsb = self._qb._start_write_verb(op_type, arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    # -- Execute --------------------------------------------------------------

    def execute_background_task(self) -> ExecuteTask:
        """Run a background write for this dataset query (see ``QueryBuilder``)."""
        qb = self._qb

        async def _run():
            return await qb.execute_background_task()

        return self._loop_manager.run_async(_run())

    def execute_udf_background_task(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Run a background UDF for this dataset query (see ``QueryBuilder``)."""
        qb = self._qb

        async def _run():
            return await qb.execute_udf_background_task(
                package_name, function_name, args)

        return self._loop_manager.run_async(_run())

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Run the configured query or write chain and block until the stream is ready.

        Args:
            on_error: Same as :meth:`~aerospike_fluent.aio.operations.query.QueryBuilder.execute`
                (:class:`~aerospike_fluent.error_strategy.ErrorStrategy` or callback).

        Returns:
            :class:`~aerospike_fluent.sync.record_stream.SyncRecordStream`.

        See Also:
            :meth:`~aerospike_fluent.aio.operations.query.QueryBuilder.execute`
        """
        qb = self._qb

        async def _run():
            return await qb.execute(on_error)

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteSegmentBuilder(_SyncWriteVerbs):
    """Synchronous multi-key write segment (mirrors :class:`WriteSegmentBuilder`).

    Bin scalars, CDT, expressions, and policies delegate to the embedded async
    segment; :meth:`execute` returns :class:`~aerospike_fluent.sync.record_stream.SyncRecordStream`.

    See Also:
        :class:`~aerospike_fluent.aio.operations.query.WriteSegmentBuilder`
    """

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

    def add_operation(self, op: Any) -> None:
        """Append an operation (used by CDT action builders)."""
        self._wsb.add_operation(op)

    def put(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Set multiple bins at once."""
        self._wsb.put(bins)
        return self

    def set_bins(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`put`."""
        return self.put(bins)

    # -- Scalar bin operations (direct on segment) ----------------------------

    def set_to(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Set a bin to *value*."""
        self._wsb.set_to(bin_name, value)
        return self

    def add(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Add a numeric *value* to a bin."""
        self._wsb.add(bin_name, value)
        return self

    def increment_by(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`add`."""
        return self.add(bin_name, value)

    def get(self, bin_name: str) -> SyncWriteSegmentBuilder:
        """Read a bin value back within a write operate."""
        self._wsb.get(bin_name)
        return self

    def append(self, bin_name: str, value: str) -> SyncWriteSegmentBuilder:
        """Append a string to a bin."""
        self._wsb.append(bin_name, value)
        return self

    def prepend(self, bin_name: str, value: str) -> SyncWriteSegmentBuilder:
        """Prepend a string to a bin."""
        self._wsb.prepend(bin_name, value)
        return self

    def remove_bin(self, bin_name: str) -> SyncWriteSegmentBuilder:
        """Delete a bin from the record."""
        self._wsb.remove_bin(bin_name)
        return self

    # -- Expression operations (direct on segment) ----------------------------

    def select_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Read a computed value into a bin using a DSL expression."""
        self._wsb.select_from(bin_name, expression, ignore_eval_failure=ignore_eval_failure)
        return self

    def insert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        self._wsb.insert_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    def update_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        self._wsb.update_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    def upsert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        self._wsb.upsert_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    # -- Transition methods ---------------------------------------------------

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Finalize current write segment and start a read segment."""
        qb = self._wsb.query(arg1, *more_keys)
        return SyncQueryBuilder(
            None, "", "", self._loop_manager, query_builder=qb,
        )

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        self._wsb._start_write_verb(op_type, arg1, *more_keys)
        return self

    # -- Per-operation settings ------------------------------------------------

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncWriteSegmentBuilder:
        """Set a filter expression on the current write segment."""
        self._wsb.where(expression)
        return self

    def expire_record_after_seconds(self, seconds: int) -> SyncWriteSegmentBuilder:
        """Set the TTL on the current write segment."""
        self._wsb.expire_record_after_seconds(seconds)
        return self

    def never_expire(self) -> SyncWriteSegmentBuilder:
        """Set this record to never expire (TTL = -1)."""
        self._wsb.never_expire()
        return self

    def with_no_change_in_expiration(self) -> SyncWriteSegmentBuilder:
        """Preserve the record's existing TTL (TTL = -2)."""
        self._wsb.with_no_change_in_expiration()
        return self

    def expiry_from_server_default(self) -> SyncWriteSegmentBuilder:
        """Use the namespace's default TTL for this record (TTL = 0)."""
        self._wsb.expiry_from_server_default()
        return self

    def ensure_generation_is(self, generation: int) -> SyncWriteSegmentBuilder:
        """Set expected generation for optimistic locking."""
        self._wsb.ensure_generation_is(generation)
        return self

    def durably_delete(self) -> SyncWriteSegmentBuilder:
        """Enable durable delete on the current segment."""
        self._wsb.durably_delete()
        return self

    def respond_all_keys(self) -> SyncWriteSegmentBuilder:
        """Include results for missing keys in the stream."""
        self._wsb.respond_all_keys()
        return self

    def fail_on_filtered_out(self) -> SyncWriteSegmentBuilder:
        """Mark filtered-out records with ``FILTERED_OUT`` result code."""
        self._wsb.fail_on_filtered_out()
        return self

    def replace_only(self) -> SyncWriteSegmentBuilder:
        """Change the current segment to replace-if-exists semantics."""
        self._wsb.replace_only()
        return self

    # -- Execution ------------------------------------------------------------

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Flush accumulated write operations and return a synchronous result stream.

        Args:
            on_error: Same as :meth:`SyncQueryBuilder.execute`.

        Returns:
            :class:`~aerospike_fluent.sync.record_stream.SyncRecordStream`.

        See Also:
            :meth:`~aerospike_fluent.aio.operations.query.WriteSegmentBuilder.execute`
        """
        wsb = self._wsb

        async def _run():
            return await wsb.execute(on_error)

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteBinBuilder(_SyncWriteVerbs):
    """Synchronous wrapper for bin-level write operations.

    Per-bin write builder that captures a bin name and delegates
    all operations to the parent ``SyncWriteSegmentBuilder``.
    """

    __slots__ = ("_sync_segment", "_bin")

    def __init__(
        self, sync_segment: SyncWriteSegmentBuilder, bin_name: str,
    ) -> None:
        self._sync_segment = sync_segment
        self._bin = bin_name

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> SyncWriteSegmentBuilder:
        """Set the bin to *value*.

        Args:
            value: New value to store.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.set_to(self._bin, value)

    def add(self, value: Any) -> SyncWriteSegmentBuilder:
        """Add a numeric *value* to the bin.

        Args:
            value: Numeric value to add.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.add(self._bin, value)

    def increment_by(self, value: Any) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`add`.

        Args:
            value: Numeric value to add.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self.add(value)

    def append(self, value: str) -> SyncWriteSegmentBuilder:
        """Append a string to the bin.

        Args:
            value: String to append.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.append(self._bin, value)

    def prepend(self, value: str) -> SyncWriteSegmentBuilder:
        """Prepend a string to the bin.

        Args:
            value: String to prepend.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.prepend(self._bin, value)

    def remove(self) -> SyncWriteSegmentBuilder:
        """Delete the bin from the record.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.remove_bin(self._bin)

    def get(self) -> SyncWriteSegmentBuilder:
        """Read the bin value back within a write operate.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.get(self._bin)

    # -- CDT list structural operations ---------------------------------------

    def list_add(self, value: Any) -> SyncWriteSegmentBuilder:
        """Add *value* to an ordered list (sorted insert).

        Args:
            value: Value to insert.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.append(self._bin, value, ListPolicy(ListOrderType.ORDERED, None)),
        )
        return self._sync_segment

    def list_append(self, value: Any) -> SyncWriteSegmentBuilder:
        """Append *value* to the end of an unordered list.

        Args:
            value: Value to append.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.append(self._bin, value, ListPolicy(None, None)),
        )
        return self._sync_segment

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> SyncWriteSegmentBuilder:
        """Remove all entries from the map bin.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.clear(self._bin))
        return self._sync_segment

    def map_size(self) -> SyncWriteSegmentBuilder:
        """Return the map element count (read within operate).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.size(self._bin))
        return self._sync_segment

    def map_upsert_items(self, items: Any) -> SyncWriteSegmentBuilder:
        """Put multiple map entries (create or update each key).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, MapPolicy(None, None)),
        )
        return self._sync_segment

    def map_insert_items(self, items: Any) -> SyncWriteSegmentBuilder:
        """Put map entries only for keys that do not yet exist.

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.CREATE_ONLY)
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, policy),
        )
        return self._sync_segment

    def map_update_items(self, items: Any) -> SyncWriteSegmentBuilder:
        """Update existing map entries only (no new keys).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.UPDATE_ONLY)
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, policy),
        )
        return self._sync_segment

    def map_create(self, order: MapOrder) -> SyncWriteSegmentBuilder:
        """Create an empty map with the given key order.

        Args:
            order: Key sort order for the map.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.create(self._bin, order))
        return self._sync_segment

    def map_set_policy(self, order: MapOrder) -> SyncWriteSegmentBuilder:
        """Set map sort order policy without changing entries.

        Args:
            order: Key sort order to apply.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            MapOperation.set_map_policy(self._bin, MapPolicy(order, None)),
        )
        return self._sync_segment

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> SyncWriteSegmentBuilder:
        """Remove all elements from the list bin.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.clear(self._bin))
        return self._sync_segment

    def list_sort(
        self, flags: ListSortFlags = ListSortFlags.DEFAULT,
    ) -> SyncWriteSegmentBuilder:
        """Sort the list bin.

        Args:
            flags: Sort behavior flags (default ``DEFAULT``).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.sort(self._bin, flags))
        return self._sync_segment

    def list_size(self) -> SyncWriteSegmentBuilder:
        """Return the list element count (read within operate).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.size(self._bin))
        return self._sync_segment

    def list_append_items(self, items: Any) -> SyncWriteSegmentBuilder:
        """Append values to an unordered list.

        Args:
            items: Values to append.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.append_items(
                self._bin, items, ListPolicy(None, None),
            ),
        )
        return self._sync_segment

    def list_add_items(self, items: Any) -> SyncWriteSegmentBuilder:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Values to insert.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.append_items(
                self._bin, items, ListPolicy(ListOrderType.ORDERED, None),
            ),
        )
        return self._sync_segment

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Create an empty list with the given order.

        Args:
            order: Element ordering.
            pad: If ``True``, allow sparse indexes.
            persist_index: If ``True``, maintain a persistent index.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.create(self._bin, order, pad, persist_index),
        )
        return self._sync_segment

    def list_set_order(self, order: ListOrderType) -> SyncWriteSegmentBuilder:
        """Set list sort order without changing elements.

        Args:
            order: Sort order to apply.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.set_order(self._bin, order))
        return self._sync_segment

    # -- Map navigation (singular -> CdtWriteBuilder) -------------------------

    def on_map_index(self, index: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by index.

        Args:
            index: Map index to target.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            lambda rt: MapOperation.remove_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(self, key: Any) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        _mp = MapPolicy(None, None)
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            lambda rt: MapOperation.remove_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_key(key),
            set_to_factory=lambda v: MapOperation.put(b, key, v, _mp),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _mp),
        )

    def on_map_rank(self, rank: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            lambda rt: MapOperation.remove_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
        )

    # -- Map navigation (invertable -> CdtWriteInvertableBuilder) -------------

    def on_map_value(self, value: Any) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            lambda rt: MapOperation.remove_by_value(b, value, rt),
            MapReturnType, is_map=True,
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by index range.

        Args:
            index: Start index.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_index_range_from(b, index, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range_from(b, index, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_index_range(b, index, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range(b, index, count, rt)
        return CdtWriteInvertableBuilder(
            self._sync_segment, get_f, rm_f, MapReturnType, is_map=True,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by key range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_key_range(b, start, end, rt),
            MapReturnType, is_map=True,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by rank range.

        Args:
            rank: Start rank (0 = lowest value).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_rank_range_from(b, rank, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range_from(b, rank, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_rank_range(b, rank, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range(b, rank, count, rt)
        return CdtWriteInvertableBuilder(
            self._sync_segment, get_f, rm_f, MapReturnType, is_map=True,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_value_range(b, start, end, rt),
            MapReturnType, is_map=True,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Anchor key.
            index: Relative index offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            lambda rt: MapOperation.remove_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            MapReturnType, is_map=True,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map entries by value rank range relative to an anchor value.

        Args:
            value: Anchor value.
            rank: Relative rank offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: MapOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            MapReturnType, is_map=True,
        )

    def on_map_key_list(self, keys: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a list of keys.

        Args:
            keys: Map keys to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            lambda rt: MapOperation.remove_by_key_list(b, keys, rt),
            MapReturnType, is_map=True,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            lambda rt: MapOperation.remove_by_value_list(b, values, rt),
            MapReturnType, is_map=True,
        )

    # -- List navigation (singular -> CdtWriteBuilder) ------------------------

    def on_list_index(self, index: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a list element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            lambda rt: ListOperation.remove_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_index(index),
        )

    def on_list_rank(self, rank: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a list element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            lambda rt: ListOperation.remove_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- List navigation (invertable -> CdtWriteInvertableBuilder) ------------

    def on_list_value(self, value: Any) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            lambda rt: ListOperation.remove_by_value(b, value, rt),
            ListReturnType, is_map=False,
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by index range.

        Args:
            index: Start index.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            lambda rt: ListOperation.remove_by_index_range(b, index, count, rt),
            ListReturnType, is_map=False,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by rank range.

        Args:
            rank: Start rank (0 = lowest value).
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            lambda rt: ListOperation.remove_by_rank_range(b, rank, count, rt),
            ListReturnType, is_map=False,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            lambda rt: ListOperation.remove_by_value_range(b, start, end, rt),
            ListReturnType, is_map=False,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by value rank range relative to an anchor value.

        Args:
            value: Anchor value.
            rank: Relative rank offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: ListOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            ListReturnType, is_map=False,
        )

    def on_list_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            lambda rt: ListOperation.remove_by_value_list(b, values, rt),
            ListReturnType, is_map=False,
        )

    # -- Expression operations ------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Read a computed value into this bin using a DSL expression.

        Args:
            expression: DSL string or ``FilterExpression``.
            ignore_eval_failure: If ``True``, suppress evaluation errors.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.select_from(
            self._bin, expression, ignore_eval_failure=ignore_eval_failure,
        )

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin does not already exist.

        Args:
            expression: DSL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.insert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin already exists.

        Args:
            expression: DSL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.update_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin.

        Args:
            expression: DSL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.upsert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    # -- Convenience transitions (delegate to segment) ------------------------

    def bin(self, bin_name: str) -> SyncWriteBinBuilder:
        """Start the next bin operation.

        Args:
            bin_name: Name of the next bin.

        Returns:
            :class:`SyncWriteBinBuilder` for the named bin.
        """
        return SyncWriteBinBuilder(self._sync_segment, bin_name)

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Shortcut: finalize write segment and start a read segment.

        Args:
            arg1: Key or list of keys.
            more_keys: Additional keys.

        Returns:
            :class:`SyncQueryBuilder` for read operations.
        """
        return self._sync_segment.query(arg1, *more_keys)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        return self._sync_segment._start_write_verb(op_type, arg1, *more_keys)

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Shortcut: execute all accumulated operations.

        Args:
            on_error: Error handling strategy.

        Returns:
            :class:`SyncRecordStream` with operation results.
        """
        return self._sync_segment.execute(on_error)
