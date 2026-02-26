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

from aerospike_fluent.aio.operations.query import QueryBinBuilder, QueryBuilder
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

    # -- Query stacking -------------------------------------------------------

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Chain another query with new key(s) for batch/point stacking."""
        self._qb.query(arg1, *more_keys)
        return self

    # -- Execute --------------------------------------------------------------

    def execute(self) -> SyncRecordStream:
        """Execute the query synchronously."""
        qb = self._qb

        async def _run():
            return await qb.execute()

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)
