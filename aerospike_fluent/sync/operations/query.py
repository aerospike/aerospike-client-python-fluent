"""SyncQueryBuilder - Synchronous wrapper for query operations."""

from __future__ import annotations

from typing import Any, List, Optional, overload, Union

from aerospike_async import (
    BasePolicy,
    Filter,
    FilterExpression,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    Recordset,
    Replica,
)
from aerospike_fluent.dsl.parser import parse_dsl

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.operations.query import QueryBuilder
from aerospike_fluent.sync.client import _EventLoopManager


class SyncQueryBuilder:
    """
    Synchronous wrapper for QueryBuilder.

    Provides the same fluent interface as QueryBuilder but with
    synchronous methods that hide async/await.
    """

    def __init__(
        self,
        async_client: FluentClient,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
        query_builder: Optional[QueryBuilder] = None,
    ) -> None:
        """
        Initialize a SyncQueryBuilder.

        Args:
            async_client: The async FluentClient instance.
            namespace: The namespace name.
            set_name: The set name.
            loop_manager: The event loop manager for running async operations.
            query_builder: Optional QueryBuilder to preserve single_key/keys state.
        """
        self._async_client = async_client
        self._namespace = namespace
        self._set_name = set_name
        self._loop_manager = loop_manager
        self._query_builder = query_builder  # Preserve the original builder
        self._bins: Optional[List[str]] = None
        self._with_no_bins: bool = False
        self._filters: List[Filter] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._policy: Optional[QueryPolicy] = None
        self._partition_filter: Optional[PartitionFilter] = None
        self._chunk_size: Optional[int] = None
        self._fail_on_filtered_out: bool = False
        self._respond_all_keys: bool = False

    def _get_async_builder(self) -> QueryBuilder:
        """Get the underlying async query builder."""
        # If we have a preserved query builder, use it and apply our modifications
        if self._query_builder is not None:
            builder = self._query_builder
        else:
            builder = QueryBuilder(
                client=self._async_client._async_client,
                namespace=self._namespace,
                set_name=self._set_name,
            )

        # Apply our modifications
        if self._with_no_bins:
            builder.with_no_bins()
        elif self._bins is not None:
            builder.bins(self._bins)
        for filter_obj in self._filters:
            builder.filter(filter_obj)
        if self._filter_expression is not None:
            builder.filter_expression(self._filter_expression)
        if self._policy:
            builder.with_policy(self._policy)
        if self._partition_filter:
            builder.partition(self._partition_filter)
        if self._chunk_size is not None:
            builder.chunk_size(self._chunk_size)
        if self._fail_on_filtered_out:
            builder.fail_on_filtered_out()
        if self._respond_all_keys:
            builder.respond_all_keys()
        return builder

    def bins(self, bin_names: List[str]) -> SyncQueryBuilder:
        """
        Specify which bins to retrieve.

        This method cannot be used together with with_no_bins().

        Args:
            bin_names: List of bin names to retrieve.

        Returns:
            self for method chaining.

        Raises:
            ValueError: If used together with with_no_bins().
        """
        if self._with_no_bins:
            raise ValueError("Cannot specify both 'with_no_bins' and provide a list of bin names")
        self._bins = bin_names
        self._with_no_bins = False
        return self

    def with_no_bins(self) -> SyncQueryBuilder:
        """
        Specify that no bins should be read (header-only query).

        This method is useful when you only need to check for record existence
        or get metadata like generation numbers, without reading the actual data.

        This method cannot be used together with bins().

        Returns:
            self for method chaining.

        Raises:
            ValueError: If used together with bins().
        """
        if self._bins is not None:
            raise ValueError("Cannot specify both 'with_no_bins' and provide a list of bin names")
        self._with_no_bins = True
        self._bins = []
        return self

    def filter(self, filter_obj: Filter) -> SyncQueryBuilder:
        """Add a filter to the query."""
        self._filters.append(filter_obj)
        return self

    def filter_expression(self, expression: FilterExpression) -> SyncQueryBuilder:
        """Set a FilterExpression for server-side filtering."""
        self._filter_expression = expression
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
        """
        Set the query filter from a DSL string (optionally with format params) or a FilterExpression.

        Args:
            expression: Either a DSL string (e.g., "$.country == 'US' and $.order_total > 500"),
                a DSL string with format placeholders (e.g., "$.age > %s", "$.name == '%s'"),
                or a FilterExpression (e.g., Exp.gt(Exp.int_bin("a"), Exp.int_val(100))).
            *params: Optional values substituted into the DSL string via % formatting (same as JFC
                where(String dsl, Object... params)). Only used when expression is a str; ignored
                when expression is a FilterExpression. For string literals in DSL, include quotes
                in the template (e.g., "$.name == '%s'" with param "Tim").

        Returns:
            self for method chaining.

        Example:
            ```python
            # Using text DSL
            query = session.query(dataset).where("$.country == 'US' and $.order_total > 500")

            # Using DSL with params (aligns with JFC where(String dsl, Object... params))
            query = session.query(dataset).where("$.age > %s", 21)
            query = session.query(dataset).where("$.age > %s and $.name == '%s'", 30, "John")

            # Using FilterExpression (Exp)
            query = session.query(dataset).where(Exp.gt(Exp.int_bin("a"), Exp.int_val(100)))
            ```
        """
        if isinstance(expression, str):
            if params:
                dsl = expression % params
                self._filter_expression = parse_dsl(dsl)
            else:
                self._filter_expression = parse_dsl(expression)
        else:
            self._filter_expression = expression
        return self

    def with_policy(self, policy: QueryPolicy) -> SyncQueryBuilder:
        """Set the query policy."""
        self._policy = policy
        return self

    def partition(self, partition_filter: PartitionFilter) -> SyncQueryBuilder:
        """Set the partition filter."""
        self._partition_filter = partition_filter
        return self

    def on_partitions(self, *partition_ids: int) -> SyncQueryBuilder:
        """
        Set partitions to query by partition IDs.

        Args:
            *partition_ids: One or more partition IDs to query.

        Returns:
            self for method chaining.
        """
        if len(partition_ids) == 1:
            self._partition_filter = PartitionFilter.by_id(partition_ids[0])
        else:
            min_id = min(partition_ids)
            max_id = max(partition_ids)
            self._partition_filter = PartitionFilter.by_range(min_id, max_id + 1)
        return self

    def on_partition(self, part_id: int) -> SyncQueryBuilder:
        """
        Target a specific partition for the query.

        This method restricts the query to a single partition. This can be useful
        for load balancing or when you know the data distribution across partitions.

        Args:
            part_id: The partition ID to target (0-4095)

        Returns:
            self for method chaining

        Raises:
            ValueError: If part_id is out of range

        Example:
            ```python
            query = session.query(dataset).on_partition(5)
            ```
        """
        return self.on_partition_range(part_id, part_id + 1)

    def on_partition_range(self, start_incl: int, end_excl: int) -> SyncQueryBuilder:
        """
        Target a range of partitions for the query.

        This method restricts the query to a specific range of partitions. This
        can be useful for load balancing, parallel processing, or when you know
        the data distribution across partitions.

        The partition range can only be set once per query. Subsequent calls
        with different ranges will overwrite the previous range.

        Args:
            start_incl: Start partition (inclusive, 0-4095)
            end_excl: End partition (exclusive, 1-4096)

        Returns:
            self for method chaining

        Raises:
            ValueError: If partition range is invalid

        Example:
            ```python
            # Query partitions 0-2047 (first half)
            query = session.query(dataset).on_partition_range(0, 2048)

            # Query partitions 100-199
            query = session.query(dataset).on_partition_range(100, 200)
            ```
        """
        # Partition range validation
        if start_incl < 0 or start_incl >= 4096:
            raise ValueError(f"Start partition must be in range 0-4095, not {start_incl}")
        if end_excl < 1 or end_excl > 4096:
            raise ValueError(f"End partition must be in range 1-4096, not {end_excl}")
        if start_incl >= end_excl:
            raise ValueError(
                f"Start partition ({start_incl}) must be < end partition ({end_excl})"
            )

        self._partition_filter = PartitionFilter.by_range(start_incl, end_excl)
        return self

    def chunk_size(self, chunk_size: int) -> SyncQueryBuilder:
        """
        Set the chunk size for server-side streaming.

        This method controls how many records are fetched per chunk from the server
        when using server-side streaming. The chunk size affects memory usage and network
        round trips. This is distinct from client-side pagination.

        Args:
            chunk_size: The number of records per chunk (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If chunk_size is <= 0.
        """
        if chunk_size <= 0:
            raise ValueError(f"Chunk size must be > 0, not {chunk_size}")
        self._chunk_size = chunk_size
        return self

    def records_per_second(self, rps: int) -> SyncQueryBuilder:
        """
        Set the maximum records per second for the query.

        Args:
            rps: Maximum records per second to process.

        Returns:
            self for method chaining.
        """
        if self._policy is None:
            self._policy = QueryPolicy()
        self._policy.records_per_second = rps
        return self

    def max_records(self, max_records: int) -> SyncQueryBuilder:
        """
        Set the maximum number of records to return.

        Args:
            max_records: Maximum number of records to return.

        Returns:
            self for method chaining.
        """
        if self._policy is None:
            self._policy = QueryPolicy()
        self._policy.max_records = max_records
        return self

    def limit(self, limit: int) -> SyncQueryBuilder:
        """
        Set the maximum number of records to return (alias for max_records).

        This method is an alias for max_records().
        It limits the total number of records returned by the query.
        Once the limit is reached, the query will stop processing.

        Args:
            limit: Maximum number of records to return (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If limit is <= 0.

        Example:
            ```python
            query = session.query(dataset).limit(100)
            ```
        """
        if limit <= 0:
            raise ValueError(f"Limit must be > 0, not {limit}")
        return self.max_records(limit)

    def expected_duration(self, duration: QueryDuration) -> SyncQueryBuilder:
        """
        Set the expected duration of the query.

        Args:
            duration: Expected duration (QueryDuration.LONG, QueryDuration.SHORT, or QueryDuration.LONG_RELAX_AP).

        Returns:
            self for method chaining.
        """
        if self._policy is None:
            self._policy = QueryPolicy()
        self._policy.expected_duration = duration
        return self

    def replica(self, replica: Replica) -> SyncQueryBuilder:
        """
        Set the replica preference for the query.

        Args:
            replica: Replica preference (Replica.MASTER, Replica.SEQUENCE, or Replica.PREFER_RACK).

        Returns:
            self for method chaining.
        """
        if self._policy is None:
            self._policy = QueryPolicy()
        self._policy.replica = replica
        return self

    def base_policy(self, base_policy: BasePolicy) -> SyncQueryBuilder:
        """
        Set the base policy for the query.

        Args:
            base_policy: The base policy to use.

        Returns:
            self for method chaining.
        """
        if self._policy is None:
            self._policy = QueryPolicy()
        self._policy.base_policy = base_policy
        return self

    def fail_on_filtered_out(self) -> SyncQueryBuilder:
        """
        Include filtered-out records in the stream with FILTERED_OUT error code.

        If the query has a where clause and is provided either a single key or a list of keys,
        any records which are filtered out will appear in the stream against an exception
        code of FILTERED_OUT rather than just not appearing in the result stream.

        Returns:
            self for method chaining.
        """
        self._fail_on_filtered_out = True
        return self

    def respond_all_keys(self) -> SyncQueryBuilder:
        """
        Return null for missing keys instead of omitting them.

        By default, if a key is provided (or is part of a list of keys) but the key does not
        map to a record, then nothing will be returned in the stream against that key.
        However, if this flag is specified, null will be in the stream against that key.

        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    def execute(self) -> Recordset:
        """
        Execute the query synchronously.

        Returns:
            A Recordset that can be iterated synchronously.
            Note: The Recordset itself may still use async internally,
            but iteration will be synchronous.
        """
        builder = self._get_async_builder()
        # Execute the async query and get the recordset
        # The recordset iteration will need to be handled synchronously
        async def _execute():
            return await builder.execute()

        recordset = self._loop_manager.run_async(_execute())
        # Wrap the recordset to make iteration synchronous
        return _SyncRecordsetWrapper(recordset, self._loop_manager)


class _SyncRecordsetWrapper:
    """Wrapper to make async Recordset iteration synchronous."""

    def __init__(self, recordset: Recordset, loop_manager: _EventLoopManager) -> None:
        self._recordset = recordset
        self._loop_manager = loop_manager
        self._iterator = None

    def __iter__(self):
        """Make the recordset iterable synchronously."""
        return self

    def __next__(self):
        """Get the next record synchronously."""
        if self._iterator is None:
            # Create async iterator
            async def _get_iterator():
                return self._recordset.__aiter__()

            self._iterator = self._loop_manager.run_async(_get_iterator())

        # Get next item from async iterator
        async def _get_next():
            return await self._iterator.__anext__()

        try:
            return self._loop_manager.run_async(_get_next())
        except StopAsyncIteration:
            raise StopIteration

    def __aiter__(self):
        """Support async iteration as well."""
        return self._recordset.__aiter__()

    async def __anext__(self):
        """Support async iteration as well."""
        return await self._recordset.__anext__()

    def close(self) -> None:
        """Close the recordset."""
        if self._recordset is not None:
            async def _close():
                self._recordset.close()

            self._loop_manager.run_async(_close())
