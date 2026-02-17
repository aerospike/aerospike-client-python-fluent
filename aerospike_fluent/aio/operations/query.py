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

"""QueryBuilder - Builder for query operations."""

from __future__ import annotations

from typing import Any, List, Optional, overload, Union

from aerospike_async import (
    BasePolicy,
    Client,
    Filter,
    FilterExpression,
    Key,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
    Statement,
)
from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.record_stream import RecordStream


class QueryBuilder:
    """
    Builder for query operations.
    
    This class provides a fluent interface for building and executing
    queries with filters, bin selection, and policies.
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
    ) -> None:
        """
        Initialize a QueryBuilder.
        
        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._bins: Optional[List[str]] = None
        self._with_no_bins: bool = False
        self._filters: List[Filter] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._policy: Optional[QueryPolicy] = None
        self._partition_filter: Optional[PartitionFilter] = None
        self._chunk_size: Optional[int] = None
        self._fail_on_filtered_out: bool = False
        self._respond_all_keys: bool = False
        # For single key or batch key queries
        self._single_key: Optional[Key] = None
        self._keys: Optional[List[Key]] = None
        self._read_policy: Optional[ReadPolicy] = None

    def bins(self, bin_names: List[str]) -> QueryBuilder:
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
    
    def with_no_bins(self) -> QueryBuilder:
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

    def filter(self, filter_obj: Filter) -> QueryBuilder:
        """
        Add a filter to the query.
        
        Args:
            filter_obj: The filter to add.
        
        Returns:
            self for method chaining.
        """
        self._filters.append(filter_obj)
        return self

    def filter_expression(self, expression: FilterExpression) -> QueryBuilder:
        """
        Set a FilterExpression for server-side filtering.

        FilterExpression allows complex server-side filtering that doesn't
        require secondary indexes. This is more efficient than client-side
        filtering as it reduces network traffic and processing.

        Args:
            expression: The FilterExpression to apply.

        Returns:
            self for method chaining.

        Example:
            # Filter by multiple conditions server-side
            filter_exp = FilterExpression.and_([
                FilterExpression.eq(
                    FilterExpression.string_bin("category"),
                    FilterExpression.string_val("Shoes")
                ),
                FilterExpression.eq(
                    FilterExpression.string_bin("usage"),
                    FilterExpression.string_val("Sports")
                )
            ])
            recordset = await client.query("test", "products").filter_expression(filter_exp).execute()
        """
        self._filter_expression = expression
        return self
    
    @overload
    def where(self, expression: str) -> QueryBuilder: ...

    @overload
    def where(self, expression: str, *params: Any) -> QueryBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> QueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> QueryBuilder:
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

    def with_policy(self, policy: QueryPolicy) -> QueryBuilder:
        """
        Set the query policy.
        
        Args:
            policy: The query policy to use.
        
        Returns:
            self for method chaining.
        """
        self._policy = policy
        return self

    def with_read_policy(self, policy: ReadPolicy) -> QueryBuilder:
        """
        Set the read policy (for single key or batch key queries).
        
        Args:
            policy: The read policy to use.
        
        Returns:
            self for method chaining.
        """
        self._read_policy = policy
        return self

    def partition(self, partition_filter: PartitionFilter) -> QueryBuilder:
        """
        Set the partition filter.
        
        Args:
            partition_filter: The partition filter to use.
        
        Returns:
            self for method chaining.
        """
        self._partition_filter = partition_filter
        return self

    def on_partitions(self, *partition_ids: int) -> QueryBuilder:
        """
        Set partitions to query by partition IDs.
        
        Args:
            *partition_ids: One or more partition IDs to query.
        
        Returns:
            self for method chaining.
            
        Example:
            ```python
            query = session.query(dataset).on_partitions(1, 2, 3)
            ```
        """
        if len(partition_ids) == 1:
            self._partition_filter = PartitionFilter.by_id(partition_ids[0])
        else:
            # For multiple partitions, we need to use a range or multiple filters
            # Since PartitionFilter.by_id only takes one ID, we'll use by_range
            # for now. This is a limitation of the underlying client.
            min_id = min(partition_ids)
            max_id = max(partition_ids)
            self._partition_filter = PartitionFilter.by_range(min_id, max_id + 1)
        return self

    def on_partition(self, part_id: int) -> QueryBuilder:
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

    def on_partition_range(self, start_incl: int, end_excl: int) -> QueryBuilder:
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

    def chunk_size(self, chunk_size: int) -> QueryBuilder:
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
            
        Example:
            ```python
            query = session.query(dataset).chunk_size(100)
            ```
        """
        if chunk_size <= 0:
            raise ValueError(f"Chunk size must be > 0, not {chunk_size}")
        self._chunk_size = chunk_size
        return self

    def records_per_second(self, rps: int) -> QueryBuilder:
        """
        Set the maximum records per second for the query.
        
        Args:
            rps: Maximum records per second to process.
        
        Returns:
            self for method chaining.
            
        Example:
            ```python
            query = session.query(dataset).records_per_second(1000)
            ```
        """
        self._ensure_policy().records_per_second = rps
        return self

    def max_records(self, max_records: int) -> QueryBuilder:
        """
        Set the maximum number of records to return.
        
        Args:
            max_records: Maximum number of records to return.
        
        Returns:
            self for method chaining.
            
        Example:
            ```python
            query = session.query(dataset).max_records(10000)
            ```
        """
        self._ensure_policy().max_records = max_records
        return self

    def limit(self, limit: int) -> QueryBuilder:
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

    def expected_duration(self, duration: "QueryDuration") -> QueryBuilder:
        """
        Set the expected duration of the query.
        
        Args:
            duration: Expected duration (QueryDuration.LONG, QueryDuration.SHORT, or QueryDuration.LONG_RELAX_AP).
        
        Returns:
            self for method chaining.
            
        Example:
            ```python
            from aerospike_async import QueryDuration
            query = session.query(dataset).expected_duration(QueryDuration.SHORT)
            ```
        """
        self._ensure_policy().expected_duration = duration
        return self

    def replica(self, replica: "Replica") -> QueryBuilder:
        """
        Set the replica preference for the query.
        
        Args:
            replica: Replica preference (Replica.MASTER, Replica.SEQUENCE, or Replica.PREFER_RACK).
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            from aerospike_async import Replica
            query = session.query(dataset).replica(Replica.SEQUENCE)
            ```
        """
        self._ensure_policy().replica = replica
        return self

    def base_policy(self, base_policy: "BasePolicy") -> QueryBuilder:
        """
        Set the base policy for the query.
        
        Args:
            base_policy: The base policy to use.
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            from aerospike_async import BasePolicy
            base = BasePolicy()
            query = session.query(dataset).base_policy(base)
            ```
        """
        self._ensure_policy().base_policy = base_policy
        return self
    
    def fail_on_filtered_out(self) -> QueryBuilder:
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
    
    def respond_all_keys(self) -> QueryBuilder:
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

    def _ensure_policy(self) -> QueryPolicy:
        """Return the existing policy or create a default one."""
        if self._policy is None:
            self._policy = QueryPolicy()
        return self._policy

    def _build_statement(self) -> Statement:
        """Build a Statement object from the builder configuration."""
        # If bins is None, pass None to Statement (which means all bins)
        # If bins is an empty list, pass [] (which means no bins)
        # If bins is a non-empty list, pass it as-is (which means specific bins)
        bins = self._bins  # None means all bins, [] means no bins, [names] means specific bins
        statement = Statement(self._namespace, self._set_name, bins)
        if self._filters:
            statement.filters = self._filters
        # Note: filter_expression is set on the policy, not the statement
        return statement

    async def execute(self) -> RecordStream:
        """Execute the query and return a :class:`RecordStream`.

        For single key queries, this performs a direct ``get()`` operation.
        For batch key queries, this performs a ``batch_read()`` operation.
        For dataset queries, this performs a standard ``query()`` operation.

        Returns:
            A :class:`RecordStream` of :class:`RecordResult` items.
        """
        # Handle single key query
        if self._single_key is not None:
            read_policy = self._read_policy or ReadPolicy()
            try:
                record = await self._client.get(read_policy, self._single_key, self._bins)
            except Exception as e:
                raise convert_pac_exception(e) from e
            return RecordStream.from_single(self._single_key, record)

        # Handle batch key query via batch_read for full per-key result codes
        if self._keys is not None:
            try:
                batch_records = await self._client.batch_read(
                    None,  # batch_policy
                    None,  # read_policy
                    self._keys,
                    self._bins,
                )
            except Exception as e:
                raise convert_pac_exception(e) from e
            return RecordStream.from_batch_records(batch_records)

        # Handle dataset query (standard query)
        policy = self._policy or QueryPolicy()
        if self._chunk_size is not None and self._chunk_size > 0:
            policy.max_records = self._chunk_size
        if self._filter_expression is not None:
            policy.filter_expression = self._filter_expression
        partition_filter = self._partition_filter or PartitionFilter.all()
        statement = self._build_statement()

        try:
            recordset = await self._client.query(policy, partition_filter, statement)
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_recordset(recordset)
