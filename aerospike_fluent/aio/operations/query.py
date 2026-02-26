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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, List, Optional, TypeVar, overload, Union

from aerospike_async import (
    BasePolicy,
    Client,
    Filter,
    FilterExpression,
    Key,
    ListOperation,
    ListReturnType,
    MapOperation,
    MapReturnType,
    Operation,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
    Statement,
    WritePolicy,
)
from aerospike_async.exceptions import (
    ResultCode,
    ServerError as PacServerError,
)

from aerospike_fluent.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
)
from aerospike_fluent.policy.policy_mapper import (
    to_batch_policy,
    to_query_policy,
    to_read_policy,
)

from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.exceptions import AerospikeError, convert_pac_exception
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape
from aerospike_fluent.record_stream import RecordStream

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


@dataclass(slots=True)
class _OperationSpec:
    """A single query segment in a stacked query chain.

    Mirrors JFC ``OperationSpec`` for read-only (query) operations only.
    Each spec captures the keys, accumulated bin operations, projected bins,
    and optional filter expression for one ``.query()`` call in a chain.
    """

    keys: List[Key]
    operations: List[Any] = field(default_factory=list)
    bins: Optional[List[str]] = None
    filter_expression: Optional[FilterExpression] = None


_T = TypeVar("_T")


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
        behavior: Optional[Behavior] = None,
    ) -> None:
        """
        Initialize a QueryBuilder.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            behavior: Optional Behavior for deriving policies.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._behavior = behavior
        self._bins: Optional[List[str]] = None
        self._with_no_bins: bool = False
        self._filters: List[Filter] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._policy: Optional[QueryPolicy] = None
        self._partition_filter: Optional[PartitionFilter] = None
        self._chunk_size: Optional[int] = None
        self._fail_on_filtered_out: bool = False
        self._respond_all_keys: bool = False
        self._operations: List[Any] = []
        self._specs: List[_OperationSpec] = []
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
    
    def bin(self, bin_name: str) -> QueryBinBuilder[QueryBuilder]:
        """Start a bin-level read operation.

        Returns a :class:`QueryBinBuilder` for specifying how to read from
        the named bin (simple get, CDT navigation, or expression read).

        Args:
            bin_name: The bin to operate on.

        Returns:
            A QueryBinBuilder for method chaining.

        Example::

            rs = await session.query(users.id(1)) \\
                .bin("settings").on_map_key("theme").get_values() \\
                .bin("age").get() \\
                .execute()
        """
        return QueryBinBuilder(self, bin_name)

    def add_operation(self, op: Any) -> None:
        """Append a read operation produced by a bin or CDT builder."""
        self._operations.append(op)

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

    # -- Query stacking -------------------------------------------------------

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> QueryBuilder:
        """Chain another query with new key(s) for batch/point stacking.

        Finalizes the current query segment and begins a new one with
        the given key(s).  Each segment can have its own bins, operations,
        and filter expression.  Dataset (index) queries cannot be stacked.

        Args:
            arg1: A single :class:`Key` or a ``List[Key]``.
            *more_keys: Additional keys (varargs).

        Returns:
            ``self`` for method chaining.

        Raises:
            ValueError: If the current query is a dataset query (no keys).

        Example::

            rs = await session.query(users.ids(1, 2, 3)) \\
                .bin("map").get() \\
                .query(users.ids(4, 5, 6)) \\
                .bin("name").get() \\
                .execute()
        """
        if (self._single_key is None and self._keys is None
                and not self._specs):
            raise ValueError(
                "Dataset (index) queries cannot be stacked. "
                "Query stacking is only supported for key-based queries."
            )

        self._finalize_current_spec()

        if isinstance(arg1, list):
            if not arg1:
                raise ValueError("keys list cannot be empty")
            self._keys = list(arg1) + list(more_keys) if more_keys else arg1
        elif isinstance(arg1, Key):
            if more_keys:
                self._keys = [arg1, *more_keys]
            else:
                self._single_key = arg1
        else:
            raise TypeError(
                f"query() requires a Key or List[Key], got {type(arg1).__name__}. "
                "Dataset queries cannot be stacked."
            )

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

        Handles single-key, batch-key, and dataset queries.  When query
        stacking is used (multiple ``.query()`` calls), each accumulated
        spec is executed and results are combined into a single stream.

        Returns:
            A :class:`RecordStream` of :class:`RecordResult` items.
        """
        self._finalize_current_spec()

        if self._specs:
            if len(self._specs) == 1:
                return await self._execute_spec(self._specs[0])
            all_results: list = []
            for spec in self._specs:
                stream = await self._execute_spec(spec)
                all_results.extend(await stream.collect())
            return RecordStream.from_list(all_results)

        # Dataset query path (no keys were specified)
        if self._operations:
            raise AerospikeError(
                "Bin-level read operations are not supported on dataset/index "
                "queries (requires Advanced Bin Projection, not yet available)",
                result_code=ResultCode.OP_NOT_APPLICABLE,
            )
        return await self._execute_dataset_query()

    # -- Private execute helpers ----------------------------------------------

    def _finalize_current_spec(self) -> None:
        """Package the current key/ops/bins/filter state into an _OperationSpec."""
        if self._single_key is not None:
            keys = [self._single_key]
        elif self._keys is not None:
            keys = self._keys
        else:
            return

        self._specs.append(_OperationSpec(
            keys=keys,
            operations=list(self._operations),
            bins=self._bins,
            filter_expression=self._filter_expression,
        ))

        self._single_key = None
        self._keys = None
        self._operations = []
        self._bins = None
        self._with_no_bins = False
        self._filter_expression = None

    async def _execute_spec(self, spec: _OperationSpec) -> RecordStream:
        """Execute a single :class:`_OperationSpec`."""
        has_ops = bool(spec.operations)
        keys = spec.keys

        if len(keys) == 1:
            if has_ops:
                return await self._execute_single_key_operate(
                    keys[0], spec.operations)
            return await self._execute_single_key_read(keys[0], spec.bins)

        if has_ops:
            return await self._execute_batch_operate(keys, spec.operations)
        return await self._execute_batch_read(keys, spec.bins)

    @staticmethod
    def _should_include_result(
        result_code: ResultCode,
        respond_all_keys: bool,
        fail_on_filtered_out: bool,
    ) -> bool:
        """Decide whether to include a result in the stream.

        Mirrors the JFC ``OperationSpecExecutor.shouldIncludeResult`` logic.
        """
        if result_code == ResultCode.OK:
            return True
        if result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            return respond_all_keys
        if result_code == ResultCode.FILTERED_OUT:
            return fail_on_filtered_out or respond_all_keys
        return True

    def _wrap_single_key_error(
        self, key: Key, exc: Exception,
    ) -> RecordStream:
        """Convert a PAC exception into a ``RecordStream``.

        Mirrors the JFC ``executeSingleKey`` catch block: the error is
        wrapped as a single-element stream (or an empty stream when
        ``shouldIncludeResult`` returns ``False``).
        """
        rc = (
            exc.result_code
            if isinstance(exc, PacServerError)
            else ResultCode.CLIENT_ERROR
        )
        in_doubt = getattr(exc, "in_doubt", False)
        if self._should_include_result(
            rc, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream.from_error(key, rc, in_doubt)
        return RecordStream.from_list([])

    async def _execute_single_key_read(
        self, key: Key, bins: Optional[List[str]],
    ) -> RecordStream:
        if self._read_policy is not None:
            read_policy = self._read_policy
        elif self._behavior is not None:
            read_policy = to_read_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        else:
            read_policy = ReadPolicy()
        try:
            record = await self._client.get(read_policy, key, bins)
        except Exception as e:
            return self._wrap_single_key_error(key, e)
        return RecordStream.from_single(key, record)

    async def _execute_single_key_operate(
        self, key: Key, operations: List[Any],
    ) -> RecordStream:
        policy = WritePolicy()
        try:
            record = await self._client.operate(policy, key, operations)
        except Exception as e:
            return self._wrap_single_key_error(key, e)
        return RecordStream.from_single(key, record)

    async def _execute_batch_read(
        self, keys: List[Key], bins: Optional[List[str]],
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.BATCH))
        try:
            batch_records = await self._client.batch_read(
                batch_policy, None, keys, bins)
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_batch_records(batch_records)

    async def _execute_batch_operate(
        self, keys: List[Key], operations: List[Any],
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.BATCH))
        ops_per_key = [operations] * len(keys)
        try:
            batch_records = await self._client.batch_operate(
                batch_policy, None, keys, ops_per_key)
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_batch_records(batch_records)

    async def _execute_dataset_query(self) -> RecordStream:
        if self._policy is not None:
            policy = self._policy
        elif self._behavior is not None:
            policy = to_query_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.QUERY))
        else:
            policy = QueryPolicy()
        if self._chunk_size is not None and self._chunk_size > 0:
            policy.max_records = self._chunk_size
        if self._filter_expression is not None:
            policy.filter_expression = self._filter_expression
        partition_filter = self._partition_filter or PartitionFilter.all()
        statement = self._build_statement()

        try:
            recordset = await self._client.query(
                policy, partition_filter, statement)
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_recordset(recordset)


class QueryBinBuilder(Generic[_T]):
    """Builder for bin-level read operations in query contexts.

    Generic on ``_T`` (the parent builder) so the same class can be
    shared between the async :class:`QueryBuilder` and the sync
    ``SyncQueryBuilder``.  The parent must expose ``add_operation(op)``.

    Provides a simple ``get()`` terminal, CDT map/list navigation, and
    ``map_size()``/``list_size()`` convenience methods.  Write operations
    are intentionally excluded.
    """

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: _T, bin_name: str) -> None:
        self._parent = parent
        self._bin = bin_name

    # -- Simple read ----------------------------------------------------------

    def get(self) -> _T:
        """Read the entire bin value."""
        self._parent.add_operation(Operation.get_bin(self._bin))  # type: ignore[union-attr]
        return self._parent

    def map_size(self) -> _T:
        """Return the element count of a map bin."""
        self._parent.add_operation(MapOperation.size(self._bin))  # type: ignore[union-attr]
        return self._parent

    def list_size(self) -> _T:
        """Return the element count of a list bin."""
        self._parent.add_operation(ListOperation.size(self._bin))  # type: ignore[union-attr]
        return self._parent

    # -- Map navigation (singular -> CdtReadBuilder) --------------------------

    def on_map_index(self, index: int) -> CdtReadBuilder[_T]:
        """Navigate to a map element by index."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            MapReturnType,
            is_map=True,
        )

    def on_map_key(self, key: Any) -> CdtReadBuilder[_T]:
        """Navigate to a map element by key."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            MapReturnType,
            is_map=True,
        )

    def on_map_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a map element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            MapReturnType,
            is_map=True,
        )

    # -- Map navigation (singular invertable -> CdtReadInvertableBuilder) -----

    def on_map_value(self, value: Any) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            MapReturnType,
            is_map=True,
        )

    # -- Map navigation (range -> CdtReadInvertableBuilder) -------------------

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by index range."""
        b = self._bin
        if count is None:
            factory = lambda rt: MapOperation.get_by_index_range_from(b, index, rt)
        else:
            factory = lambda rt: MapOperation.get_by_index_range(b, index, count, rt)
        return CdtReadInvertableBuilder(
            self._parent, factory, MapReturnType, is_map=True,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by key range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            MapReturnType,
            is_map=True,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by rank range."""
        b = self._bin
        if count is None:
            factory = lambda rt: MapOperation.get_by_rank_range_from(b, rank, rt)
        else:
            factory = lambda rt: MapOperation.get_by_rank_range(b, rank, count, rt)
        return CdtReadInvertableBuilder(
            self._parent, factory, MapReturnType, is_map=True,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by value range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            MapReturnType,
            is_map=True,
        )

    # -- Map navigation (list selectors -> CdtReadInvertableBuilder) ----------

    def on_map_key_list(self, keys: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a list of keys."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            MapReturnType,
            is_map=True,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a list of values."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            MapReturnType,
            is_map=True,
        )

    # -- List navigation (singular -> CdtReadBuilder) -------------------------

    def on_list_index(self, index: int) -> CdtReadBuilder[_T]:
        """Navigate to a list element by index."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            ListReturnType,
            is_map=False,
        )

    def on_list_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a list element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            ListReturnType,
            is_map=False,
        )

    # -- List navigation (singular invertable -> CdtReadInvertableBuilder) ----

    def on_list_value(self, value: Any) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements matching a value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            ListReturnType,
            is_map=False,
        )

    # -- List navigation (range -> CdtReadInvertableBuilder) ------------------

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by index range."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            ListReturnType,
            is_map=False,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by rank range."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            ListReturnType,
            is_map=False,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by value range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            ListReturnType,
            is_map=False,
        )

    # -- List navigation (list selector -> CdtReadInvertableBuilder) ----------

    def on_list_value_list(self, values: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements matching a list of values."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            ListReturnType,
            is_map=False,
        )
