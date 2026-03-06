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
    BatchDeleteOp,
    BatchDeletePolicy,
    BatchReadOp,
    BatchReadPolicy,
    BatchWriteOp,
    BatchWritePolicy,
    Client,
    Expiration,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
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
    RecordExistsAction,
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
_EXP_WRITE_DEFAULT = int(ExpWriteFlags.DEFAULT)
_EXP_WRITE_CREATE_ONLY = int(ExpWriteFlags.CREATE_ONLY)
_EXP_WRITE_UPDATE_ONLY = int(ExpWriteFlags.UPDATE_ONLY)
_EXP_WRITE_ALLOW_DELETE = int(ExpWriteFlags.ALLOW_DELETE)
_EXP_WRITE_POLICY_NO_FAIL = int(ExpWriteFlags.POLICY_NO_FAIL)
_EXP_WRITE_EVAL_NO_FAIL = int(ExpWriteFlags.EVAL_NO_FAIL)

_TTL_NEVER_EXPIRE = -1
_TTL_DONT_UPDATE = -2
_TTL_SERVER_DEFAULT = 0


def _to_expiration(ttl: int) -> Expiration:
    """Convert an integer TTL value to an ``Expiration`` object."""
    if ttl == _TTL_NEVER_EXPIRE:
        return Expiration.NEVER_EXPIRE
    if ttl == _TTL_DONT_UPDATE:
        return Expiration.DONT_UPDATE
    if ttl == _TTL_SERVER_DEFAULT:
        return Expiration.NAMESPACE_DEFAULT
    return Expiration.seconds(ttl)
from aerospike_fluent.policy.policy_mapper import (
    to_batch_policy,
    to_batch_read_policy,
    to_query_policy,
    to_read_policy,
    to_write_policy,
)

from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.error_strategy import (
    ErrorHandler,
    ErrorStrategy,
    OnError,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_fluent.exceptions import (
    AerospikeError,
    convert_pac_exception,
    result_code_to_exception,
)
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape
from aerospike_fluent.record_result import RecordResult, batch_records_to_results
from aerospike_fluent.record_stream import RecordStream

_EXP_READ_DEFAULT = int(ExpReadFlags.DEFAULT)
_EXP_READ_EVAL_NO_FAIL = int(ExpReadFlags.EVAL_NO_FAIL)

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class _WriteVerbs:
    """Mixin providing the 8 write-verb transition methods.

    Subclasses implement ``_start_write_verb`` to define how a new write
    segment is opened.  The mixin supplies ``upsert``, ``insert``,
    ``update``, ``replace``, ``replace_if_exists``, ``delete``,
    ``touch``, and ``exists`` — all thin wrappers around
    ``_start_write_verb``.
    """

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        raise NotImplementedError

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start an upsert write segment."""
        return self._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start an insert (create-only) segment."""
        return self._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start an update (update-only) segment."""
        return self._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start a replace segment."""
        return self._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start a replace-if-exists segment."""
        return self._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start a delete segment."""
        return self._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start a touch segment (reset TTL)."""
        return self._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Start an exists-check segment."""
        return self._start_write_verb("exists", arg1, *more_keys)


_OP_TYPE_TO_REA: dict[str, RecordExistsAction] = {
    "insert": RecordExistsAction.CREATE_ONLY,
    "update": RecordExistsAction.UPDATE_ONLY,
    "replace": RecordExistsAction.REPLACE,
    "replace_if_exists": RecordExistsAction.REPLACE_ONLY,
}


def _build_exp_write_flags(
    base: int,
    ignore_op_failure: bool,
    ignore_eval_failure: bool,
    delete_if_null: bool,
) -> int:
    """OR together ExpWriteFlags bitmask from boolean kwargs."""
    flags = base
    if ignore_op_failure:
        flags |= _EXP_WRITE_POLICY_NO_FAIL
    if ignore_eval_failure:
        flags |= _EXP_WRITE_EVAL_NO_FAIL
    if delete_if_null:
        flags |= _EXP_WRITE_ALLOW_DELETE
    return flags


@dataclass(slots=True)
class _OperationSpec:
    """A single operation segment in a chained builder.

    Each spec captures the keys,
    accumulated bin operations, projected bins, optional filter
    expression, and the operation type for one segment in a chain.

    ``op_type`` is ``None`` for read/query segments.  For write
    segments it is one of ``"upsert"``, ``"insert"``, ``"update"``,
    ``"replace"``, ``"replace_if_exists"``, ``"delete"``, ``"touch"``,
    or ``"exists"``.
    """

    keys: List[Key]
    operations: List[Any] = field(default_factory=list)
    bins: Optional[List[str]] = None
    filter_expression: Optional[FilterExpression] = None
    op_type: Optional[str] = None
    generation: Optional[int] = None
    ttl_seconds: Optional[int] = None
    durable_delete: Optional[bool] = None


_T = TypeVar("_T")


class QueryBuilder(_WriteVerbs):
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
        self._single_key: Optional[Key] = None
        self._keys: Optional[List[Key]] = None
        self._read_policy: Optional[ReadPolicy] = None
        self._op_type: Optional[str] = None
        self._generation: Optional[int] = None
        self._ttl_seconds: Optional[int] = None
        self._durable_delete: Optional[bool] = None
        self._default_filter_expression: Optional[FilterExpression] = None
        self._default_ttl_seconds: Optional[int] = None

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
        if not bin_names:
            raise ValueError("bin_names must not be empty; use with_no_bins() for metadata-only reads")
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
            *params: Optional values substituted into the DSL string via % formatting.
                Only used when expression is a str; ignored
                when expression is a FilterExpression. For string literals in DSL, include quotes
                in the template (e.g., "$.name == '%s'" with param "Tim").

        Returns:
            self for method chaining.

        Example:
            ```python
            # Using text DSL
            query = session.query(dataset).where("$.country == 'US' and $.order_total > 500")

            # Using DSL with params
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

    # -- Chain-level defaults -------------------------------------------------

    @overload
    def default_where(self, expression: str) -> QueryBuilder: ...

    @overload
    def default_where(self, expression: str, *params: Any) -> QueryBuilder: ...

    @overload
    def default_where(self, expression: FilterExpression) -> QueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> QueryBuilder:
        """Set a default filter expression for all specs that lack their own.

        Args:
            expression: DSL string or pre-built FilterExpression.
            *params: Optional format parameters for DSL strings.

        Returns:
            self for method chaining.
        """
        if isinstance(expression, str):
            dsl = expression % params if params else expression
            self._default_filter_expression = parse_dsl(dsl)
        else:
            self._default_filter_expression = expression
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> QueryBuilder:
        """Set a default TTL applied to specs that lack their own.

        Args:
            seconds: Time-to-live in seconds (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If seconds is <= 0.
        """
        if seconds <= 0:
            raise ValueError("seconds must be greater than 0")
        self._default_ttl_seconds = seconds
        return self

    def default_never_expire(self) -> QueryBuilder:
        """Set the default TTL to never expire (TTL = -1)."""
        self._default_ttl_seconds = _TTL_NEVER_EXPIRE
        return self

    def default_with_no_change_in_expiration(self) -> QueryBuilder:
        """Set the default to preserve each record's existing TTL (TTL = -2)."""
        self._default_ttl_seconds = _TTL_DONT_UPDATE
        return self

    def default_expiry_from_server_default(self) -> QueryBuilder:
        """Set the default TTL to the namespace's server default (TTL = 0)."""
        self._default_ttl_seconds = _TTL_SERVER_DEFAULT
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
        self._op_type = None
        self._set_current_keys(arg1, *more_keys)
        return self

    # -- Write transitions (QueryBuilder -> WriteSegmentBuilder) ---------------

    def _start_write_segment(
        self,
        op_type: str,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize current spec, set up a write segment, return builder."""
        self._finalize_current_spec()
        self._op_type = op_type
        self._set_current_keys(arg1, *more_keys)
        return WriteSegmentBuilder(self)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._start_write_segment(op_type, arg1, *more_keys)

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

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Execute the query and return a :class:`RecordStream`.

        Handles single-key, batch-key, and dataset queries.  When query
        stacking is used (multiple ``.query()`` calls), each accumulated
        spec is executed and results are combined into a single stream.

        Args:
            on_error: Controls how per-record errors are surfaced.

                - ``None`` (default): single-key operations raise on error;
                  batch / multi-key operations embed errors in the stream.
                - ``ErrorStrategy.IN_STREAM``: always embed errors in the
                  stream as ``RecordResult`` entries.
                - A callable ``(key, index, exception) -> None``: errors are
                  dispatched to the callback and excluded from the stream.

        Returns:
            A :class:`RecordStream` of :class:`RecordResult` items.
        """
        self._finalize_current_spec()

        if self._specs:
            is_single = (
                len(self._specs) == 1
                and len(self._specs[0].keys) == 1
            )
            disp = _resolve_disposition(on_error, is_single)
            handler = on_error if callable(on_error) else None
            if len(self._specs) == 1:
                return await self._execute_spec(
                    self._specs[0], disp, handler)
            return await self._execute_batch_mixed(
                self._specs, disp, handler)

        # Dataset query path (no keys were specified)
        if self._operations:
            raise AerospikeError(
                "Bin-level read operations are not supported on dataset/index "
                "queries (requires Advanced Bin Projection, not yet available)",
                result_code=ResultCode.OP_NOT_APPLICABLE,
            )
        return await self._execute_dataset_query()

    # -- Private helpers -------------------------------------------------------

    def _set_current_keys(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> None:
        """Parse key argument(s) and set ``_single_key`` or ``_keys``."""
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
                f"requires a Key or List[Key], got {type(arg1).__name__}"
            )

    def _finalize_current_spec(self) -> None:
        """Package the current key/ops/bins/filter/op_type state into an _OperationSpec."""
        if self._single_key is not None:
            keys = [self._single_key]
        elif self._keys is not None:
            keys = self._keys
        else:
            return

        filt = self._filter_expression or self._default_filter_expression
        ttl = self._ttl_seconds if self._ttl_seconds is not None else self._default_ttl_seconds

        self._specs.append(_OperationSpec(
            keys=keys,
            operations=list(self._operations),
            bins=self._bins,
            filter_expression=filt,
            op_type=self._op_type,
            generation=self._generation,
            ttl_seconds=ttl,
            durable_delete=self._durable_delete,
        ))

        self._single_key = None
        self._keys = None
        self._operations = []
        self._bins = None
        self._with_no_bins = False
        self._filter_expression = None
        self._op_type = None
        self._generation = None
        self._ttl_seconds = None
        self._durable_delete = None

    async def _execute_spec(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        """Execute a single :class:`_OperationSpec`."""
        keys = spec.keys
        op_type = spec.op_type

        if op_type is None:
            has_ops = bool(spec.operations)
            if len(keys) == 1:
                if has_ops:
                    return await self._execute_single_key_operate(
                        spec, disp, handler)
                return await self._execute_single_key_read(
                    spec, disp, handler)
            if has_ops:
                return await self._execute_batch_read_operate(
                    spec, disp, handler)
            return await self._execute_batch_read(spec, disp, handler)

        if op_type == "delete":
            if len(keys) == 1:
                return await self._execute_single_key_delete(spec, disp, handler)
            return await self._execute_batch_delete(spec, disp, handler)

        if op_type == "touch":
            if len(keys) == 1:
                return await self._execute_single_key_touch(spec, disp, handler)
            return await self._execute_batch_touch(spec, disp, handler)

        if op_type == "exists":
            if len(keys) == 1:
                return await self._execute_single_key_exists(spec, disp, handler)
            return await self._execute_batch_exists(spec, disp, handler)

        if len(keys) == 1:
            return await self._execute_single_key_write(spec, disp, handler)
        return await self._execute_batch_write(spec, disp, handler)

    @staticmethod
    def _should_include_result(
        result_code: ResultCode,
        respond_all_keys: bool,
        fail_on_filtered_out: bool,
    ) -> bool:
        """Decide whether to include a result in the stream.

        Decides whether to include a per-key result in the stream.
        """
        if result_code == ResultCode.OK:
            return True
        if result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            return respond_all_keys
        if result_code == ResultCode.FILTERED_OUT:
            return fail_on_filtered_out or respond_all_keys
        return True

    def _filtered_batch_stream(
        self,
        batch_records,
        disp: _ErrorDisposition = _ErrorDisposition.IN_STREAM,
        handler: ErrorHandler | None = None,
        op_type: Optional[str] = None,
    ) -> RecordStream:
        """Convert batch records to a filtered RecordStream.

        Applies the error disposition to each per-record result:
        THROW raises on the first error, HANDLER dispatches errors to the
        callback (excluding them from the stream), IN_STREAM includes them.
        """
        all_results = batch_records_to_results(list(batch_records))
        filtered: list[RecordResult] = []
        for r in all_results:
            if not r.is_ok and self._is_actionable(r.result_code, op_type):
                if disp is _ErrorDisposition.THROW:
                    raise result_code_to_exception(
                        r.result_code, str(r.result_code), r.in_doubt)
                if disp is _ErrorDisposition.HANDLER and handler is not None:
                    handler(r.key, r.index, result_code_to_exception(
                        r.result_code, str(r.result_code), r.in_doubt))
                    continue

            if not self._should_include_result(
                r.result_code, self._respond_all_keys, self._fail_on_filtered_out
            ):
                continue

            filtered.append(r)
        return RecordStream.from_list(filtered)

    _WRITES_REQUIRING_EXISTING_KEY = frozenset({"update", "replace_if_exists"})

    def _is_actionable(self, rc: ResultCode, op_type: Optional[str]) -> bool:
        """Whether *rc* should be routed through disposition logic.

        ``KEY_NOT_FOUND_ERROR`` is only actionable when the operation
        explicitly requires an existing record (update, replace_if_exists).
        ``FILTERED_OUT`` is only actionable when ``fail_on_filtered_out``
        has been set.  All other non-OK codes are always actionable.
        """
        if rc == ResultCode.KEY_NOT_FOUND_ERROR:
            return op_type in self._WRITES_REQUIRING_EXISTING_KEY
        if rc == ResultCode.FILTERED_OUT:
            return self._fail_on_filtered_out
        return True

    def _handle_error(
        self,
        key: Key,
        exc: Exception,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
        index: int = 0,
        op_type: Optional[str] = None,
    ) -> RecordStream:
        """Route a per-key error according to the resolved disposition.

        The PAC raises ``ServerError`` for ``KEY_NOT_FOUND_ERROR`` and
        ``FILTERED_OUT`` (unlike the Java client which returns null).
        Whether these codes are routed through disposition depends on
        the operation context (see ``_is_actionable``).
        """
        pfc_exc = convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        in_doubt = pfc_exc.in_doubt

        if self._is_actionable(rc, op_type):
            if disp is _ErrorDisposition.THROW:
                raise pfc_exc from exc
            if disp is _ErrorDisposition.HANDLER and handler is not None:
                handler(key, index, pfc_exc)
                return RecordStream.from_list([])

        if not self._should_include_result(
            rc, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream.from_list([])

        return RecordStream.from_error(key, rc, in_doubt, exception=pfc_exc)

    def _handle_batch_error(
        self,
        keys: List[Key],
        exc: Exception,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        """Route a batch-level error according to the resolved disposition.

        When the entire batch call fails (e.g. timeout, connection error),
        we create one error result per key.
        """
        pfc_exc = convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        in_doubt = pfc_exc.in_doubt

        if disp is _ErrorDisposition.THROW:
            raise pfc_exc from exc

        if disp is _ErrorDisposition.HANDLER and handler is not None:
            for i, key in enumerate(keys):
                handler(key, i, pfc_exc)
            return RecordStream.from_list([])

        results = [
            RecordResult(
                key=key, record=None, result_code=rc,
                in_doubt=in_doubt, index=i, exception=pfc_exc,
            )
            for i, key in enumerate(keys)
        ]
        return RecordStream.from_list(results)

    def _make_read_policy(
        self, spec: _OperationSpec,
    ) -> ReadPolicy:
        """Build a ``ReadPolicy`` for single-key reads."""
        if self._read_policy is not None:
            rp = self._read_policy
        elif self._behavior is not None:
            rp = to_read_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        else:
            rp = ReadPolicy()
        if spec.filter_expression is not None:
            rp.filter_expression = spec.filter_expression
        return rp

    async def _execute_single_key_read(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        read_policy = self._make_read_policy(spec)
        try:
            record = await self._client.get(read_policy, key, spec.bins)
        except Exception as e:
            return self._handle_error(key, e, disp, handler)
        return RecordStream.from_single(key, record)

    async def _execute_single_key_operate(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        policy = WritePolicy()
        if spec.filter_expression is not None:
            policy.filter_expression = spec.filter_expression
        try:
            record = await self._client.operate(policy, key, spec.operations)
        except Exception as e:
            return self._handle_error(key, e, disp, handler)
        return RecordStream.from_single(key, record)

    async def _execute_batch_read(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        batch_read_policy = None
        if self._behavior is not None:
            settings = self._behavior.get_settings(OpKind.READ, OpShape.BATCH)
            batch_policy = to_batch_policy(settings)
            batch_read_policy = to_batch_read_policy(settings)
        if spec.filter_expression is not None:
            if batch_read_policy is None:
                batch_read_policy = BatchReadPolicy()
            batch_read_policy.filter_expression = spec.filter_expression
        try:
            batch_records = await self._client.batch_read(
                batch_policy, batch_read_policy, spec.keys, spec.bins)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler)

    async def _execute_batch_read_operate(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.BATCH))
        ops_per_key = [spec.operations] * len(spec.keys)
        bwp = None
        if spec.filter_expression is not None:
            bwp = BatchWritePolicy()
            bwp.filter_expression = spec.filter_expression
        try:
            batch_records = await self._client.batch_operate(
                batch_policy, bwp, spec.keys, ops_per_key)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler)

    # -- Write execution helpers ----------------------------------------------

    def _make_write_policy(self, spec: _OperationSpec) -> WritePolicy:
        """Build a ``WritePolicy`` for single-key writes."""
        if self._behavior is not None:
            wp = to_write_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        else:
            wp = WritePolicy()
        op_type = spec.op_type or "upsert"
        rea = _OP_TYPE_TO_REA.get(op_type)
        if rea is not None:
            wp.record_exists_action = rea
        if spec.filter_expression is not None:
            wp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            from aerospike_async import GenerationPolicy
            wp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            wp.generation = spec.generation
        if spec.ttl_seconds is not None:
            wp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.durable_delete:
            wp.durable_delete = True
        return wp

    @staticmethod
    def _make_batch_write_policy(spec: _OperationSpec) -> Optional[BatchWritePolicy]:
        """Build a ``BatchWritePolicy`` for multi-key batch writes."""
        has_settings = (
            spec.filter_expression is not None
            or spec.generation is not None
            or spec.ttl_seconds is not None
            or spec.durable_delete
        )
        if not has_settings:
            return None
        bwp = BatchWritePolicy()
        if spec.filter_expression is not None:
            bwp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            bwp.generation = spec.generation
        if spec.ttl_seconds is not None:
            bwp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.durable_delete:
            bwp.durable_delete = True
        return bwp

    @staticmethod
    def _make_batch_delete_policy(spec: _OperationSpec) -> Optional[BatchDeletePolicy]:
        """Build a ``BatchDeletePolicy`` for multi-key batch deletes."""
        has_settings = (
            spec.filter_expression is not None
            or spec.generation is not None
            or spec.durable_delete
        )
        if not has_settings:
            return None
        bdp = BatchDeletePolicy()
        if spec.filter_expression is not None:
            bdp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            bdp.generation = spec.generation
        if spec.durable_delete:
            bdp.durable_delete = True
        return bdp

    async def _execute_single_key_write(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            record = await self._client.operate(wp, key, spec.operations)
        except Exception as e:
            return self._handle_error(
                key, e, disp, handler, op_type=spec.op_type)
        return RecordStream.from_single(key, record)

    async def _execute_single_key_delete(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            existed = await self._client.delete(wp, key)
        except Exception as e:
            return self._handle_error(
                key, e, disp, handler, op_type="delete")
        rc = ResultCode.OK if existed else ResultCode.KEY_NOT_FOUND_ERROR
        if self._should_include_result(
            rc, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream.from_error(key, rc)
        return RecordStream.from_list([])

    async def _execute_batch_write(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        bwp = self._make_batch_write_policy(spec)
        ops_per_key = [spec.operations] * len(spec.keys)
        try:
            batch_records = await self._client.batch_operate(
                batch_policy, bwp, spec.keys, ops_per_key)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(
            batch_records, disp, handler, op_type=spec.op_type)

    async def _execute_batch_delete(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        bdp = self._make_batch_delete_policy(spec)
        try:
            batch_records = await self._client.batch_delete(
                batch_policy, bdp, spec.keys)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(
            batch_records, disp, handler, op_type="delete")

    async def _execute_single_key_touch(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            await self._client.touch(wp, key)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="touch")
        if self._should_include_result(
            ResultCode.OK, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream.from_error(key, ResultCode.OK)
        return RecordStream.from_list([])

    async def _execute_batch_touch(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        bwp = self._make_batch_write_policy(spec)
        touch_ops = [Operation.touch()]
        ops_per_key = [touch_ops] * len(spec.keys)
        try:
            batch_records = await self._client.batch_operate(
                batch_policy, bwp, spec.keys, ops_per_key)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(
            batch_records, disp, handler, op_type="touch")

    async def _execute_single_key_exists(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        if self._read_policy is not None:
            rp = self._read_policy
        elif self._behavior is not None:
            rp = to_read_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        else:
            rp = ReadPolicy()
        if spec.filter_expression is not None:
            rp.filter_expression = spec.filter_expression
        try:
            found = await self._client.exists(rp, key)
        except Exception as e:
            return self._handle_error(
                key, e, disp, handler, op_type="exists")
        rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
        if self._should_include_result(
            rc, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream.from_error(key, rc)
        return RecordStream.from_list([])

    async def _execute_batch_exists(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.READ, OpShape.BATCH))
        brp = self._make_batch_read_policy(spec)
        try:
            found_list = await self._client.batch_exists(
                batch_policy, brp, spec.keys)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        results = []
        for key, found in zip(spec.keys, found_list):
            rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
            if self._should_include_result(
                rc, self._respond_all_keys, self._fail_on_filtered_out
            ):
                results.append(RecordResult(key, None, rc))
        return RecordStream.from_list(results)

    # -- Mixed-batch execution (multi-spec chains) ----------------------------

    def _spec_to_batch_ops(
        self, spec: _OperationSpec,
    ) -> list:
        """Convert one spec into a list of ``BatchReadOp`` / ``BatchWriteOp``
        / ``BatchDeleteOp`` objects for the PAC mixed-batch API."""
        ops: list = []
        op_type = spec.op_type

        if op_type is None:
            brp = self._make_batch_read_policy(spec)
            for key in spec.keys:
                if spec.operations:
                    ops.append(BatchReadOp(
                        key, operations=spec.operations, policy=brp))
                else:
                    ops.append(BatchReadOp(key, bins=spec.bins, policy=brp))
        elif op_type == "delete":
            bdp = self._make_batch_delete_policy(spec)
            for key in spec.keys:
                ops.append(BatchDeleteOp(key, policy=bdp))
        elif op_type == "touch":
            bwp = self._make_batch_write_policy_mixed(spec)
            touch_ops = [Operation.touch()]
            for key in spec.keys:
                ops.append(BatchWriteOp(key, touch_ops, policy=bwp))
        elif op_type == "exists":
            brp = self._make_batch_read_policy(spec)
            for key in spec.keys:
                ops.append(BatchReadOp(key, bins=[], policy=brp))
        else:
            bwp = self._make_batch_write_policy_mixed(spec)
            for key in spec.keys:
                ops.append(BatchWriteOp(
                    key, spec.operations, policy=bwp))
        return ops

    @staticmethod
    def _make_batch_read_policy(
        spec: _OperationSpec,
    ) -> Optional[BatchReadPolicy]:
        """Build a ``BatchReadPolicy`` from per-spec settings."""
        if spec.filter_expression is None:
            return None
        brp = BatchReadPolicy()
        brp.filter_expression = spec.filter_expression
        return brp

    @staticmethod
    def _make_batch_write_policy_mixed(
        spec: _OperationSpec,
    ) -> Optional[BatchWritePolicy]:
        """Build a ``BatchWritePolicy`` that includes ``record_exists_action``
        for use in mixed-batch calls."""
        op_type = spec.op_type or "upsert"
        rea = _OP_TYPE_TO_REA.get(op_type)
        has_settings = (
            rea is not None
            or spec.filter_expression is not None
            or spec.generation is not None
            or spec.ttl_seconds is not None
            or spec.durable_delete
        )
        if not has_settings:
            return None
        bwp = BatchWritePolicy()
        if rea is not None:
            bwp.record_exists_action = rea
        if spec.filter_expression is not None:
            bwp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            from aerospike_async import GenerationPolicy
            bwp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            bwp.generation = spec.generation
        if spec.ttl_seconds is not None:
            bwp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.durable_delete:
            bwp.durable_delete = True
        return bwp

    async def _execute_batch_mixed(
        self, specs: List[_OperationSpec],
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        """Send all specs in one round-trip via ``Client.batch()``."""
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        all_ops: list = []
        all_keys: List[Key] = []
        for spec in specs:
            all_keys.extend(spec.keys)
            all_ops.extend(self._spec_to_batch_ops(spec))
        try:
            batch_records = await self._client.batch(batch_policy, all_ops)
        except Exception as e:
            return self._handle_batch_error(all_keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler)

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


class WriteSegmentBuilder(_WriteVerbs):
    """Builder for a write segment in a chained operation.

    Accumulates bin-level write operations for the current key(s)
    and provides transition methods to start new read or write
    segments.  Delegates execution to the parent ``QueryBuilder``.
    """

    __slots__ = ("_qb",)

    def __init__(self, qb: QueryBuilder) -> None:
        self._qb = qb

    # -- Bin operations -------------------------------------------------------

    def bin(self, bin_name: str) -> WriteBinBuilder:
        """Start a bin-level write operation.

        Args:
            bin_name: The bin to operate on.

        Returns:
            A WriteBinBuilder for method chaining.
        """
        return WriteBinBuilder(self, bin_name)

    def put(self, bins: dict) -> WriteSegmentBuilder:
        """Set multiple bins at once.

        Args:
            bins: Dictionary of bin name to value mappings.

        Returns:
            self for method chaining.
        """
        for bin_name, value in bins.items():
            self._qb._operations.append(Operation.put(bin_name, value))
        return self

    def set_bins(self, bins: dict) -> WriteSegmentBuilder:
        """Alias for :meth:`put`."""
        return self.put(bins)

    def _add_op(self, op: Any) -> WriteSegmentBuilder:
        self._qb._operations.append(op)
        return self

    # -- Scalar bin operations (direct on segment) ----------------------------

    def set_to(self, bin_name: str, value: Any) -> WriteSegmentBuilder:
        """Set a bin to *value*."""
        return self._add_op(Operation.put(bin_name, value))

    def add(self, bin_name: str, value: Any) -> WriteSegmentBuilder:
        """Add *value* to a bin (numeric increment)."""
        return self._add_op(Operation.add(bin_name, value))

    def increment_by(self, bin_name: str, value: Any) -> WriteSegmentBuilder:
        """Alias for :meth:`add`."""
        return self.add(bin_name, value)

    def get(self, bin_name: str) -> WriteSegmentBuilder:
        """Read a bin value back within a write operate."""
        return self._add_op(Operation.get_bin(bin_name))

    def append(self, bin_name: str, value: str) -> WriteSegmentBuilder:
        """Append a string to a bin."""
        return self._add_op(Operation.append(bin_name, value))

    def prepend(self, bin_name: str, value: str) -> WriteSegmentBuilder:
        """Prepend a string to a bin."""
        return self._add_op(Operation.prepend(bin_name, value))

    def remove_bin(self, bin_name: str) -> WriteSegmentBuilder:
        """Delete a bin from the record."""
        return self._add_op(Operation.put(bin_name, None))

    # -- Expression operations (direct on segment) ----------------------------

    def select_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> WriteSegmentBuilder:
        """Read a computed value into a bin using a DSL expression."""
        flags = _EXP_READ_EVAL_NO_FAIL if ignore_eval_failure else _EXP_READ_DEFAULT
        expr = parse_dsl(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.read(bin_name, expr, flags))

    def insert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        flags = _build_exp_write_flags(
            _EXP_WRITE_CREATE_ONLY, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_dsl(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    def update_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        flags = _build_exp_write_flags(
            _EXP_WRITE_UPDATE_ONLY, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_dsl(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    def upsert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        flags = _build_exp_write_flags(
            _EXP_WRITE_DEFAULT, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_dsl(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    # -- Transition methods ---------------------------------------------------

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> QueryBuilder:
        """Finalize current write segment and start a read segment.

        Args:
            arg1: A single Key or List[Key].
            *more_keys: Additional keys (varargs).

        Returns:
            The parent QueryBuilder for method chaining.
        """
        self._qb._finalize_current_spec()
        self._qb._op_type = None
        self._qb._set_current_keys(arg1, *more_keys)
        return self._qb

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        self._qb._finalize_current_spec()
        self._qb._op_type = op_type
        self._qb._set_current_keys(arg1, *more_keys)
        return self

    # -- Per-spec settings ----------------------------------------------------

    @overload
    def where(self, expression: str) -> WriteSegmentBuilder: ...

    @overload
    def where(self, expression: str, *params: Any) -> WriteSegmentBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> WriteSegmentBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> WriteSegmentBuilder:
        """Set a filter expression on the current write segment.

        Args:
            expression: DSL string or pre-built FilterExpression.
            *params: Optional format parameters for DSL strings.

        Returns:
            self for method chaining.
        """
        if isinstance(expression, str):
            dsl = expression % params if params else expression
            self._qb._filter_expression = parse_dsl(dsl)
        else:
            self._qb._filter_expression = expression
        return self

    def expire_record_after_seconds(self, seconds: int) -> WriteSegmentBuilder:
        """Set the TTL on the current write segment.

        Args:
            seconds: Time-to-live in seconds (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If seconds is <= 0.
        """
        if seconds <= 0:
            raise ValueError("seconds must be greater than 0")
        self._qb._ttl_seconds = seconds
        return self

    def never_expire(self) -> WriteSegmentBuilder:
        """Set this record to never expire (TTL = -1).

        Returns:
            self for method chaining.
        """
        self._qb._ttl_seconds = _TTL_NEVER_EXPIRE
        return self

    def with_no_change_in_expiration(self) -> WriteSegmentBuilder:
        """Preserve the record's existing TTL (TTL = -2).

        Returns:
            self for method chaining.
        """
        self._qb._ttl_seconds = _TTL_DONT_UPDATE
        return self

    def expiry_from_server_default(self) -> WriteSegmentBuilder:
        """Use the namespace's default TTL for this record (TTL = 0).

        Returns:
            self for method chaining.
        """
        self._qb._ttl_seconds = _TTL_SERVER_DEFAULT
        return self

    def ensure_generation_is(self, generation: int) -> WriteSegmentBuilder:
        """Set expected generation for optimistic locking on the current segment.

        Args:
            generation: The expected generation number (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If generation is <= 0.
        """
        if generation <= 0:
            raise ValueError("Generation must be greater than 0")
        self._qb._generation = generation
        return self

    def durably_delete(self) -> WriteSegmentBuilder:
        """Enable durable delete on the current segment.

        Returns:
            self for method chaining.
        """
        self._qb._durable_delete = True
        return self

    def respond_all_keys(self) -> WriteSegmentBuilder:
        """Include results for missing keys in the stream.

        Returns:
            self for method chaining.
        """
        self._qb._respond_all_keys = True
        return self

    def fail_on_filtered_out(self) -> WriteSegmentBuilder:
        """Mark filtered-out records with ``FILTERED_OUT`` result code.

        Returns:
            self for method chaining.
        """
        self._qb._fail_on_filtered_out = True
        return self

    def replace_only(self) -> WriteSegmentBuilder:
        """Change the current segment to replace-if-exists semantics.

        The record must already exist; the operation fails with
        ``KEY_NOT_FOUND_ERROR`` if it does not.  All existing bins
        are removed and only the bins specified in this segment are
        written.

        Returns:
            self for method chaining.
        """
        self._qb._op_type = "replace_if_exists"
        return self

    # -- Execution ------------------------------------------------------------

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Execute all accumulated specs.

        Args:
            on_error: Error handling strategy (see ``QueryBuilder.execute``).
        """
        return await self._qb.execute(on_error)


class WriteBinBuilder(_WriteVerbs):
    """Builder for a single bin write operation within a write segment.

    Thin currying wrapper that captures a bin name and delegates all
    operations to the parent ``WriteSegmentBuilder``.  CDT navigation
    methods remain here since they require a captured bin name and
    return CDT-specific builders.
    """

    __slots__ = ("_segment", "_bin")

    def __init__(self, segment: WriteSegmentBuilder, bin_name: str) -> None:
        self._segment = segment
        self._bin = bin_name

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> WriteSegmentBuilder:
        """Set the bin to *value*."""
        return self._segment.set_to(self._bin, value)

    def add(self, value: Any) -> WriteSegmentBuilder:
        """Add *value* to the bin (numeric increment)."""
        return self._segment.add(self._bin, value)

    def increment_by(self, value: Any) -> WriteSegmentBuilder:
        """Alias for :meth:`add`."""
        return self.add(value)

    def append(self, value: str) -> WriteSegmentBuilder:
        """Append a string to the bin."""
        return self._segment.append(self._bin, value)

    def prepend(self, value: str) -> WriteSegmentBuilder:
        """Prepend a string to the bin."""
        return self._segment.prepend(self._bin, value)

    def remove(self) -> WriteSegmentBuilder:
        """Delete the bin from the record."""
        return self._segment.remove_bin(self._bin)

    def get(self) -> WriteSegmentBuilder:
        """Read the bin value back within a write operate."""
        return self._segment.get(self._bin)

    # -- Expression operations ------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> WriteSegmentBuilder:
        """Read a computed value into this bin using a DSL expression."""
        return self._segment.select_from(
            self._bin, expression, ignore_eval_failure=ignore_eval_failure,
        )

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        return self._segment.insert_from(
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
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        return self._segment.update_from(
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
    ) -> WriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        return self._segment.upsert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    # -- Convenience transitions (delegate to segment) ------------------------

    def bin(self, bin_name: str) -> WriteBinBuilder:
        """Start the next bin operation without leaving the write segment."""
        return WriteBinBuilder(self._segment, bin_name)

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> QueryBuilder:
        """Shortcut: finalize write segment and start a read segment."""
        return self._segment.query(arg1, *more_keys)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._segment._start_write_verb(op_type, arg1, *more_keys)

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Shortcut: execute all accumulated specs."""
        return await self._segment.execute(on_error)


class QueryBinBuilder(_WriteVerbs, Generic[_T]):
    """Builder for bin-level read operations in query contexts.

    Generic on ``_T`` (the parent builder) so the same class can be
    shared between the async :class:`QueryBuilder` and the sync
    ``SyncQueryBuilder``.  The parent must expose ``add_operation(op)``.

    Provides a simple ``get()`` terminal, CDT map/list navigation, and
    ``map_size()``/``list_size()`` convenience methods.  Write transitions
    delegate to the parent builder.
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

    # -- Expression read ------------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> _T:
        """Read a computed value into this bin using a DSL expression.

        The result appears as a virtual bin in the returned record.

        Args:
            expression: DSL string or pre-built FilterExpression.
            ignore_eval_failure: If True, silently return None when the
                expression cannot be evaluated (e.g. missing bin).

        Returns:
            The parent builder for method chaining.
        """
        flags = _EXP_READ_EVAL_NO_FAIL if ignore_eval_failure else _EXP_READ_DEFAULT
        expr = parse_dsl(expression) if isinstance(expression, str) else expression
        self._parent.add_operation(ExpOperation.read(self._bin, expr, flags))  # type: ignore[union-attr]
        return self._parent

    # -- Write transitions (delegate to parent) -------------------------------

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._parent._start_write_verb(op_type, arg1, *more_keys)  # type: ignore[union-attr]

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
