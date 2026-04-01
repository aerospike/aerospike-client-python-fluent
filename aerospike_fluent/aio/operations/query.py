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

"""Fluent builders for reads, writes, and chained multi-operation queries."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    overload,
)

from aerospike_async import (
    BasePolicy,
    BatchDeleteOp,
    BatchDeletePolicy,
    BatchReadOp,
    BatchReadPolicy,
    BatchUDFPolicy,
    BatchWriteOp,
    BatchWritePolicy,
    CTX,
    Client,
    ExecuteTask,
    Expiration,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    Filter,
    FilterExpression,
    GenerationPolicy,
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
    _map_item_pairs,
)
from aerospike_fluent.aio.operations.cdt_write import (
    CdtWriteBuilder,
    CdtWriteInvertableBuilder,
)

log = logging.getLogger("aerospike_fluent.query")

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

from aerospike_fluent.background_shared import (
    make_background_write_policy,
    reject_unsupported_background_write_ops,
)
from aerospike_fluent.dsl.parser import parse_dsl, parse_dsl_with_index
from aerospike_fluent.error_strategy import (
    ErrorHandler,
    ErrorStrategy,
    OnError,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_fluent.exceptions import (
    AerospikeError,
    _convert_pac_exception,
    _result_code_to_exception,
)
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape
from aerospike_fluent.record_result import RecordResult, batch_records_to_results
from aerospike_fluent.record_stream import RecordStream

_EXP_READ_DEFAULT = int(ExpReadFlags.DEFAULT)
_EXP_READ_EVAL_NO_FAIL = int(ExpReadFlags.EVAL_NO_FAIL)


@dataclass(frozen=True)
class QueryHint:
    """Hint for influencing secondary index selection and query scheduling.

    Provide ``index_name`` to force a specific named secondary index, or
    ``bin_name`` to redirect the filter to a different bin's index.  These two
    are mutually exclusive.  ``query_duration`` overrides the policy's
    ``expected_duration`` for this query only.

    Example:
        >>> hint = QueryHint(index_name="age_idx",
        ...                  query_duration=QueryDuration.SHORT)
        >>> stream = await (
        ...     session.query(dataset)
        ...         .filter(Filter.equal("age", 30))
        ...         .with_hint(hint)
        ...         .execute()
        ... )

    Args:
        index_name: Force the query to use the named secondary index.
        bin_name: Redirect the filter to use a different bin's index.
        query_duration: Override ``expected_duration`` on the query policy.

    Raises:
        ValueError: If both ``index_name`` and ``bin_name`` are provided.

    See Also:
        :meth:`QueryBuilder.with_hint`
    """

    index_name: Optional[str] = None
    bin_name: Optional[str] = None
    query_duration: Optional[QueryDuration] = None

    def __post_init__(self) -> None:
        if self.index_name is not None and self.bin_name is not None:
            raise ValueError(
                "index_name and bin_name are mutually exclusive; "
                "provide one or neither, not both"
            )


@dataclass
class _FilterRecord:
    """Internal: wraps a Filter with optional creation metadata for hint reconstruction."""

    filter: Filter
    method: Optional[str] = None
    identifier: Optional[str] = None
    args: Optional[tuple] = None
    ctx: Optional[List[CTX]] = None

    def rebuild_for_hint(self, hint: QueryHint) -> Filter:
        """Reconstruct this filter with the hint's index_name or bin_name override."""
        if self.method is None or self.args is None:
            raise ValueError(
                "Cannot apply index_name/bin_name hint to a pre-built Filter. "
                "Use Filter.*_by_index() directly or let the PFC generate the "
                "filter via parse_dsl_with_index()."
            )
        if hint.index_name is not None:
            factory = getattr(Filter, f"{self.method}_by_index")
            f = factory(hint.index_name, *self.args)
        elif hint.bin_name is not None:
            factory = getattr(Filter, self.method)
            f = factory(hint.bin_name, *self.args)
        else:
            return self.filter
        if self.ctx:
            f = f.context(self.ctx)
        return f


if TYPE_CHECKING:
    from aerospike_fluent.dsl.filter_gen import IndexContext
    from aerospike_fluent.index_monitor import IndexesMonitor
    from aerospike_fluent.policy.behavior import Behavior


class _WriteVerbs:
    """Mixin exposing write verbs that open a :class:`WriteSegmentBuilder`.

    Implemented on :class:`QueryBuilder` (chain from a read query) and
    :class:`WriteBinBuilder` (chain from a bin-scoped write). Each method
    finalizes the prior segment when applicable and targets new key(s).
    """

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        raise NotImplementedError

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open a create-or-update segment for the given key(s).

        Example::
            await session.upsert(key).put({"name": "Bob"}).execute()

        Returns:
            :class:`WriteSegmentBuilder` for ``put`` / ``bin`` / ``execute``.

        See Also:
            :meth:`~aerospike_fluent.aio.session.Session.upsert`: Session entry point.
        """
        return self._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open a create-only segment (fails if the record exists).

        Example::
            await session.insert(key).put({"name": "Ada"}).execute()

        Returns:
            :class:`WriteSegmentBuilder` for further bins and :meth:`execute`.
        """
        return self._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open an update-only segment (fails if the record is missing).

        Example::
            await session.update(key).bin("count").add(1).execute()

        Returns:
            :class:`WriteSegmentBuilder` for further bins and :meth:`execute`.
        """
        return self._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open a full replace segment (removes bins not written in this segment).

        Example::
            await session.replace(key).put({"a": 1}).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open replace-if-exists semantics (like replace, but only if the record exists).

        Example::
            await session.replace_if_exists(key).put({"a": 1}).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open a delete segment.

        Example::
            await session.delete(key).execute()

        Returns:
            :class:`WriteSegmentBuilder` (often followed immediately by :meth:`execute`).
        """
        return self._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open a touch segment (TTL refresh).

        Example::
            await session.touch(key).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Open an exists-check segment.

        Example::
            stream = await session.exists(key).execute()
            found = (await stream.first_or_raise()).as_bool()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
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
    udf_package: Optional[str] = None
    udf_function: Optional[str] = None
    udf_args: Optional[List[Any]] = None


_T = TypeVar("_T")


class QueryBuilder(_WriteVerbs):
    """Chain reads, writes, UDF calls, filters, and policies before ``execute``.

    Start from :meth:`~aerospike_fluent.aio.session.Session.query` or
    :meth:`~aerospike_fluent.aio.client.FluentClient.query`. Use :meth:`where`
    or :meth:`filter_expression` for server-side predicates, :meth:`bins` or
    :meth:`bin` for projections, and transition methods such as :meth:`upsert`
    for writes. Await :meth:`execute` for a :class:`~aerospike_fluent.record_stream.RecordStream`.

    Example:
        Set-wide read with filter and projection::

            stream = await (
                session.query(users)
                    .where("$.status == 'active'")
                    .bins(["user_id", "name"])
                    .execute()
            )
            async for row in stream:
                if row.is_ok and row.record:
                    print(row.record.bins)

        Point read on a key, then chain an upsert::

            stream = await (
                session.query(users.id("u1"))
                    .bins(["name"])
                    .upsert(users.id("u1"))
                    .put({"last_seen": 123})
                    .execute()
            )

    See Also:
        :class:`WriteSegmentBuilder`: Bin writes after a write verb.
        :class:`QueryBinBuilder`: Per-bin read operations.
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
        behavior: Optional[Behavior] = None,
        indexes_monitor: Optional["IndexesMonitor"] = None,
    ) -> None:
        """
        Initialize a QueryBuilder.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            behavior: Optional Behavior for deriving policies.
            indexes_monitor: Optional monitor providing cached index metadata
                for transparent filter generation from DSL expressions.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._behavior = behavior
        self._indexes_monitor = indexes_monitor
        self._bins: Optional[List[str]] = None
        self._with_no_bins: bool = False
        self._filter_records: List[_FilterRecord] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._query_hint: Optional[QueryHint] = None
        self._where_dsl: Optional[str] = None
        self._index_context: Optional["IndexContext"] = None
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
        self._udf_package: Optional[str] = None
        self._udf_function: Optional[str] = None
        self._udf_args: Optional[List[Any]] = None

    def bins(self, bin_names: List[str]) -> QueryBuilder:
        """Restrict the read to a non-empty set of bin names.

        Mutually exclusive with :meth:`with_no_bins`.

        Args:
            bin_names: Non-empty list of bin names to return.

        Returns:
            This builder for method chaining.

        Raises:
            ValueError: If ``bin_names`` is empty or :meth:`with_no_bins` was
                already called.

        Example:
            Restrict a query or key read to specific bins::

                stream = await session.query(users.id(1)).bins(["name", "email"]).execute()

        See Also:
            :meth:`with_no_bins`: Metadata-only reads without bin payloads.
            :meth:`bin`: Per-bin operations (CDT, expressions).
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

    def with_write_operations(
        self, operations: Sequence[Any],
    ) -> QueryBuilder:
        """Attach scalar write operations for a background dataset task.

        Prefer :meth:`aerospike_fluent.aio.session.Session.background_task` for
        fluent bin writes. Use with :meth:`execute_background_task` on a dataset
        query (no keys).
        Only ``Operation`` and ``ExpOperation.write``-style writes are valid;
        list, map, bit, and HLL operations are rejected before calling the client.

        Args:
            operations: Sequence of write operations (e.g. ``Operation.put``,
                ``Operation.touch``).

        Returns:
            self for method chaining.
        """
        self._operations.extend(operations)
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
        """Add a secondary index filter to the query.

        Args:
            filter_obj: The filter to add.

        Returns:
            This builder for method chaining.
        """
        self._filter_records.append(_FilterRecord(filter=filter_obj))
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
    def where(self, expression: FilterExpression) -> QueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> QueryBuilder:
        """Apply a server-side filter for dataset queries or keyed reads that support it.

        String arguments are parsed with the fluent DSL; prefer f-strings for
        dynamic literals. Pass a pre-built :class:`~aerospike_async.FilterExpression`
        when constructing filters programmatically.

        Args:
            expression: DSL string or ``FilterExpression``.

        Returns:
            This builder for chaining.

        Example:
            qb = session.query(ds).where("$.status == 'active'")
            qb = session.query(ds).where(f"$.score > {min_score}")

        See Also:
            :meth:`default_where`: Default filter for chained operations without their own.
            :meth:`filter_expression`: Attach an expression without DSL parsing.
        """
        if isinstance(expression, str):
            self._where_dsl = expression
            self._filter_expression = parse_dsl(expression)
        else:
            self._where_dsl = None
            self._filter_expression = expression
        return self

    def with_index_context(self, index_context: "IndexContext") -> QueryBuilder:
        """Explicitly override the secondary index metadata used for filter generation.

        Most applications do **not** need this method. The client automatically
        discovers and caches secondary index metadata from the cluster in the
        background. Use this only when you need to force a specific index
        context that differs from the live cluster state.

        Args:
            index_context: Index metadata for the query's namespace.

        Returns:
            This builder for method chaining.

        See Also:
            :class:`~aerospike_fluent.dsl.filter_gen.IndexContext`
        """
        self._index_context = index_context
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
        """Restrict a dataset query using a PAC :class:`~aerospike_async.PartitionFilter`.

        Prefer :meth:`on_partition` or :meth:`on_partition_range` for common cases.

        Args:
            partition_filter: Built filter (all partitions, by id, by range, etc.).

        Returns:
            This builder for chaining.

        See Also:
            :meth:`on_partition_range`: Inclusive start, exclusive end partition ids.
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
        """Tune server-side streaming chunk size (maps to query policy ``max_records`` chunking).

		This method controls how many records are fetched per chunk from the server
        when using server-side streaming. The chunk size affects memory usage and network
        round trips (Larger values reduce round trips; smaller values bound memory per fetch).
		This is distinct from client-side pagination.
        

        Args:
            chunk_size: Records per chunk; must be positive.

        Returns:
            This builder for chaining.

        Raises:
            ValueError: If ``chunk_size <= 0``.

            
        Example:
            ```python
            query = session.query(dataset).chunk_size(100)

            ```
        See Also:
            :meth:`max_records`: Cap total records returned.
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

    def with_hint(self, hint: QueryHint) -> QueryBuilder:
        """Attach a query hint for secondary index selection or scheduling.

        A hint can redirect which secondary index is used (``index_name``),
        remap the filter to a different bin (``bin_name``), or override the
        expected query duration (``query_duration``).  Only one call to
        ``with_hint`` is allowed per builder.

        Example:
            >>> stream = await (
            ...     session.query(dataset)
            ...         .filter(Filter.equal("age", 30))
            ...         .with_hint(QueryHint(index_name="age_idx"))
            ...         .execute()
            ... )

        Args:
            hint: A :class:`QueryHint` instance.

        Returns:
            This builder for method chaining.

        Raises:
            ValueError: If ``with_hint`` has already been called on this builder.

        See Also:
            :class:`QueryHint`
        """
        if self._query_hint is not None:
            raise ValueError("with_hint() can only be called once per query builder")
        self._query_hint = hint
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
        """Surface rows that fail a filter as ``FILTERED_OUT`` instead of omitting them.

        Applies to key-based reads where a filter excludes the record. Without this
        flag, filtered keys may be absent from the stream depending on policy.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`respond_all_keys`: Include missing-key rows in batch reads.
        """
        self._fail_on_filtered_out = True
        return self
    
    def respond_all_keys(self) -> QueryBuilder:
        """Ensure batch/point reads emit one row per requested key, including not-found.

        Missing keys appear as non-OK :class:`~aerospike_fluent.record_result.RecordResult`
        entries (typically ``KEY_NOT_FOUND``) instead of being skipped.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`fail_on_filtered_out`: Filter mismatch vs missing key.
        """
        self._respond_all_keys = True
        return self

    # -- Chain-level defaults -------------------------------------------------

    @overload
    def default_where(self, expression: str) -> QueryBuilder: ...

    @overload
    def default_where(self, expression: FilterExpression) -> QueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
    ) -> QueryBuilder:
        """Set a filter applied to any chained operation that does not call :meth:`where`.

        When a chain contains multiple operations (reads, writes, UDFs), each
        operation inherits this filter unless it supplies its own :meth:`where`.

        Example::

            stream = await (
                session.upsert(k1)
                    .bin("status").set_to("active")
                    .where(f"$.age >= {min_age}")
                .delete(k2, k3)
                .upsert(k4)
                    .bin("flag").set_to(True)
                .default_where("$.active == true")
                .execute()
            )
            # upsert(k1) keeps its own where(); the delete and
            # second upsert inherit default_where.

        Args:
            expression: DSL string or ``FilterExpression``.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`where`: Per-operation filter on the current operation.
        """
        if isinstance(expression, str):
            self._default_filter_expression = parse_dsl(expression)
        else:
            self._default_filter_expression = expression
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> QueryBuilder:
        """Set a default TTL applied to chained operations that lack their own.

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
        bins = self._bins
        statement = Statement(self._namespace, self._set_name, bins)
        if self._filter_records:
            hint = self._query_hint
            needs_rebuild = (
                hint is not None
                and (hint.index_name is not None or hint.bin_name is not None)
            )
            filters = []
            for rec in self._filter_records:
                if needs_rebuild and hint is not None and rec.method is not None:
                    filters.append(rec.rebuild_for_hint(hint))
                else:
                    filters.append(rec.filter)
            statement.filters = filters
        return statement

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Execute the query and return a :class:`RecordStream`.

        Handles single-key, batch-key, and dataset queries.  When a chain
        contains multiple operations, each operation is executed and results
        are combined into a single stream.

        Args:
            on_error: Controls how per-record errors are surfaced.

                - ``None`` (default): single-key operations raise on error;
                  batch / multi-key operations embed errors in the stream.
                - ``ErrorStrategy.IN_STREAM``: always embed errors in the
                  stream as ``RecordResult`` entries.
                - A callable ``(key, index, exception) -> None``: errors are
                  dispatched to the callback and excluded from the stream.

        Returns:
            A :class:`~aerospike_fluent.record_stream.RecordStream` of
            :class:`~aerospike_fluent.record_result.RecordResult` rows.

        Raises:
            AerospikeError: If the builder mixes dataset query with per-bin read
                operations (unsupported combination).
            AerospikeError: Typed subclasses for timeouts, connection failures, etc.
                when the client raises instead of embedding errors.

        Example:
            Single-key read with default error handling::

                stream = await session.query(key).bins(["x"]).execute()
                row = await stream.first_or_raise()

            Multi-key read, keep errors in the stream::

                stream = await (
                    session.query(k1, k2, k3)
                        .execute(on_error=ErrorStrategy.IN_STREAM)
                )
                rows = await stream.collect()

        See Also:
            :class:`~aerospike_fluent.error_strategy.ErrorStrategy`: ``on_error`` options.
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
            if self._specs_require_sequential_run():
                sub_disp = _resolve_disposition(on_error, is_single_key=False)
                streams: List[RecordStream] = []
                for spec in self._specs:
                    streams.append(
                        await self._execute_spec(spec, sub_disp, handler))
                return RecordStream.chain(streams)
            batch_policy = None
            if self._behavior is not None:
                batch_policy = to_batch_policy(
                    self._behavior.get_settings(
                        OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
            all_ops: list = []
            all_keys: List[Key] = []
            for spec in self._specs:
                all_keys.extend(spec.keys)
                all_ops.extend(self._spec_to_batch_ops(spec))
            try:
                batch_records = await self._client.batch(batch_policy, all_ops)
            except Exception as e:
                return self._handle_batch_error(all_keys, e, disp, handler)
            return self._filtered_batch_stream(batch_records, disp, handler)

        # Dataset query path (no keys were specified)
        if self._operations:
            raise AerospikeError(
                "Bin-level read operations are not supported on dataset/index "
                "queries (requires Advanced Bin Projection, not yet available)",
                result_code=ResultCode.OP_NOT_APPLICABLE,
            )
        return await self._execute_dataset_query()

    @staticmethod
    def _reject_unsupported_background_write_ops(
        operations: Sequence[Any],
    ) -> None:
        reject_unsupported_background_write_ops(operations)

    def _make_background_write_policy(self) -> WritePolicy:
        return make_background_write_policy(
            self._behavior,
            self._filter_expression,
            None,
            None,
        )

    async def execute_background_task(self) -> ExecuteTask:
        """Run a background write against all records matching this dataset query.

        Returns a server task handle; poll with ``wait_till_complete`` or
        ``query_status``. Requires :meth:`with_write_operations`; only scalar
        ``Operation`` / expression writes are allowed.

        Raises:
            ValueError: If the builder targets keys or has no write operations.
            AerospikeError: If unsupported operation types are present.
        """
        self._finalize_current_spec()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if not self._operations:
            raise ValueError(
                "At least one write operation is required; use "
                "with_write_operations(...).",
            )
        self._reject_unsupported_background_write_ops(self._operations)
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        try:
            return await self._client.query_operate(
                wp, statement, list(self._operations))
        except Exception as e:
            raise _convert_pac_exception(e) from e

    async def execute_udf_background_task(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Apply a registered UDF to matching records as a background task.

        Do not use :meth:`with_write_operations` on the same builder.

        Raises:
            ValueError: If the builder targets keys or has write operations set.
        """
        self._finalize_current_spec()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if self._operations:
            raise ValueError(
                "Do not combine with_write_operations with "
                "execute_udf_background_task.",
            )
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        py_args: Optional[List[Any]] = list(args) if args is not None else None
        try:
            return await self._client.query_execute_udf(
                wp, statement, package_name, function_name, py_args)
        except Exception as e:
            raise _convert_pac_exception(e) from e

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
            udf_package=None,
            udf_function=None,
            udf_args=None,
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

    def _set_current_keys_from_varargs(self, keys: tuple[Key, ...]) -> None:
        if len(keys) == 1:
            self._single_key = keys[0]
            self._keys = None
        else:
            self._keys = list(keys)
            self._single_key = None

    def _clear_pending_udf_state(self) -> None:
        self._udf_package = None
        self._udf_function = None
        self._udf_args = None

    def _finalize_udf_spec(self) -> None:
        if self._udf_function is None:
            return
        if self._udf_package is None:
            raise ValueError("UDF package name is required")
        if self._single_key is not None:
            keys: List[Key] = [self._single_key]
        elif self._keys is not None:
            keys = list(self._keys)
        else:
            return
        filt = self._filter_expression or self._default_filter_expression
        udf_args: Optional[List[Any]] = (
            list(self._udf_args) if self._udf_args is not None else None
        )
        self._specs.append(_OperationSpec(
            keys=keys,
            operations=[],
            bins=None,
            filter_expression=filt,
            op_type="udf",
            generation=None,
            ttl_seconds=None,
            durable_delete=None,
            udf_package=self._udf_package,
            udf_function=self._udf_function,
            udf_args=udf_args,
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
        self._clear_pending_udf_state()

    def _specs_require_sequential_run(self) -> bool:
        return any(spec.op_type == "udf" for spec in self._specs)

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

        if op_type == "udf":
            if len(keys) == 1:
                return await self._execute_single_key_udf(spec, disp, handler)
            return await self._execute_batch_udf(spec, disp, handler)

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

    def _make_udf_write_policy(self, spec: _OperationSpec) -> WritePolicy:
        if self._behavior is not None:
            wp = to_write_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        else:
            wp = WritePolicy()
        if spec.filter_expression is not None:
            wp.filter_expression = spec.filter_expression
        return wp

    async def _execute_single_key_udf(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        pkg = spec.udf_package
        fn = spec.udf_function
        if pkg is None or fn is None:
            raise ValueError("UDF spec missing package or function name")
        wp = self._make_udf_write_policy(spec)
        try:
            val = await self._client.execute_udf(
                wp, key, pkg, fn, spec.udf_args)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="udf")
        return RecordStream.from_list([
            RecordResult(
                key=key,
                record=None,
                result_code=ResultCode.OK,
                index=0,
                udf_result=val,
            ),
        ])

    async def _execute_batch_udf(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        pkg = spec.udf_package
        fn = spec.udf_function
        if pkg is None or fn is None:
            raise ValueError("UDF spec missing package or function name")
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        udf_policy: Optional[BatchUDFPolicy] = None
        fe = spec.filter_expression
        if fe is not None:
            up = BatchUDFPolicy()
            up.filter_expression = fe
            udf_policy = up
        try:
            batch_records = await self._client.batch_apply(
                batch_policy,
                udf_policy,
                spec.keys,
                pkg,
                fn,
                spec.udf_args,
            )
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(
            batch_records, disp, handler, op_type="udf")

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
                    raise _result_code_to_exception(
                        r.result_code, str(r.result_code), r.in_doubt)
                if disp is _ErrorDisposition.HANDLER and handler is not None:
                    handler(r.key, r.index, _result_code_to_exception(
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
        pfc_exc = _convert_pac_exception(exc)
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
        pfc_exc = _convert_pac_exception(exc)
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
                        key, operations=list(spec.operations), policy=brp))
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
                    key, list(spec.operations), policy=bwp))
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
            bwp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            bwp.generation = spec.generation
        if spec.ttl_seconds is not None:
            bwp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.durable_delete:
            bwp.durable_delete = True
        return bwp

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

        hint = self._query_hint
        if hint is not None and hint.query_duration is not None:
            policy.expected_duration = hint.query_duration

        self._resolve_index_context()

        partition_filter = self._partition_filter or PartitionFilter.all()

        if self._where_dsl is not None and self._index_context is not None:
            self._auto_generate_filters(hint, policy)

        statement = self._build_statement()

        try:
            recordset = await self._client.query(
                policy, partition_filter, statement)
        except Exception as e:
            raise _convert_pac_exception(e) from e

        if self._chunk_size is not None and self._chunk_size > 0:
            client = self._client

            async def _reexecute(pf: PartitionFilter) -> Any:
                return await client.query(policy, pf, statement)

            return RecordStream.from_chunked_recordset(
                recordset,
                reexecute=_reexecute,
                limit=0,
            )

        return RecordStream.from_recordset(recordset)

    def _resolve_index_context(self) -> None:
        """Auto-populate ``_index_context`` from the monitor when not set."""
        if self._index_context is not None:
            return
        if self._indexes_monitor is None:
            return
        ctx = self._indexes_monitor.get_index_context(self._namespace)
        if ctx is not None:
            self._index_context = ctx

    def _auto_generate_filters(
        self,
        hint: Optional[QueryHint],
        policy: QueryPolicy,
    ) -> None:
        """Parse DSL with index context to generate Filter + Exp.

        When a hint provides ``index_name`` or ``bin_name``, those overrides
        are forwarded to the filter generation pipeline.
        """
        if self._where_dsl is None or self._index_context is None:
            return

        hint_index = hint.index_name if hint is not None else None
        hint_bin = hint.bin_name if hint is not None else None

        result = parse_dsl_with_index(
            self._where_dsl,
            self._index_context,
            hint_index_name=hint_index,
            hint_bin_name=hint_bin,
        )
        if result.filter is not None:
            self._filter_records.append(_FilterRecord(filter=result.filter))
            log.debug(
                "Auto-selected secondary index filter for query on %s.%s",
                self._namespace,
                self._set_name,
            )
        if result.exp is not None:
            policy.filter_expression = result.exp


class WriteSegmentBuilder(_WriteVerbs):
    """Accumulate scalar and CDT writes for the current operation's key(s).

    Obtained from :class:`QueryBuilder` after a write verb or from
    :class:`WriteBinBuilder` when chaining. Call :meth:`put`, :meth:`bin`,
    expression helpers, optional :meth:`where` / TTL / generation guards, then
    :meth:`execute` on this object or transition with :meth:`query` /
    another write verb on the mixin.

    Example:
        Upsert two bins, then read the stream of results::

            stream = await (
                session.upsert(key)
                    .put({"name": "Ada", "score": 100})
                    .execute()
            )

    See Also:
        :meth:`QueryBuilder.execute`: Runs all chained operations.
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
        """Apply ``Operation.put`` for each bin in the mapping.

        Args:
            bins: Map of bin name to value.

        Returns:
            This segment for chaining.

        Example::
            await session.upsert(key).put({"email": "a@b.com", "age": 30}).execute()

        See Also:
            :meth:`bin`: Per-bin CDT or scalar follow-ups.
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

    def add_operation(self, op: Any) -> None:
        """Append an operation (used by CDT action builders)."""
        self._qb._operations.append(op)

    # -- Scalar bin operations (direct on segment) ----------------------------

    def set_to(self, bin_name: str, value: Any) -> WriteSegmentBuilder:
        """Set a bin to *value*."""
        return self._add_op(Operation.put(bin_name, value))

    def add(self, bin_name: str, value: Any) -> WriteSegmentBuilder:
        """Add a numeric *value* to a bin."""
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

    # -- Per-operation settings ------------------------------------------------

    @overload
    def where(self, expression: str) -> WriteSegmentBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> WriteSegmentBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> WriteSegmentBuilder:
        """Set a filter expression on the current write segment.

        Args:
            expression: DSL string or pre-built FilterExpression.

        Returns:
            self for method chaining.
        """
        if isinstance(expression, str):
            self._qb._filter_expression = parse_dsl(expression)
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
        """Run the parent :class:`QueryBuilder` stack (same as ``self._qb.execute``).

        Args:
            on_error: Optional :class:`~aerospike_fluent.error_strategy.ErrorStrategy`
                or error callback; see :meth:`QueryBuilder.execute`.

        Returns:
            :class:`~aerospike_fluent.record_stream.RecordStream` of results.

        Example::
            stream = await session.upsert(key).put({"x": 1}).execute()
            await stream.first_or_raise()

        Raises:
            Same as :meth:`QueryBuilder.execute`.
        """
        return await self._qb.execute(on_error)


class WriteBinBuilder(_WriteVerbs):
    """Per-bin write builder inside a :class:`WriteSegmentBuilder`.

    Start with :meth:`WriteSegmentBuilder.bin`. Scalar methods delegate to the
    segment; ``map_*`` and ``list_*`` append collection operations; nested CDT
    builders capture context for maps and lists. Write verbs on this class
    finalize the segment and start a new one on new keys.

    Example:
        Set a map key and append to a list within the same write::

            await (
                session.upsert(key)
                    .bin("config").on_map_key("level").set_to(5)
                    .bin("tags").list_append(value="new_tag")
                    .execute()
            )

    See Also:
        :class:`QueryBinBuilder`: Read-side analogue for queries.
    """

    __slots__ = ("_segment", "_bin")

    def __init__(self, segment: WriteSegmentBuilder, bin_name: str) -> None:
        self._segment = segment
        self._bin = bin_name

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> WriteSegmentBuilder:
        """Set the bin to *value* (``Operation.put``)."""
        return self._segment.set_to(self._bin, value)

    def add(self, value: Any) -> WriteSegmentBuilder:
        """Add a numeric *value* to the bin (``Operation.add``)."""
        return self._segment.add(self._bin, value)

    def increment_by(self, value: Any) -> WriteSegmentBuilder:
        """Alias of :meth:`add`."""
        return self.add(value)

    def append(self, value: str) -> WriteSegmentBuilder:
        """String append (``Operation.append``)."""
        return self._segment.append(self._bin, value)

    def prepend(self, value: str) -> WriteSegmentBuilder:
        """String prepend (``Operation.prepend``)."""
        return self._segment.prepend(self._bin, value)

    def remove(self) -> WriteSegmentBuilder:
        """Drop the bin (write ``None``)."""
        return self._segment.remove_bin(self._bin)

    def get(self) -> WriteSegmentBuilder:
        """Return the bin value after writes complete (``Operation.get_bin``)."""
        return self._segment.get(self._bin)

    # -- CDT list structural operations ---------------------------------------

    def list_add(self, value: Any) -> WriteSegmentBuilder:
        """Add *value* to an ordered list (sorted insert).

        Args:
            value: Element to insert in sorted order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            ListOperation.append(self._bin, value, ListPolicy(ListOrderType.ORDERED, None)),
        )

    def list_append(self, value: Any) -> WriteSegmentBuilder:
        """Append *value* to the end of an unordered list.

        Example::
            .bin("tags").list_append(value="python")
        """
        return self._segment._add_op(
            ListOperation.append(self._bin, value, ListPolicy(None, None)),
        )

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> WriteSegmentBuilder:
        """Remove all entries from the map bin.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.clear(self._bin))

    def map_size(self) -> WriteSegmentBuilder:
        """Return the map element count (read within operate).

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.size(self._bin))

    def map_upsert_items(self, items: Any) -> WriteSegmentBuilder:
        """Put multiple map entries (create or update each key).

        Example::
            .bin("settings").map_upsert_items({"theme": "dark", "lang": "en"})
        """
        pairs = _map_item_pairs(items)
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, MapPolicy(None, None)),
        )

    def map_insert_items(self, items: Any) -> WriteSegmentBuilder:
        """Put map entries only for keys that do not yet exist."""
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.CREATE_ONLY)
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, policy),
        )

    def map_update_items(self, items: Any) -> WriteSegmentBuilder:
        """Update existing map entries only (no new keys).

        Args:
            items: Key-value pairs to update for existing keys only.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = MapPolicy.new_with_flags(None, MapWriteFlags.UPDATE_ONLY)
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, policy),
        )

    def map_create(self, order: MapOrder) -> WriteSegmentBuilder:
        """Create an empty map with the given key order.

        Args:
            order: Map key sort order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.create(self._bin, order))

    def map_set_policy(self, order: MapOrder) -> WriteSegmentBuilder:
        """Set map sort order policy without changing entries.

        Args:
            order: Map key sort order policy.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            MapOperation.set_map_policy(self._bin, MapPolicy(order, None)),
        )

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> WriteSegmentBuilder:
        """Remove all elements from the list bin.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.clear(self._bin))

    def list_sort(
        self, flags: ListSortFlags = ListSortFlags.DEFAULT,
    ) -> WriteSegmentBuilder:
        """Sort the list bin.

        Args:
            flags: Sort behavior flags.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.sort(self._bin, flags))

    def list_size(self) -> WriteSegmentBuilder:
        """Return the list element count (read within operate).

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.size(self._bin))

    def list_append_items(self, items: Any) -> WriteSegmentBuilder:
        """Append values to an unordered list."""
        return self._segment._add_op(
            ListOperation.append_items(
                self._bin, items, ListPolicy(None, None),
            ),
        )

    def list_add_items(self, items: Any) -> WriteSegmentBuilder:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Sequence of values to insert in sorted order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            ListOperation.append_items(
                self._bin, items, ListPolicy(ListOrderType.ORDERED, None),
            ),
        )

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> WriteSegmentBuilder:
        """Create an empty list with the given order.

        Args:
            order: List element order.
            pad: Whether to pad with None entries.
            persist_index: Whether to persist element indices.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            ListOperation.create(self._bin, order, pad, persist_index),
        )

    def list_set_order(self, order: ListOrderType) -> WriteSegmentBuilder:
        """Set list sort order without changing elements.

        Args:
            order: List element order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.set_order(self._bin, order))

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

    # -- Map navigation (singular -> CdtWriteBuilder) --------------------------

    def on_map_index(self, index: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            lambda rt: MapOperation.remove_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(self, key: Any) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        _mp = MapPolicy(None, None)
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            lambda rt: MapOperation.remove_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_key(key),
            set_to_factory=lambda v: MapOperation.put(b, key, v, _mp),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _mp),
        )

    def on_map_rank(self, rank: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by rank (0 = lowest value)."""
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            lambda rt: MapOperation.remove_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
        )

    # -- Map navigation (invertable -> CdtWriteInvertableBuilder) -------------

    def on_map_value(self, value: Any) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            lambda rt: MapOperation.remove_by_value(b, value, rt),
            MapReturnType, is_map=True,
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by index range.

        Args:
            index: List index (0-based, negative counts from end).
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
            self._segment, get_f, rm_f, MapReturnType, is_map=True,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by key range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_key_range(b, start, end, rt),
            MapReturnType, is_map=True,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by rank range.

        Args:
            rank: Rank position (0 = lowest value).
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
            self._segment, get_f, rm_f, MapReturnType, is_map=True,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_value_range(b, start, end, rt),
            MapReturnType, is_map=True,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Map key to target.
            index: Index offset from the anchor key.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
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
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map entries by value rank range relative to an anchor value.

        Args:
            value: Value to match.
            rank: Rank offset from the anchor value.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: MapOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            MapReturnType, is_map=True,
        )

    def on_map_key_list(self, keys: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a list of keys.

        Args:
            keys: Map keys to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            lambda rt: MapOperation.remove_by_key_list(b, keys, rt),
            MapReturnType, is_map=True,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            lambda rt: MapOperation.remove_by_value_list(b, values, rt),
            MapReturnType, is_map=True,
        )

    # -- List navigation (singular -> CdtWriteBuilder) ------------------------

    def on_list_index(self, index: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a list element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).

        Example::
            .bin("items").on_list_index(0).set_to("first")
        """
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            lambda rt: ListOperation.remove_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_index(index),
        )

    def on_list_rank(self, rank: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a list element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            lambda rt: ListOperation.remove_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- List navigation (invertable -> CdtWriteInvertableBuilder) ------------

    def on_list_value(self, value: Any) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            lambda rt: ListOperation.remove_by_value(b, value, rt),
            ListReturnType, is_map=False,
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by index range.

        Args:
            index: List index (0-based, negative counts from end).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            lambda rt: ListOperation.remove_by_index_range(b, index, count, rt),
            ListReturnType, is_map=False,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by rank range.

        Args:
            rank: Rank position (0 = lowest value).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            lambda rt: ListOperation.remove_by_rank_range(b, rank, count, rt),
            ListReturnType, is_map=False,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            lambda rt: ListOperation.remove_by_value_range(b, start, end, rt),
            ListReturnType, is_map=False,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by value rank range relative to an anchor value.

        Args:
            value: Value to match.
            rank: Rank offset from the anchor value.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: ListOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            ListReturnType, is_map=False,
        )

    def on_list_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            lambda rt: ListOperation.remove_by_value_list(b, values, rt),
            ListReturnType, is_map=False,
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
    """Per-bin reads and CDT navigation for :class:`QueryBuilder` (and sync twin).

    The type parameter is the parent builder type; the parent must implement
    ``add_operation``. Use :meth:`get` for whole-bin reads, :meth:`select_from`
    for expression reads, ``on_map_*`` / ``on_list_*`` for paths, then
    :meth:`QueryBuilder.execute`. Write verbs delegate to the parent to chain
    writes after reads.

    Example:
        Read map keys and list size in a single query::

            stream = await (
                session.query(key)
                    .bin("settings").on_map_key("theme").get_values()
                    .bin("tags").list_size()
                    .execute()
            )

    See Also:
        :class:`WriteBinBuilder`: Per-bin write builder.
        :class:`~aerospike_fluent.aio.operations.cdt_read.CdtReadBuilder`: Nested reads.
    """

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: _T, bin_name: str) -> None:
        self._parent = parent
        self._bin = bin_name

    # -- Simple read ----------------------------------------------------------

    def get(self) -> _T:
        """Include the bin value in the read result.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`select_from`: Virtual bin from an expression.
        """
        self._parent.add_operation(Operation.get_bin(self._bin))  # type: ignore[union-attr]
        return self._parent

    def map_size(self) -> _T:
        """Return the number of entries in the map."""
        self._parent.add_operation(MapOperation.size(self._bin))  # type: ignore[union-attr]
        return self._parent

    def list_size(self) -> _T:
        """Read list length into the operate/read result."""
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
        """Navigate to a map element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element(s).
        """
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(self, key: Any) -> CdtReadBuilder[_T]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element(s).
        """
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_key(key),
        )

    def on_map_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a map element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
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

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Map key to target.
            index: List index (0-based, negative counts from end).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the targeted element(s).
        """
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            MapReturnType,
            is_map=True,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map entries by value rank range relative to an anchor value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
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
        """Navigate to a list element by index.

        Example::
            .bin("items").on_list_index(-1).get_values()
        """
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_index(index),
        )

    def on_list_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a list element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
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

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by value rank range relative to an anchor value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
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
