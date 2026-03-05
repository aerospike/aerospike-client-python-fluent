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

"""KeyValueOperation - Builder for key-value operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from aerospike_async import (
    BitOperation,
    Client,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    FilterExpression,
    GenerationPolicy,
    Key,
    ListOperation,
    MapOperation,
    Operation,
    ReadPolicy,
    Record,
    WritePolicy,
)
from aerospike_async.exceptions import ServerError

from aerospike_fluent.dsl.parser import parse_dsl
from aerospike_fluent.policy.policy_mapper import to_read_policy, to_write_policy

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape

# Derive int constants from PAC enums (PyO3 eq_int enums support int())
_EXP_WRITE_DEFAULT = int(ExpWriteFlags.DEFAULT)
_EXP_WRITE_CREATE_ONLY = int(ExpWriteFlags.CREATE_ONLY)
_EXP_WRITE_UPDATE_ONLY = int(ExpWriteFlags.UPDATE_ONLY)
_EXP_WRITE_ALLOW_DELETE = int(ExpWriteFlags.ALLOW_DELETE)
_EXP_WRITE_POLICY_NO_FAIL = int(ExpWriteFlags.POLICY_NO_FAIL)
_EXP_WRITE_EVAL_NO_FAIL = int(ExpWriteFlags.EVAL_NO_FAIL)

_EXP_READ_DEFAULT = int(ExpReadFlags.DEFAULT)
_EXP_READ_EVAL_NO_FAIL = int(ExpReadFlags.EVAL_NO_FAIL)

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class BinBuilder:
    """
    Builder for chaining bin operations (e.g., .bin("name").set_to("Tim")).

    This class enables fluent chaining of bin operations.
    """

    def __init__(self, operation: KeyValueOperation, bin_name: Optional[str] = None) -> None:
        """
        Initialize a BinBuilder.

        Args:
            operation: The parent KeyValueOperation instance.
            bin_name: Optional initial bin name (for chaining).
        """
        self._operation = operation
        self._bins: Dict[str, Any] = {}
        self._increments: Dict[str, int] = {}
        self._exp_ops: List[ExpOperation] = []
        self._remove_other_bins: bool = False
        self._current_bin: Optional[str] = bin_name

    def bin(self, bin_name: str) -> BinBuilder:
        """
        Start a bin operation chain.
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            self for method chaining.
        """
        self._current_bin = bin_name
        return self

    def set_to(self, value: Any) -> BinBuilder:
        """
        Set a bin value (used after .bin(name)).
        
        Args:
            value: The value to set.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .set_to(value)")
        self._bins[self._current_bin] = value
        self._current_bin = None
        return self

    def increment_by(self, value: int) -> BinBuilder:
        """
        Increment a bin value (used after .bin(name)).
        
        Alias: add().
        
        Args:
            value: The amount to increment by.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .increment_by(value)")
        self._increments[self._current_bin] = self._increments.get(self._current_bin, 0) + value
        self._current_bin = None
        return self

    def add(self, value: int) -> BinBuilder:
        """
        Add (increment) a numeric bin value (used after .bin(name)).
        
        This is an alias for increment_by().
        
        Args:
            value: The amount to add.
        
        Returns:
            self for method chaining.
        """
        return self.increment_by(value)

    def append(self, value: str) -> BinBuilder:
        """
        Append a string to a bin value (used after .bin(name)).
        
        Args:
            value: The string to append.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .append(value)")
        if "_appends" not in self.__dict__:
            self._appends: Dict[str, str] = {}
        self._appends[self._current_bin] = value
        self._current_bin = None
        return self

    def prepend(self, value: str) -> BinBuilder:
        """
        Prepend a string to a bin value (used after .bin(name)).
        
        Args:
            value: The string to prepend.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .prepend(value)")
        if "_prepends" not in self.__dict__:
            self._prepends: Dict[str, str] = {}
        self._prepends[self._current_bin] = value
        self._current_bin = None
        return self

    def remove(self) -> BinBuilder:
        """
        Remove (delete) a bin (used after .bin(name)).
        
        This sets the bin to None, which deletes it from the record.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .remove()")
        if "_removes" not in self.__dict__:
            self._removes: List[str] = []
        self._removes.append(self._current_bin)
        self._current_bin = None
        return self

    def get(self) -> BinBuilder:
        """
        Mark a bin for reading (used after .bin(name)).
        
        The bin value will be included in the result when execute() is called.
        
        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .get()")
        if "_gets" not in self.__dict__:
            self._gets: List[str] = []
        self._gets.append(self._current_bin)
        self._current_bin = None
        return self

    def and_remove_other_bins(self) -> BinBuilder:
        """
        Mark that all bins not explicitly set should be removed.
        
        Returns:
            self for method chaining.
        """
        self._remove_other_bins = True
        return self

    def ensure_generation_is(self, generation: int) -> BinBuilder:
        """
        Set expected generation for optimistic locking.
        
        The operation will fail if the record's current generation
        doesn't match the expected generation.
        
        Args:
            generation: The expected generation number.
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            record = await session.key_value(key=key).get()
            await session.upsert(key) \\
                .ensure_generation_is(record.generation) \\
                .bin("counter").set_to(new_value) \\
                .execute()
            ```
        """
        self._expected_generation = generation
        return self

    # -- Expression operations ------------------------------------------------

    @staticmethod
    def _resolve_expression(expression: Union[str, FilterExpression]) -> FilterExpression:
        """Convert a DSL string to FilterExpression, or pass through."""
        if isinstance(expression, str):
            return parse_dsl(expression)
        return expression

    @staticmethod
    def _build_write_flags(
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

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> BinBuilder:
        """Read a computed value into this bin using a DSL expression.

        The result appears as a virtual bin in the returned record.

        Args:
            expression: DSL string or pre-built FilterExpression.
            ignore_eval_failure: If True, silently return None when the
                expression cannot be evaluated (e.g. missing bin).

        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .select_from()")
        flags = _EXP_READ_EVAL_NO_FAIL if ignore_eval_failure else _EXP_READ_DEFAULT
        expr = self._resolve_expression(expression)
        self._exp_ops.append(ExpOperation.read(self._current_bin, expr, flags))
        self._current_bin = None
        return self

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BinBuilder:
        """Write expression result only if the bin does not exist.

        Fails with BIN_EXISTS_ERROR if the bin already exists (unless
        ``ignore_op_failure=True``).

        Args:
            expression: DSL string or pre-built FilterExpression.
            ignore_op_failure: Suppress error when bin already exists.
            ignore_eval_failure: Suppress error on expression eval failure.
            delete_if_null: Delete the bin if the expression returns nil.

        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .insert_from()")
        flags = self._build_write_flags(
            _EXP_WRITE_CREATE_ONLY, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = self._resolve_expression(expression)
        self._exp_ops.append(ExpOperation.write(self._current_bin, expr, flags))
        self._current_bin = None
        return self

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BinBuilder:
        """Write expression result only if the bin already exists.

        Fails with BIN_NOT_FOUND if the bin does not exist (unless
        ``ignore_op_failure=True``).

        Args:
            expression: DSL string or pre-built FilterExpression.
            ignore_op_failure: Suppress error when bin does not exist.
            ignore_eval_failure: Suppress error on expression eval failure.
            delete_if_null: Delete the bin if the expression returns nil.

        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .update_from()")
        flags = self._build_write_flags(
            _EXP_WRITE_UPDATE_ONLY, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = self._resolve_expression(expression)
        self._exp_ops.append(ExpOperation.write(self._current_bin, expr, flags))
        self._current_bin = None
        return self

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BinBuilder:
        """Write expression result, creating or overwriting the bin.

        Args:
            expression: DSL string or pre-built FilterExpression.
            ignore_op_failure: Suppress error on policy denial.
            ignore_eval_failure: Suppress error on expression eval failure.
            delete_if_null: Delete the bin if the expression returns nil.

        Returns:
            self for method chaining.
        """
        if self._current_bin is None:
            raise ValueError("Must call .bin(name) before .upsert_from()")
        flags = self._build_write_flags(
            _EXP_WRITE_DEFAULT, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = self._resolve_expression(expression)
        self._exp_ops.append(ExpOperation.write(self._current_bin, expr, flags))
        self._current_bin = None
        return self

    def _build_operations(self) -> List[Union[Operation, ListOperation, MapOperation, BitOperation, ExpOperation]]:
        """Collect all accumulated bin operations into a flat list."""
        operations: List[Union[Operation, ListOperation, MapOperation, BitOperation, ExpOperation]] = []
        for bin_name, value in self._bins.items():
            operations.append(Operation.put(bin_name, value))
        for bin_name, increment in self._increments.items():
            operations.append(Operation.add(bin_name, increment))
        if hasattr(self, "_appends"):
            for bin_name, value in self._appends.items():
                operations.append(Operation.append(bin_name, value))
        if hasattr(self, "_prepends"):
            for bin_name, value in self._prepends.items():
                operations.append(Operation.prepend(bin_name, value))
        if hasattr(self, "_removes"):
            for bin_name in self._removes:
                operations.append(Operation.put(bin_name, None))
        operations.extend(self._exp_ops)
        if hasattr(self, "_gets"):
            for bin_name in self._gets:
                operations.append(Operation.get_bin(bin_name))
        return operations

    def _infer_op_type(self) -> str:
        """Derive the op_type string from the parent KVO's write policy."""
        wp = self._operation._write_policy
        if wp is None:
            return "upsert"
        rea = getattr(wp, "record_exists_action", None)
        if rea is None:
            return "upsert"
        from aerospike_async import RecordExistsAction as REA
        _rea_map = {
            REA.CREATE_ONLY: "insert",
            REA.UPDATE_ONLY: "update",
            REA.REPLACE: "replace",
            REA.REPLACE_ONLY: "replace_if_exists",
        }
        return _rea_map.get(rea, "upsert")

    # -- Cross-builder transitions --------------------------------------------

    def _to_query_builder(self):
        """Create a QueryBuilder seeded with the current key's operations."""
        from aerospike_fluent.aio.operations.query import (
            QueryBuilder,
            _OperationSpec,
        )
        kvo = self._operation
        key = kvo._get_key()
        ops = self._build_operations()
        op_type = self._infer_op_type()

        qb = QueryBuilder(
            client=kvo._client,
            namespace=kvo._namespace,
            set_name=kvo._set_name,
            behavior=kvo._behavior,
        )
        if ops:
            qb._specs.append(_OperationSpec(
                keys=[key],
                operations=ops,
                op_type=op_type,
            ))
        return qb

    def query(self, arg1, *more_keys):
        """Transition to a QueryBuilder for chaining a read segment.

        Packages the current key's accumulated writes as a spec
        and returns a QueryBuilder ready for the next segment.

        Args:
            arg1: A single Key or List[Key].
            *more_keys: Additional keys (varargs).

        Returns:
            A QueryBuilder for method chaining.
        """
        qb = self._to_query_builder()
        qb._op_type = None
        qb._set_current_keys(arg1, *more_keys)
        return qb

    def _transition_write(self, op_type: str, arg1, *more_keys):
        """Transition to a WriteSegmentBuilder for chaining a write segment."""
        from aerospike_fluent.aio.operations.query import WriteSegmentBuilder
        qb = self._to_query_builder()
        qb._op_type = op_type
        qb._set_current_keys(arg1, *more_keys)
        return WriteSegmentBuilder(qb)

    def upsert(self, arg1, *more_keys):
        """Transition to an upsert write segment."""
        return self._transition_write("upsert", arg1, *more_keys)

    def insert(self, arg1, *more_keys):
        """Transition to an insert (create-only) write segment."""
        return self._transition_write("insert", arg1, *more_keys)

    def update(self, arg1, *more_keys):
        """Transition to an update (update-only) write segment."""
        return self._transition_write("update", arg1, *more_keys)

    def replace(self, arg1, *more_keys):
        """Transition to a replace write segment."""
        return self._transition_write("replace", arg1, *more_keys)

    def replace_if_exists(self, arg1, *more_keys):
        """Transition to a replace-if-exists write segment."""
        return self._transition_write("replace_if_exists", arg1, *more_keys)

    def delete(self, arg1, *more_keys):
        """Transition to a delete write segment."""
        return self._transition_write("delete", arg1, *more_keys)

    def touch(self, arg1, *more_keys):
        """Transition to a touch write segment."""
        return self._transition_write("touch", arg1, *more_keys)

    def exists(self, arg1, *more_keys):
        """Transition to an exists-check segment."""
        return self._transition_write("exists", arg1, *more_keys)

    async def execute(self) -> Optional[Record]:
        """Execute the accumulated bin operations.

        Returns:
            Record if any .get() operations were included, None otherwise.
        """
        operations = self._build_operations()

        # Handle remove_other_bins
        if self._remove_other_bins:
            record = await self._operation.get()
            if record and record.bins:
                existing_bins = set(record.bins.keys())
                set_bins = set(self._bins.keys())
                bins_to_remove = existing_bins - set_bins
                for bin_name in bins_to_remove:
                    operations.append(Operation.put(bin_name, None))

        # Set up generation policy if specified
        if hasattr(self, "_expected_generation"):
            if self._operation._write_policy is None:
                self._operation._write_policy = WritePolicy()
            self._operation._write_policy.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            self._operation._write_policy.generation = self._expected_generation

        if operations:
            return await self._operation.operate(operations)
        elif self._bins:
            await self._operation.put(self._bins)
            return None
        return None


class KeyValueOperation:
    """
    Builder for key-value operations (get, put, delete, etc.).
    
    This class provides a fluent interface for building and executing
    key-value operations on a single record.
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
        key: Union[str, int],
        behavior: Optional[Behavior] = None,
    ) -> None:
        """
        Initialize a KeyValueOperation builder.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            key: The record key (string or integer).
            behavior: Optional Behavior for deriving policies.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._key = key
        self._behavior = behavior
        self._read_policy: Optional[ReadPolicy] = None
        self._write_policy: Optional[WritePolicy] = None
        self._bins: Optional[List[str]] = None
        self._durable_delete: Optional[bool] = None

    def _get_key(self) -> Key:
        """Create a Key object from the namespace, set, and key."""
        return Key(self._namespace, self._set_name, self._key)

    def _resolve_read_policy(self) -> ReadPolicy:
        """Return the explicit read policy, or derive one from Behavior."""
        if self._read_policy is not None:
            return self._read_policy
        if self._behavior is not None:
            settings = self._behavior.get_settings(OpKind.READ, OpShape.POINT)
            return to_read_policy(settings)
        return ReadPolicy()

    def _resolve_write_policy(self, kind: OpKind = OpKind.WRITE_NON_RETRYABLE) -> WritePolicy:
        """Return the explicit write policy, or derive one from Behavior."""
        if self._write_policy is not None:
            return self._write_policy
        if self._behavior is not None:
            settings = self._behavior.get_settings(kind, OpShape.POINT)
            return to_write_policy(settings)
        return WritePolicy()

    def with_read_policy(self, policy: ReadPolicy) -> KeyValueOperation:
        """
        Set the read policy for this operation.
        
        Args:
            policy: The read policy to use.
        
        Returns:
            self for method chaining.
        """
        self._read_policy = policy
        return self

    def with_write_policy(self, policy: WritePolicy) -> KeyValueOperation:
        """
        Set the write policy for this operation.
        
        Args:
            policy: The write policy to use.
        
        Returns:
            self for method chaining.
        """
        self._write_policy = policy
        return self

    def bins(self, bin_names: List[str]) -> KeyValueOperation:
        """
        Specify which bins to retrieve for get operations.
        
        Args:
            bin_names: List of bin names to retrieve.
        
        Returns:
            self for method chaining.
        """
        self._bins = bin_names
        return self

    def ensure_generation_is(self, generation: int) -> KeyValueOperation:
        """
        Set expected generation for optimistic locking.
        
        The operation will fail if the record's current generation
        doesn't match the expected generation.
        
        Args:
            generation: The expected generation number.
        
        Returns:
            self for method chaining.
        
        Example:
            ```python
            record = await session.key_value(key=key).get()
            await session.upsert(key) \\
                .ensure_generation_is(record.generation) \\
                .bin("counter").set_to(new_value) \\
                .execute()
            ```
        """
        if self._write_policy is None:
            self._write_policy = WritePolicy()
        self._write_policy.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
        self._write_policy.generation = generation
        return self

    def bin(self, bin_name: str) -> BinBuilder:
        """
        Start a bin operation chain (e.g., .bin("name").set_to("Tim")).
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            A BinBuilder for chaining operations.
        
        Example:
            await session.key_value(key="mykey").bin("name").set_to("Tim").bin("age").set_to(25).execute()
        """
        return BinBuilder(self, bin_name)

    async def get(self) -> Optional[Record]:
        """
        Get a record.
        
        Returns:
            The record if found, None otherwise.
        """
        policy = self._resolve_read_policy()
        key = self._get_key()
        try:
            return await self._client.get(policy, key, self._bins)
        except ServerError as e:
            if "KeyNotFoundError" in str(e):
                return None
            raise convert_pac_exception(e) from e

    async def put(self, bins: Dict[str, Any]) -> None:
        """
        Put (create or update) a record.
        
        Args:
            bins: Dictionary of bin name to value mappings.
        """
        policy = self._resolve_write_policy()
        if self._durable_delete is not None:
            policy.durable_delete = self._durable_delete
        key = self._get_key()
        try:
            await self._client.put(policy, key, bins)
        except Exception as e:
            raise convert_pac_exception(e) from e

    def set_bins(self, bins: Dict[str, Any]) -> KeyValueOperation:
        """
        Set bins for a put operation (alias for put, but returns self for chaining).
        
        Args:
            bins: Dictionary of bin name to value mappings.
        
        Returns:
            self for method chaining.
        
        Note:
            This method stores the bins but does not execute the operation.
            Call .execute() to perform the put.
        """
        self._pending_bins = bins
        return self

    async def execute(self) -> Optional[bool]:
        """
        Execute a pending operation (put, delete, or exists).
        
        For put operations (used with .set_bins()):
            await session.key_value(key="mykey").set_bins({"name": "Tim"}).execute()
        
        For delete operations:
            await session.delete(key="mykey").execute()  # Returns bool
        
        For exists operations:
            await session.exists(key="mykey").execute()  # Returns bool
        """
        if hasattr(self, '_pending_bins'):
            await self.put(self._pending_bins)
            delattr(self, '_pending_bins')
            return None
        elif hasattr(self, '_pending_delete'):
            result = await self._delete_impl()
            delattr(self, '_pending_delete')
            return result
        else:
            raise ValueError("No pending operation to execute. Use .set_bins() or .delete() first.")

    def durably(self, durable: bool = True) -> KeyValueOperation:
        """
        Set durable delete flag for delete operations.
        
        Args:
            durable: If True, the delete will be durable (default: True).
        
        Returns:
            self for method chaining.
        
        Example:
            await session.delete(key="mykey").durably(False).delete()
        """
        self._durable_delete = durable
        return self

    async def delete(self) -> bool:
        """
        Delete a record.
        
        Returns:
            True if the record existed, False otherwise.
        """
        policy = self._resolve_write_policy(OpKind.WRITE_RETRYABLE)
        if self._durable_delete is not None:
            policy.durable_delete = self._durable_delete
        key = self._get_key()
        try:
            return await self._client.delete(policy, key)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def exists(self) -> bool:
        """
        Check if a record exists.
        
        Returns:
            True if the record exists, False otherwise.
        """
        policy = self._resolve_read_policy()
        key = self._get_key()
        try:
            return await self._client.exists(policy, key)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def add(self, bins: Dict[str, int]) -> None:
        """
        Add (increment) integer values in bins.
        
        Args:
            bins: Dictionary of bin name to integer increment values.
        """
        policy = self._resolve_write_policy(OpKind.WRITE_NON_RETRYABLE)
        key = self._get_key()
        try:
            await self._client.add(policy, key, bins)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def append(self, bins: Dict[str, str]) -> None:
        """
        Append strings to string bins.
        
        Args:
            bins: Dictionary of bin name to string values to append.
        """
        policy = self._resolve_write_policy(OpKind.WRITE_NON_RETRYABLE)
        key = self._get_key()
        try:
            await self._client.append(policy, key, bins)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def prepend(self, bins: Dict[str, str]) -> None:
        """
        Prepend strings to string bins.
        
        Args:
            bins: Dictionary of bin name to string values to prepend.
        """
        policy = self._resolve_write_policy(OpKind.WRITE_NON_RETRYABLE)
        key = self._get_key()
        try:
            await self._client.prepend(policy, key, bins)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def touch(self) -> None:
        """
        Touch (update TTL) a record without modifying its data.
        """
        policy = self._resolve_write_policy(OpKind.WRITE_NON_RETRYABLE)
        key = self._get_key()
        try:
            await self._client.touch(policy, key)
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def operate(
        self,
        operations: List[Union[Operation, ListOperation, MapOperation, BitOperation, ExpOperation]],
    ) -> Optional[Record]:
        """
        Execute multiple operations atomically on a record.

        This method supports:
        - Basic operations: Operation.put(), Operation.get(), etc.
        - List operations: ListOperation.append(), ListOperation.get(), etc.
        - Map operations: MapOperation.put(), MapOperation.get_by_key(), etc.
        - Bit operations: BitOperation.set(), BitOperation.get(), etc.
        - Expression operations: ExpOperation.read(), ExpOperation.write().
        
        Args:
            operations: List of operations to execute atomically.
        
        Returns:
            The record with results of the operations, or None if the record
            doesn't exist and no read operations were performed.
        
        Example:
            # Put a value and get the record
            record = await session.key_value(key="mykey").operate([
                Operation.put("bin1", "value1"),
                Operation.get()
            ])
            
            # List append and get
            record = await session.key_value(key="mykey").operate([
                ListOperation.append("list_bin", "new_item", ListPolicy()),
                ListOperation.size("list_bin")
            ])
            
            # Map put and get
            record = await session.key_value(key="mykey").operate([
                MapOperation.put("map_bin", "key1", "value1", MapPolicy()),
                MapOperation.get_by_key("map_bin", "key1", MapReturnType.VALUE)
            ])
        """
        policy = self._resolve_write_policy(OpKind.WRITE_NON_RETRYABLE)
        key = self._get_key()
        try:
            return await self._client.operate(policy, key, operations)
        except ServerError as e:
            if "KeyNotFoundError" in str(e):
                return None
            raise convert_pac_exception(e) from e