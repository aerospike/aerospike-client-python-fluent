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

"""SyncKeyValueOperation - Synchronous wrapper for key-value operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from aerospike_async import (
    BitOperation,
    ListOperation,
    MapOperation,
    Operation,
    ReadPolicy,
    Record,
    WritePolicy,
)

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.operations.key_value import BinBuilder, KeyValueOperation
from aerospike_fluent.sync.client import _EventLoopManager

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class SyncBinBuilder:
    """
    Synchronous wrapper for BinBuilder.

    Provides the same fluent interface as BinBuilder but with
    synchronous methods that hide async/await.
    """

    def __init__(self, operation: SyncKeyValueOperation, bin_name: Optional[str] = None) -> None:
        """
        Initialize a SyncBinBuilder.

        Args:
            operation: The parent SyncKeyValueOperation instance.
            bin_name: Optional initial bin name (for chaining).
        """
        self._operation = operation
        async_op = operation._get_async_operation()
        self._async_builder = BinBuilder(async_op, bin_name)

    def bin(self, bin_name: str) -> SyncBinBuilder:
        """Start a bin operation chain."""
        self._async_builder.bin(bin_name)
        return self

    def set_to(self, value: Any) -> SyncBinBuilder:
        """Set a bin value (used after .bin(name))."""
        self._async_builder.set_to(value)
        return self

    def increment_by(self, value: int) -> SyncBinBuilder:
        """Increment a bin value (used after .bin(name))."""
        self._async_builder.increment_by(value)
        return self

    def and_remove_other_bins(self) -> SyncBinBuilder:
        """Mark that all bins not explicitly set should be removed."""
        self._async_builder.and_remove_other_bins()
        return self

    def execute(self) -> None:
        """Execute the accumulated bin operations synchronously."""
        self._operation._loop_manager.run_async(self._async_builder.execute())


class SyncKeyValueOperation:
    """
    Synchronous wrapper for KeyValueOperation.
    
    Provides the same fluent interface as KeyValueOperation but with
    synchronous methods that hide async/await.
    """

    def __init__(
        self,
        async_client: FluentClient,
        namespace: str,
        set_name: str,
        key: Union[str, int],
        loop_manager: _EventLoopManager,
        write_policy: Optional[WritePolicy] = None,
        behavior: Optional[Behavior] = None,
    ) -> None:
        """
        Initialize a SyncKeyValueOperation.

        Args:
            async_client: The async FluentClient instance.
            namespace: The namespace name.
            set_name: The set name.
            key: The record key (string or integer).
            loop_manager: The event loop manager for running async operations.
            write_policy: Optional write policy to use for this operation.
            behavior: Optional Behavior for deriving policies.
        """
        self._async_client = async_client
        self._namespace = namespace
        self._set_name = set_name
        self._key = key
        self._loop_manager = loop_manager
        self._behavior = behavior
        self._read_policy: Optional[ReadPolicy] = None
        self._write_policy: Optional[WritePolicy] = write_policy
        self._bins: Optional[List[str]] = None
        self._durable_delete: Optional[bool] = None

    def _get_async_operation(self) -> KeyValueOperation:
        """Get the underlying async operation builder."""
        op = KeyValueOperation(
            client=self._async_client._async_client,
            namespace=self._namespace,
            set_name=self._set_name,
            key=self._key,
            behavior=self._behavior,
        )
        if self._read_policy:
            op.with_read_policy(self._read_policy)
        if self._write_policy:
            op.with_write_policy(self._write_policy)
        if self._bins:
            op.bins(self._bins)
        if self._durable_delete is not None:
            op._durable_delete = self._durable_delete
        return op

    def with_read_policy(self, policy: ReadPolicy) -> SyncKeyValueOperation:
        """Set the read policy for this operation."""
        self._read_policy = policy
        return self

    def with_write_policy(self, policy: WritePolicy) -> SyncKeyValueOperation:
        """Set the write policy for this operation."""
        self._write_policy = policy
        return self

    def bins(self, bin_names: List[str]) -> SyncKeyValueOperation:
        """Specify which bins to retrieve for get operations."""
        self._bins = bin_names
        return self

    def bin(self, bin_name: str) -> SyncBinBuilder:
        """
        Start a bin operation chain (e.g., .bin("name").set_to("Tim")).
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            A SyncBinBuilder for chaining operations.
        
        Example:
            session.key_value(key="mykey").bin("name").set_to("Tim").bin("age").set_to(25).execute()
        """
        return SyncBinBuilder(self, bin_name)

    def get(self) -> Optional[Record]:
        """Get a record synchronously."""
        op = self._get_async_operation()
        return self._loop_manager.run_async(op.get())

    def put(self, bins: Dict[str, Any]) -> None:
        """Put (create or update) a record synchronously."""
        op = self._get_async_operation()
        self._loop_manager.run_async(op.put(bins))

    def set_bins(self, bins: Dict[str, Any]) -> SyncKeyValueOperation:
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

    def execute(self) -> None:
        """
        Execute a pending put operation (used with .set_bins()).
        
        Example:
            session.key_value(key="mykey").set_bins({"name": "Tim"}).execute()
        """
        if hasattr(self, '_pending_bins'):
            self.put(self._pending_bins)
            delattr(self, '_pending_bins')
        else:
            raise ValueError("No pending operation to execute. Use .set_bins() or .put() first.")

    def durably(self, durable: bool = True) -> SyncKeyValueOperation:
        """
        Set durable delete flag for delete operations.
        
        Args:
            durable: If True, the delete will be durable (default: True).
        
        Returns:
            self for method chaining.
        
        Example:
            session.delete(key="mykey").durably(False).delete()
        """
        self._durable_delete = durable
        return self

    def delete(self) -> bool:
        """Delete a record synchronously."""
        op = self._get_async_operation()
        return self._loop_manager.run_async(op.delete())

    def exists(self) -> bool:
        """Check if a record exists synchronously."""
        op = self._get_async_operation()
        return self._loop_manager.run_async(op.exists())

    def add(self, bins: Dict[str, int]) -> None:
        """Add (increment) integer values in bins synchronously."""
        op = self._get_async_operation()
        self._loop_manager.run_async(op.add(bins))

    def append(self, bins: Dict[str, str]) -> None:
        """Append strings to string bins synchronously."""
        op = self._get_async_operation()
        self._loop_manager.run_async(op.append(bins))

    def prepend(self, bins: Dict[str, str]) -> None:
        """Prepend strings to string bins synchronously."""
        op = self._get_async_operation()
        self._loop_manager.run_async(op.prepend(bins))

    def touch(self) -> None:
        """Touch (update TTL) a record synchronously."""
        op = self._get_async_operation()
        self._loop_manager.run_async(op.touch())

    def operate(
        self,
        operations: List[Union[Operation, ListOperation, MapOperation, BitOperation]],
    ) -> Optional[Record]:
        """
        Execute multiple operations atomically on a record synchronously.
        
        This method supports:
        - Basic operations: Operation.put(), Operation.get(), etc.
        - List operations: ListOperation.append(), ListOperation.get(), etc.
        - Map operations: MapOperation.put(), MapOperation.get_by_key(), etc.
        - Bit operations: BitOperation.set(), BitOperation.get(), etc.
        
        Args:
            operations: List of operations to execute atomically.
        
        Returns:
            The record with results of the operations, or None if the record
            doesn't exist and no read operations were performed.
        """
        op = self._get_async_operation()
        return self._loop_manager.run_async(op.operate(operations))