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

"""SyncBatchDeleteOperation - Synchronous wrapper for batch delete operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from aerospike_async import WritePolicy

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.operations.batch_delete import BatchDeleteOperation
from aerospike_fluent.sync.client import _EventLoopManager
from aerospike_fluent.sync.record_stream import SyncRecordStream

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class SyncBatchDeleteOperation:
    """
    Synchronous wrapper for BatchDeleteOperation.
    
    Provides the same fluent interface as BatchDeleteOperation but with
    synchronous methods that hide async/await.
    """

    def __init__(
        self,
        async_client: FluentClient,
        keys: List,
        loop_manager: _EventLoopManager,
        behavior: Optional[Behavior] = None,
    ) -> None:
        """
        Initialize a SyncBatchDeleteOperation.

        Args:
            async_client: The async FluentClient instance.
            keys: List of keys to delete.
            loop_manager: The event loop manager for running async operations.
            behavior: Optional Behavior for deriving policies.
        """
        self._async_client = async_client
        self._keys = keys
        self._loop_manager = loop_manager
        self._behavior = behavior
        self._write_policy: Optional[WritePolicy] = None

    def _get_async_operation(self) -> BatchDeleteOperation:
        """Get the underlying async operation builder."""
        op = BatchDeleteOperation(self._async_client._async_client, self._keys, self._behavior)
        if self._write_policy:
            op.with_write_policy(self._write_policy)
        return op

    def with_write_policy(self, policy: WritePolicy) -> SyncBatchDeleteOperation:
        """Set the write policy for this operation."""
        self._write_policy = policy
        return self

    def durably(self, durable: bool = True) -> SyncBatchDeleteOperation:
        """
        Set whether the delete operations should be durable.
        
        Args:
            durable: If True, the deletes will be durable. If False, the deletes
                    will not be durable (default: True).
        
        Returns:
            self for method chaining.
        """
        if self._write_policy is None:
            self._write_policy = WritePolicy()
        self._write_policy.durable_delete = durable
        return self

    def execute(self) -> SyncRecordStream:
        """Execute the batch delete operation synchronously.

        Returns:
            A :class:`SyncRecordStream` of per-key :class:`RecordResult` items.
        """
        op = self._get_async_operation()
        stream = self._loop_manager.run_async(op.execute())
        return SyncRecordStream(stream, self._loop_manager)


