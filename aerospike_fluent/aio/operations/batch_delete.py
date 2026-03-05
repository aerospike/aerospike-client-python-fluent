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

"""BatchDeleteOperation - Builder for batch delete operations."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from aerospike_async import Client, Key, WritePolicy

from aerospike_async.exceptions import ResultCode

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape
from aerospike_fluent.policy.policy_mapper import to_batch_policy
from aerospike_fluent.record_result import batch_records_to_results
from aerospike_fluent.record_stream import RecordStream

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class BatchDeleteOperation:
    """
    Builder for batch delete operations.
    
    This class handles batch deletes for multiple keys using the
    underlying async client's batch_delete for optimal performance.
    
    Example:
        ```python
        # Delete multiple keys
        results = await session.delete(users.ids("user1", "user2", "user3")).execute()
        
        # Check results
        for i, result in enumerate(results):
            print(f"Key {i}: deleted={result.result_code == 0}")
        ```
    """

    def __init__(
        self,
        client: Client,
        keys: List[Key],
        behavior: Optional[Behavior] = None,
    ) -> None:
        """
        Initialize a BatchDeleteOperation.

        Args:
            client: The underlying async client.
            keys: List of keys to delete.
            behavior: Optional Behavior for deriving policies.
        """
        self._client = client
        self._keys = keys
        self._behavior = behavior
        self._write_policy: Optional[WritePolicy] = None
        self._respond_all_keys: bool = False

    def with_write_policy(self, policy: WritePolicy) -> BatchDeleteOperation:
        """
        Set the write policy for this operation.
        
        Args:
            policy: The write policy to use.
        
        Returns:
            self for method chaining.
        """
        self._write_policy = policy
        return self

    def durably(self, durable: bool = True) -> BatchDeleteOperation:
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

    def respond_all_keys(self) -> BatchDeleteOperation:
        """
        Request that results be returned for all keys, not just failures.
        
        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    async def execute(self) -> RecordStream:
        """Execute the batch delete operation.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.
        """
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH))
        try:
            batch_records = await self._client.batch_delete(
                batch_policy,
                None,
                self._keys,
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
        results = batch_records_to_results(list(batch_records))
        if not self._respond_all_keys:
            results = [
                r for r in results
                if r.result_code not in (
                    ResultCode.KEY_NOT_FOUND_ERROR,
                    ResultCode.FILTERED_OUT,
                )
            ]
        return RecordStream.from_list(results)

