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

"""BatchExistsOperation - Builder for batch exists operations."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from aerospike_async import Client, Key

from aerospike_fluent.exceptions import convert_pac_exception
from aerospike_fluent.policy.behavior_settings import OpKind, OpShape
from aerospike_fluent.policy.policy_mapper import to_batch_policy
from aerospike_fluent.record_stream import RecordStream

if TYPE_CHECKING:
    from aerospike_fluent.policy.behavior import Behavior


class BatchExistsOperation:
    """Builder for batch exists operations.

    Uses ``batch_read`` under the hood so that each result carries a full
    ``result_code`` (enabling callers to distinguish "not found" from errors
    via :meth:`RecordResult.as_bool`).

    Example::

        stream = await session.exists(users.ids("user1", "user2")).execute()
        async for result in stream:
            print(f"{result.key}: exists={result.as_bool()}")
    """

    def __init__(
        self,
        client: Client,
        keys: List[Key],
        behavior: Optional[Behavior] = None,
    ) -> None:
        self._client = client
        self._keys = keys
        self._behavior = behavior
        self._respond_all_keys: bool = False

    def respond_all_keys(self) -> BatchExistsOperation:
        """Request that results be returned for all keys.

        Returns:
            self for method chaining.
        """
        self._respond_all_keys = True
        return self

    async def execute(self) -> RecordStream:
        """Execute the batch exists operation.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.
            Use :meth:`RecordResult.as_bool` for a simple existence check.
        """
        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.BATCH))
        try:
            results = await self._client.batch_read(
                batch_policy,
                None,
                self._keys,
                [],
            )
        except Exception as e:
            raise convert_pac_exception(e) from e
        return RecordStream.from_batch_records(results)
