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

"""TransactionalSession - Session for transactional operations."""

from __future__ import annotations

from typing import Any, Optional

from aerospike_async import Client

from aerospike_fluent.aio.services.key_value_service import KeyValueService


class TransactionalSession:
    """
    Session for transactional operations.

    This session manages transaction state and ensures all operations
    within the session are part of the same transaction. When the session
    exits, the transaction is automatically committed (or rolled back on error).

    Note: Full transaction support depends on Aerospike server capabilities.
    This is a placeholder for future transaction support.

    Example:
        ```python
        async with client.transaction_session() as session:
            kv = session.key_value("test", "users")
            await kv.put("user1", {"name": "John"})
            await kv.put("user2", {"name": "Jane"})
            # Transaction auto-committed on exit
        ```
    """

    def __init__(self, client: Client) -> None:
        """
        Initialize a TransactionalSession.

        Args:
            client: The underlying async client.
        """
        self._client = client
        self._active = False

    def key_value(
        self,
        namespace: str,
        set_name: str,
    ) -> KeyValueService:
        """
        Create a key-value service within this transaction session.

        Args:
            namespace: The namespace name.
            set_name: The set name.

        Returns:
            A KeyValueService that operates within this transaction.
        """
        # For now, this is a regular KeyValueService
        # In the future, this will be a transactional KeyValueService
        # that tracks operations for commit/rollback
        return KeyValueService(
            client=self._client,
            namespace=namespace,
            set_name=set_name,
        )

    async def commit(self) -> None:
        """
        Commit the transaction.

        Note: Currently a no-op. Will be implemented when transaction
        support is available in the underlying client.
        """
        # TODO: Implement transaction commit when supported
        self._active = False

    async def rollback(self) -> None:
        """
        Rollback the transaction.

        Note: Currently a no-op. Will be implemented when transaction
        support is available in the underlying client.
        """
        # TODO: Implement transaction rollback when supported
        self._active = False

    async def __aenter__(self) -> TransactionalSession:
        """Async context manager entry."""
        self._active = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Async context manager exit."""
        if self._active:
            if exc_type is None:
                # No exception - commit transaction
                await self.commit()
            else:
                # Exception occurred - rollback transaction
                await self.rollback()

