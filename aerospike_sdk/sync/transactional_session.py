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

"""Synchronous multi-record transaction (MRT) session wrapper."""

from __future__ import annotations

import types
from typing import Optional, TYPE_CHECKING

from aerospike_async import AbortStatus, CommitStatus, Txn

from aerospike_sdk.aio.transactional_session import TransactionalSession as AsyncTransactionalSession
from aerospike_sdk.sync.client import _EventLoopManager
from aerospike_sdk.sync.session import SyncSession

if TYPE_CHECKING:
    from aerospike_sdk.policy.behavior import Behavior


class SyncTransactionalSession(SyncSession):
    """Sync context manager that groups operations into a multi-record transaction.

    Subclasses :class:`~aerospike_sdk.sync.session.SyncSession`, so every
    session API (``query``, ``upsert``, ``insert``, ``batch``, ...) works
    unchanged inside ``with``; the underlying async
    :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
    threads the active :class:`~aerospike_async.Txn` onto every policy the
    builders hand to the PAC — the user never touches a policy.

    On clean exit the transaction is committed; if an exception propagates
    out of the block the transaction is aborted. Explicit :meth:`commit`,
    :meth:`abort`, and :meth:`rollback` (alias for ``abort``) are also
    available for manual control.

    Example:
        >>> with client.create_session().begin_transaction() as tx:
        ...     tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
        ...     tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()
        # Auto-committed on clean exit; auto-aborted on exception.

    See Also:
        :meth:`aerospike_sdk.sync.session.SyncSession.begin_transaction`
        :meth:`aerospike_sdk.sync.cluster.Cluster.create_transactional_session`
        :class:`aerospike_sdk.aio.transactional_session.TransactionalSession`
    """

    def __init__(
        self,
        async_txn_session: AsyncTransactionalSession,
        loop_manager: _EventLoopManager,
    ) -> None:
        """Wrap ``async_txn_session``; use :meth:`SyncSession.begin_transaction` instead.

        Args:
            async_txn_session: Underlying async
                :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
                sharing the same client and behavior.
            loop_manager: Loop manager shared with the parent
                :class:`~aerospike_sdk.sync.client.SyncClient`.

        Note:
            Application code should not construct ``SyncTransactionalSession``
            directly; call :meth:`SyncSession.begin_transaction` or
            :meth:`Cluster.create_transactional_session` instead.

        See Also:
            :meth:`aerospike_sdk.sync.session.SyncSession.begin_transaction`
        """
        super().__init__(async_txn_session, loop_manager)

    @property
    def _async_txn_session(self) -> AsyncTransactionalSession:
        # The underlying async session is always the transactional subtype
        # once this class wraps it.
        return self._async_session  # type: ignore[return-value]

    @property
    def txn(self) -> Txn:
        """Return the underlying :class:`~aerospike_async.Txn`.

        Raises:
            RuntimeError: If the session has not been entered (no active txn).

        Returns:
            The active :class:`~aerospike_async.Txn`.
        """
        return self._async_txn_session.txn

    @property
    def active(self) -> bool:
        """``True`` when a transaction has been started and not yet finalized.

        Returns:
            Whether a transaction is currently active on this session.
        """
        return self._async_txn_session.active

    def commit(self) -> CommitStatus:
        """Commit the transaction and return the server-reported status.

        Raises:
            RuntimeError: If the session has no active transaction.

        Returns:
            :class:`~aerospike_async.CommitStatus` reported by the server.

        See Also:
            :meth:`abort`: Undo the transaction instead of committing.
        """
        return self._loop_manager.run_async(self._async_txn_session.commit())

    def abort(self) -> AbortStatus:
        """Abort the transaction and return the server-reported status.

        Raises:
            RuntimeError: If the session has no active transaction.

        Returns:
            :class:`~aerospike_async.AbortStatus` reported by the server.

        See Also:
            :meth:`commit`: Persist the transaction instead of aborting.
            :meth:`rollback`: Alias for this method.
        """
        return self._loop_manager.run_async(self._async_txn_session.abort())

    def rollback(self) -> AbortStatus:
        """Alias for :meth:`abort`.

        Returns:
            :class:`~aerospike_async.AbortStatus` reported by the server.
        """
        return self.abort()

    def __enter__(self) -> "SyncTransactionalSession":
        self._loop_manager.run_async(self._async_txn_session.__aenter__())
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        self._loop_manager.run_async(
            self._async_txn_session.__aexit__(exc_type, exc_val, exc_tb)
        )
