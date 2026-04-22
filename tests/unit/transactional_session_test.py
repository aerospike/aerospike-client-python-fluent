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

"""Unit tests for TransactionalSession API shape and implicit txn threading.

These tests mock the underlying PAC client so they can verify the
PSDK-level API contract (txn lifecycle, commit/abort dispatch,
context-manager semantics, and implicit Session -> builder threading)
without requiring an SC cluster.
"""

import pytest

from aerospike_sdk import AbortStatus, CommitStatus, Txn, TransactionalSession
from aerospike_sdk.policy.behavior import Behavior


class _FakePacClient:
    """Minimal async stand-in for the PAC Client with commit/abort plus
    put/get/operate/etc. stubs used by higher-level SDK paths."""

    def __init__(self) -> None:
        self.commit_calls: list = []
        self.abort_calls: list = []
        self.commit_return: CommitStatus = CommitStatus.OK
        self.abort_return: AbortStatus = AbortStatus.OK

    async def commit(self, txn):
        self.commit_calls.append(txn)
        return self.commit_return

    async def abort(self, txn):
        self.abort_calls.append(txn)
        return self.abort_return


class _FakeSdkClient:
    """Stand-in for aerospike_sdk.aio.client.Client.

    TransactionalSession and Session only poke at ``_async_client``,
    ``_client``, and ``_indexes_monitor`` — enough to exercise txn
    threading without a live cluster.
    """

    def __init__(self) -> None:
        self._async_client = _FakePacClient()
        self._client = self._async_client
        self._indexes_monitor = None


@pytest.fixture
def sdk_client() -> _FakeSdkClient:
    return _FakeSdkClient()


@pytest.fixture
def tx_session(sdk_client: _FakeSdkClient) -> TransactionalSession:
    return TransactionalSession(client=sdk_client)  # type: ignore[arg-type]


async def test_txn_attribute_raises_before_enter(
    tx_session: TransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="not active"):
        _ = tx_session.txn
    assert tx_session.active is False


async def test_aenter_allocates_txn(
    tx_session: TransactionalSession,
) -> None:
    async with tx_session as tx:
        assert tx is tx_session
        assert isinstance(tx.txn, Txn)
        assert tx.active is True


async def test_clean_exit_commits(
    tx_session: TransactionalSession,
    sdk_client: _FakeSdkClient,
) -> None:
    async with tx_session as tx:
        txn_ref = tx.txn
    assert len(sdk_client._async_client.commit_calls) == 1
    assert sdk_client._async_client.commit_calls[0] is txn_ref
    assert len(sdk_client._async_client.abort_calls) == 0
    assert tx_session.active is False


async def test_exception_exit_aborts(
    tx_session: TransactionalSession,
    sdk_client: _FakeSdkClient,
) -> None:
    class _BoomError(RuntimeError):
        pass

    with pytest.raises(_BoomError):
        async with tx_session as tx:
            _ = tx.txn
            raise _BoomError("oops")
    assert len(sdk_client._async_client.abort_calls) == 1
    assert len(sdk_client._async_client.commit_calls) == 0
    assert tx_session.active is False


async def test_explicit_commit_returns_status(
    tx_session: TransactionalSession,
    sdk_client: _FakeSdkClient,
) -> None:
    async with tx_session as tx:
        status = await tx.commit()
        assert status == CommitStatus.OK
        assert tx.active is False
        # Subsequent __aexit__ should not double-commit:
    assert len(sdk_client._async_client.commit_calls) == 1


async def test_explicit_abort_returns_status(
    tx_session: TransactionalSession,
    sdk_client: _FakeSdkClient,
) -> None:
    async with tx_session as tx:
        status = await tx.abort()
        assert status == AbortStatus.OK
        assert tx.active is False
    assert len(sdk_client._async_client.abort_calls) == 1
    assert len(sdk_client._async_client.commit_calls) == 0


async def test_rollback_is_alias_for_abort(
    tx_session: TransactionalSession,
    sdk_client: _FakeSdkClient,
) -> None:
    async with tx_session as tx:
        status = await tx.rollback()
        assert status == AbortStatus.OK
    assert len(sdk_client._async_client.abort_calls) == 1


async def test_commit_without_active_txn_raises(
    tx_session: TransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="No active transaction"):
        await tx_session.commit()


async def test_abort_without_active_txn_raises(
    tx_session: TransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="No active transaction"):
        await tx_session.abort()


async def test_double_aenter_raises(
    tx_session: TransactionalSession,
) -> None:
    async with tx_session:
        with pytest.raises(RuntimeError, match="already active"):
            await tx_session.__aenter__()


# -- Subclass + hook behavior -------------------------------------------------

def test_transactional_session_subclasses_session() -> None:
    """TransactionalSession must be a proper Session subclass so session
    APIs (query, upsert, batch, ...) work inside the ``async with`` block.
    """
    from aerospike_sdk.aio.session import Session
    assert issubclass(TransactionalSession, Session)


def test_get_current_transaction_is_none_on_plain_session() -> None:
    from aerospike_sdk.aio.session import Session
    session = Session.__new__(Session)
    session._txn = None
    assert session.get_current_transaction() is None


async def test_get_current_transaction_yields_active_txn(
    tx_session: TransactionalSession,
) -> None:
    assert tx_session.get_current_transaction() is None
    async with tx_session as tx:
        assert tx.get_current_transaction() is tx.txn
    assert tx_session.get_current_transaction() is None


async def test_default_behavior_applied(
    sdk_client: _FakeSdkClient,
) -> None:
    tx = TransactionalSession(client=sdk_client)  # type: ignore[arg-type]
    assert tx.behavior is Behavior.DEFAULT


async def test_explicit_behavior_honored(
    sdk_client: _FakeSdkClient,
) -> None:
    custom = Behavior.DEFAULT.derive_with_changes(name="custom_mrt")
    tx = TransactionalSession(  # type: ignore[arg-type]
        client=sdk_client, behavior=custom,
    )
    assert tx.behavior is custom


# -- PAC Txn surface guards ---------------------------------------------------
# These lock in the PAC Txn shape that the MRT integration tests
# depend on. If PAC later adds ``set_state`` / ``set_timeout`` setters,
# these tests will start failing and tell us to lift the ``@pytest.skip``
# on the two currently-stubbed integration tests.

def test_pac_txn_has_expected_state_enum() -> None:
    """PAC must expose the four Txn state values (even if read-only)."""
    from aerospike_async import TxnState
    assert {
        TxnState.OPEN, TxnState.COMMITTED, TxnState.ABORTED, TxnState.VERIFIED,
    } == {getattr(TxnState, n) for n in ("OPEN", "COMMITTED", "ABORTED", "VERIFIED")}


def test_pac_txn_state_is_writable_and_round_trips() -> None:
    """PAC exposes ``Txn.state`` as a writable property (used by the
    ``test_txn_read_fails_for_all_states_except_open`` integration test).
    """
    from aerospike_async import Txn, TxnState
    t = Txn()
    assert t.state == TxnState.OPEN
    for target in (TxnState.COMMITTED, TxnState.ABORTED,
                   TxnState.VERIFIED, TxnState.OPEN):
        t.state = target
        assert t.state == target


def test_pac_txn_timeout_is_read_only() -> None:
    """Guard for the ``test_txn_mrt_expired_after_deadline`` skip.

    Setting ``Txn.timeout`` needs core's ``set_timeout`` to be callable
    through ``Arc<Txn>`` (which today requires ``&mut self``). When core
    changes the ``timeout`` field to atomic storage and PAC gains a
    ``timeout`` setter, this guard will fail — at which point the
    stubbed integration test should be filled in.
    """
    from aerospike_async import Txn
    t = Txn()
    with pytest.raises((AttributeError, TypeError)):
        t.timeout = 2  # type: ignore[misc]
