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

"""MRT test suite: covers PSDK's multi-record transaction surface
against a fixed catalog of scenarios so behavior doesn't silently
regress.

Every test here needs a strong-consistency namespace. Set
``AEROSPIKE_SC_NAMESPACE`` (default: ``test_sc``) to the namespace
configured with ``strong-consistency true`` on your cluster. When no SC
namespace is reachable each test skips with a clear reason.

Provenance (per repo rules):
    reference: client/src/test/java/com/aerospike/client/sdk/TxnTest.java
"""

from __future__ import annotations

import os
import pytest

from aerospike_async import ResultCode
from aerospike_sdk import Client, DataSet
from aerospike_sdk.exceptions import AerospikeError


BIN_NAME = "bin"
DEFAULT_SC_NAMESPACE = "test_sc"


@pytest.fixture(scope="module")
def sc_namespace() -> str:
    """Namespace name to run MRT tests against (env-tunable)."""
    return os.environ.get("AEROSPIKE_SC_NAMESPACE", DEFAULT_SC_NAMESPACE)


@pytest.fixture
async def client(aerospike_host, client_policy):
    """SDK Client opened against the default cluster for the session fixture."""
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest.fixture
async def session(client, sc_namespace):
    """Top-level (non-transactional) session used outside ``doInTransaction``.

    Skips the test if the configured SC namespace isn't available on the
    cluster.
    """
    sess = client.create_session()
    try:
        is_sc = await sess.is_namespace_sc(sc_namespace)
    except Exception as exc:
        pytest.skip(
            f"SC namespace {sc_namespace!r} unreachable "
            f"({exc}); set AEROSPIKE_SC_NAMESPACE or stand up Phase 3e.3"
        )
    if not is_sc:
        pytest.skip(
            f"Namespace {sc_namespace!r} is not strong-consistency; "
            "MRT tests require SC (see Phase 3e.3 in the cutover plan)"
        )
    return sess


@pytest.fixture
def mrt_set(sc_namespace):
    """DataSet scoped to a dedicated set under the SC namespace."""
    return DataSet.of(sc_namespace, "mrt_async")


async def _fetch_bin(session, key) -> object | None:
    """Return the BIN_NAME value for ``key``, or ``None`` when missing."""
    stream = await session.exists(key).execute()
    first = await stream.first()
    if first is None or not first.as_bool():
        return None
    stream = await session.query(key).execute()
    result = (await stream.first_or_raise()).record_or_raise()
    return result.bins.get(BIN_NAME)


async def _reset(session, key) -> None:
    """Delete ``key`` if present so each test starts clean."""
    try:
        await session.delete(key).execute()
    except AerospikeError:
        pass


# ---------------------------------------------------------------------------
# 1. txnWrite: write inside a txn is visible after commit.
# ---------------------------------------------------------------------------
async def test_txn_write(session, mrt_set):
    key = mrt_set.id("txnWrite")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 2. txnWriteTwice: last write inside a txn wins.
# ---------------------------------------------------------------------------
async def test_txn_write_twice(session, mrt_set):
    key = mrt_set.id("txnWriteTwice")
    await _reset(session, key)

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val1"}).execute()
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 3. txnWriteConflict: another txn trying to write the same key while a
#    txn holds it gets MRT_BLOCKED. We use begin_transaction (rather than
#    do_in_transaction) for the inner txn so its retry loop doesn't mask
#    the MRT_BLOCKED we're asserting on.
# ---------------------------------------------------------------------------
async def test_txn_write_conflict(session, mrt_set):
    key = mrt_set.id("txnWriteConflict")
    await _reset(session, key)

    async def outer(tx1):
        await tx1.upsert(key).put({BIN_NAME: "val1"}).execute()

        async with session.begin_transaction() as tx2:
            with pytest.raises(AerospikeError) as excinfo:
                await tx2.upsert(key).put({BIN_NAME: "val2"}).execute()
            assert excinfo.value.result_code == ResultCode.MRT_BLOCKED
            # Let the inner txn abort cleanly on context exit.
            await tx2.abort()

    await session.do_in_transaction(outer)
    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 4. txnReadFailsForAllStatesExceptOpen: issuing any command against a non-
#    OPEN txn must raise client-side (reference test does the same via
#    ``txn.setState(...)`` — PAC now exposes an equivalent setter).
# ---------------------------------------------------------------------------
async def test_txn_read_fails_for_all_states_except_open(session, client, mrt_set):
    # ``session`` dep triggers the shared SC-namespace skip.
    del session
    from aerospike_async import Txn, TxnState

    key = mrt_set.id("txnReadFailsForAllStatesExceptOpen")

    # OPEN must not raise on a fresh txn — the query may return zero rows
    # for a missing key, and that's fine; we only care that no forbidden-
    # state error fires.
    for state, should_raise in (
        (TxnState.OPEN, False),
        (TxnState.COMMITTED, True),
        (TxnState.ABORTED, True),
        (TxnState.VERIFIED, True),
    ):
        tx_session = client.transaction_session()
        # Allocate a txn without going through __aenter__, then force
        # the state to exercise the non-OPEN state-machine guard.
        tx_session._txn = Txn()
        tx_session._finalized = False
        tx_session._txn.state = state

        try:
            stream = await tx_session.query(key).execute()
            async for _ in stream:
                break
            if should_raise:
                pytest.fail(f"Expected AerospikeError for state {state}")
        except AerospikeError as exc:
            if not should_raise:
                pytest.fail(f"Unexpected error for state {state}: {exc}")
            msg = str(exc).lower()
            # Core reports a single generic "forbidden" message for all
            # non-OPEN states; tolerate that and any future state-specific
            # variants the reference uses.
            assert (
                "forbidden" in msg
                or "commit" in msg
                or "abort" in msg
                or "committed" in msg
                or "aborted" in msg
            ), f"Unexpected error text for state {state}: {exc}"
        finally:
            # Short-circuit finalization to avoid trying to commit/abort
            # a faked-state txn at teardown.
            tx_session._finalized = True
            tx_session._txn = None


# ---------------------------------------------------------------------------
# 5. txnWriteBlock: outside-txn write is blocked while a txn holds the key.
# ---------------------------------------------------------------------------
async def test_txn_write_block(session, mrt_set):
    key = mrt_set.id("txnWriteBlock")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

        with pytest.raises(AerospikeError) as excinfo:
            await session.upsert(key).put({BIN_NAME: "val3"}).execute()
        assert excinfo.value.result_code == ResultCode.MRT_BLOCKED

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 6. txnWriteRead: outside-txn read sees pre-txn value until commit.
# ---------------------------------------------------------------------------
async def test_txn_write_read(session, mrt_set):
    key = mrt_set.id("txnWriteRead")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        # Non-transactional reader must still see val1 pre-commit.
        assert await _fetch_bin(session, key) == "val1"

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 7. txnWriteAbort: explicit abort rolls back writes.
# ---------------------------------------------------------------------------
async def test_txn_write_abort(session, mrt_set):
    key = mrt_set.id("txnWriteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        # Read-your-own-writes inside the txn:
        assert await _fetch_bin(tx, key) == "val2"
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 8. txnDelete: delete inside a txn commits.
# ---------------------------------------------------------------------------
async def test_txn_delete(session, mrt_set):
    key = mrt_set.id("txnDelete")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.delete(key).durably_delete().execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) is None


# ---------------------------------------------------------------------------
# 9. txnDeleteAbort: delete rolled back restores the record.
# ---------------------------------------------------------------------------
async def test_txn_delete_abort(session, mrt_set):
    key = mrt_set.id("txnDeleteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.delete(key).durably_delete().execute()
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 10. txnDeleteTwice: second delete of the same key within one txn is a
#     no-op (idempotent) rather than raising.
# ---------------------------------------------------------------------------
async def test_txn_delete_twice(session, mrt_set):
    key = mrt_set.id("txnDeleteTwice")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.delete(key).durably_delete().execute()
        # The second delete must not blow up: we only assert it doesn't
        # raise an unexpected error — a KEY_NOT_FOUND_ERROR is acceptable.
        try:
            await tx.delete(key).durably_delete().execute()
        except AerospikeError as exc:
            assert exc.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) is None


# ---------------------------------------------------------------------------
# 11. txnTouch: touch under a txn keeps the record and commits.
# ---------------------------------------------------------------------------
async def test_txn_touch(session, mrt_set):
    key = mrt_set.id("txnTouch")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.touch(key).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 12. txnTouchAbort: touch rolled back leaves record intact.
# ---------------------------------------------------------------------------
async def test_txn_touch_abort(session, mrt_set):
    key = mrt_set.id("txnTouchAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.touch(key).execute()
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 13. txnOperateWrite: combined set + read in one operate under txn.
# ---------------------------------------------------------------------------
async def test_txn_operate_write(session, mrt_set):
    key = mrt_set.id("txnOperateWrite")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1", "bin2": "bal1"}).execute()

    async def op(tx):
        stream = await (
            tx.upsert(key).set_to(BIN_NAME, "val2").get("bin2").execute()
        )
        rec = (await stream.first_or_raise()).record_or_raise()
        assert rec.bins.get("bin2") == "bal1"

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 14. txnOperateWriteAbort: combined op rolled back.
# ---------------------------------------------------------------------------
async def test_txn_operate_write_abort(session, mrt_set):
    key = mrt_set.id("txnOperateWriteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1", "bin2": "bal1"}).execute()

    async with session.begin_transaction() as tx:
        stream = await (
            tx.upsert(key).set_to(BIN_NAME, "val2").get("bin2").execute()
        )
        rec = (await stream.first_or_raise()).record_or_raise()
        assert rec.bins.get("bin2") == "bal1"
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 15. txnBatch: 10-key batch upsert under a txn commits all together.
# ---------------------------------------------------------------------------
async def test_txn_batch(session, mrt_set):
    keys = [mrt_set.id(i) for i in range(0, 10)]
    for k in keys:
        await _reset(session, k)
        await session.upsert(k).put({BIN_NAME: 1}).execute()

    async def op(tx):
        stream = await tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        count = 0
        async for result in stream:
            # Only ensure the per-key op succeeded: writes don't return
            # the new bin value. The final check below queries
            # post-commit to confirm values landed.
            result.record_or_raise()
            count += 1
        assert count == len(keys)

    await session.do_in_transaction(op)
    for k in keys:
        assert await _fetch_bin(session, k) == 2


# ---------------------------------------------------------------------------
# 16. txnBatchAbort: 10-key batch upsert rolled back.
# ---------------------------------------------------------------------------
async def test_txn_batch_abort(session, mrt_set):
    keys = [mrt_set.id(i) for i in range(10, 20)]
    for k in keys:
        await _reset(session, k)
        await session.upsert(k).put({BIN_NAME: 1}).execute()

    async with session.begin_transaction() as tx:
        stream = await tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        async for result in stream:
            result.record_or_raise()
        await tx.abort()

    for k in keys:
        assert await _fetch_bin(session, k) == 1


# ---------------------------------------------------------------------------
# 17. txnMrtExpiredAfterDeadline: requires Txn.set_timeout, which the PAC
#     does not expose as a setter today (timeout is a read-only property).
#     Tracked as a PAC gap; wire up when the setter lands.
# ---------------------------------------------------------------------------
@pytest.mark.skip(
    reason="Requires PAC to expose Txn.set_timeout (timeout is read-only "
    "today). Tracked as a Phase 3 follow-up in "
    "core_v3_cutover_08bef130.plan.md."
)
async def test_txn_mrt_expired_after_deadline() -> None:
    pass
