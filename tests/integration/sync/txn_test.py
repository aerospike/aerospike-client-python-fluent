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

"""Sync MRT test subset: exercises :class:`SyncTransactionalSession`
against a live strong-consistency cluster.

This file is intentionally a focused subset of ``tests/integration/async/
txn_test.py`` — the async suite is the authoritative surface; this
suite verifies the sync wrapper correctly drives the underlying async
path for the critical MRT behaviors (commit, abort, conflict,
outside-txn block, batch).

All tests skip cleanly when the configured namespace is not strong-
consistency, matching the async suite's gate.

Provenance (per repo rules):
    reference: client/src/test/java/com/aerospike/client/sdk/TxnTest.java
"""

from __future__ import annotations

import os
import pytest

from aerospike_async import ResultCode
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync import SyncClient


BIN_NAME = "bin"
DEFAULT_SC_NAMESPACE = "test_sc"


@pytest.fixture(scope="module")
def sc_namespace() -> str:
    """Namespace name to run MRT tests against (env-tunable)."""
    return os.environ.get("AEROSPIKE_SC_NAMESPACE", DEFAULT_SC_NAMESPACE)


@pytest.fixture
def sync_client(aerospike_host, client_policy):
    """SyncClient sharing the conftest-built authed :class:`ClientPolicy`.

    Uses :class:`SyncClient` rather than :class:`ClusterDefinition` so the
    authenticated policy from ``conftest.py`` (driven by ``AEROSPIKE_*``
    env vars) is honored — required for the SC cluster.
    """
    client = SyncClient(seeds=aerospike_host, policy=client_policy)
    try:
        client.connect()
    except Exception as exc:
        pytest.skip(f"SC cluster unreachable at {aerospike_host!r}: {exc}")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def session(sync_client, sc_namespace):
    """Top-level (non-transactional) session; skips if the target namespace
    isn't strong-consistency — matches the async suite's ``assumeTrue`` gate.
    """
    sess = sync_client.create_session()
    try:
        is_sc = sess.is_namespace_sc(sc_namespace)
    except Exception as exc:
        pytest.skip(
            f"SC namespace {sc_namespace!r} unreachable "
            f"({exc}); set AEROSPIKE_HOST / AEROSPIKE_SC_NAMESPACE to an SC cluster"
        )
    if not is_sc:
        pytest.skip(
            f"Namespace {sc_namespace!r} is not strong-consistency; "
            "MRT tests require SC"
        )
    return sess


@pytest.fixture
def mrt_set(sc_namespace):
    """DataSet scoped to a dedicated set under the SC namespace."""
    return DataSet.of(sc_namespace, "mrt_sync")


def _fetch_bin(session, key):
    """Return the BIN_NAME value for ``key``, or ``None`` when missing."""
    stream = session.exists(key).execute()
    first = stream.first()
    if first is None or not first.as_bool():
        return None
    stream = session.query(key).execute()
    result = stream.first_or_raise().record_or_raise()
    return result.bins.get(BIN_NAME)


def _reset(session, key) -> None:
    """Delete ``key`` if present so each test starts clean."""
    try:
        session.delete(key).execute()
    except AerospikeError:
        pass


# ---------------------------------------------------------------------------
# 1. txnWrite: write inside a txn is visible after commit.
# ---------------------------------------------------------------------------
def test_txn_write(session, mrt_set):
    key = mrt_set.id("syncTxnWrite")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    def op(tx):
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    session.do_in_transaction(op)
    assert _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 2. txnAbortRollsBack: abort on context exit leaves the original value.
# ---------------------------------------------------------------------------
def test_txn_abort_rolls_back(session, mrt_set):
    key = mrt_set.id("syncTxnAbort")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    with session.begin_transaction() as tx:
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        tx.abort()

    assert _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 3. txnWriteConflict: nested txn writing the same key gets MRT_BLOCKED.
# ---------------------------------------------------------------------------
def test_txn_write_conflict(session, mrt_set):
    key = mrt_set.id("syncTxnConflict")
    _reset(session, key)

    def outer(tx1):
        tx1.upsert(key).put({BIN_NAME: "val1"}).execute()

        with session.begin_transaction() as tx2:
            with pytest.raises(AerospikeError) as excinfo:
                tx2.upsert(key).put({BIN_NAME: "val2"}).execute()
            assert excinfo.value.result_code == ResultCode.MRT_BLOCKED
            tx2.abort()

    session.do_in_transaction(outer)
    assert _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 4. txnWriteBlock: outside-txn write while a txn holds the key is blocked.
# ---------------------------------------------------------------------------
def test_txn_write_block(session, mrt_set):
    key = mrt_set.id("syncTxnBlock")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    def op(tx):
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()

        with pytest.raises(AerospikeError) as excinfo:
            session.upsert(key).put({BIN_NAME: "val3"}).execute()
        assert excinfo.value.result_code == ResultCode.MRT_BLOCKED

    session.do_in_transaction(op)
    assert _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 5. txnBatch: 10-key batch upsert under a txn commits all together.
# ---------------------------------------------------------------------------
def test_txn_batch(session, mrt_set):
    keys = [mrt_set.id(100 + i) for i in range(10)]
    for k in keys:
        _reset(session, k)
        session.upsert(k).put({BIN_NAME: 1}).execute()

    def op(tx):
        stream = tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        count = 0
        for result in stream:
            result.record_or_raise()
            count += 1
        assert count == len(keys)

    session.do_in_transaction(op)
    for k in keys:
        assert _fetch_bin(session, k) == 2


# ---------------------------------------------------------------------------
# 6. txnBatchAbort: 10-key batch upsert rolled back.
# ---------------------------------------------------------------------------
def test_txn_batch_abort(session, mrt_set):
    keys = [mrt_set.id(200 + i) for i in range(10)]
    for k in keys:
        _reset(session, k)
        session.upsert(k).put({BIN_NAME: 1}).execute()

    with session.begin_transaction() as tx:
        stream = tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        for result in stream:
            result.record_or_raise()
        tx.abort()

    for k in keys:
        assert _fetch_bin(session, k) == 1
