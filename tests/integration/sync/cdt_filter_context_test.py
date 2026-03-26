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

"""Sync integration tests for query Filter with CDT context (nested map path).

See async :mod:`tests.integration.async.cdt_filter_context_test` for notes on
``Filter.context`` and PAC ``create_index`` with ``ctx``.
"""

import time

import pytest
from aerospike_async import CTX, Filter, IndexType

from aerospike_fluent import DataSet, SyncFluentClient

_NS = "test"
_SET = "cdt_filter_ctx_test"
_INDEX = "pfc_cdt_fctx_map_num"
_BIN = "mapbin"
_OUTER = "outer"
_INNER = "inner"


def _require_filter_context() -> None:
    probe = Filter.equal("__bin", 1)
    if not hasattr(probe, "context"):
        pytest.skip(
            "aerospike_async Filter.context is required; upgrade the native async client."
        )


@pytest.fixture
def client(aerospike_host, client_policy):
    with SyncFluentClient(seeds=aerospike_host, policy=client_policy) as client:
        yield client


def _cleanup_records(session, keys):
    for k in keys:
        try:
            session.delete(k).execute()
        except Exception:
            pass


def _user_keys_from_stream(stream):
    keys = []
    for res in stream:
        if res.is_ok and res.record is not None:
            keys.append(res.record.key.value)
    return keys


def _run_async(client: SyncFluentClient, coro):
    return client._loop_manager.run_async(coro)


async def _admin_drop(pac, ns: str, st: str, name: str) -> None:
    await pac.drop_index(ns, st, name)


async def _admin_create_nested(pac) -> None:
    await pac.create_index(
        _NS,
        _SET,
        _BIN,
        _INDEX,
        IndexType.NUMERIC,
        None,
        ctx=[CTX.map_key(_OUTER), CTX.map_key(_INNER)],
    )


async def _admin_create_flat(pac, index_name: str) -> None:
    await pac.create_index(
        _NS,
        _SET,
        _BIN,
        index_name,
        IndexType.NUMERIC,
        None,
        ctx=[CTX.map_key(_INNER)],
    )


def test_query_filter_equal_with_map_nested_context(client):
    """Sync query with ``Filter.equal(...).context([...])`` on a nested map value."""
    _require_filter_context()

    ds = DataSet.of(_NS, _SET)
    key_hi = ds.id("cdt_ctx_hi")
    key_lo = ds.id("cdt_ctx_lo")
    key_missing_inner = ds.id("cdt_ctx_no_inner")
    keys = (key_hi, key_lo, key_missing_inner)

    session = client.create_session()
    ac = client._ensure_connected()
    pac = ac.underlying_client

    _cleanup_records(session, keys)
    try:
        _run_async(client, _admin_drop(pac, _NS, _SET, _INDEX))
    except Exception:
        pass

    try:
        _run_async(client, _admin_create_nested(pac))
    except Exception as e:
        pytest.skip(f"Could not create nested-map secondary index: {e}")

    time.sleep(0.75)

    target = 4242
    other_inner = 7

    try:
        (
            session.upsert(key_hi)
            .put(
                {
                    _BIN: {
                        _OUTER: {_INNER: target, "noise": other_inner},
                    },
                }
            )
            .execute()
        )
        (
            session.upsert(key_lo)
            .put(
                {
                    _BIN: {
                        _OUTER: {_INNER: 9999, "noise": 1},
                    },
                }
            )
            .execute()
        )
        (
            session.upsert(key_missing_inner)
            .put({_BIN: {_OUTER: {"noise": 3}}})
            .execute()
        )

        flt = Filter.equal(_BIN, target).context(
            [CTX.map_key(_OUTER), CTX.map_key(_INNER)]
        )
        stream = client.query(_NS, _SET).filter(flt).bins([_BIN]).execute()
        try:
            user_keys = sorted(_user_keys_from_stream(stream))
        finally:
            stream.close()

        assert user_keys == ["cdt_ctx_hi"]

        flt2 = Filter.equal(_BIN, 9999).context(
            [CTX.map_key(_OUTER), CTX.map_key(_INNER)]
        )
        stream2 = client.query(_NS, _SET).filter(flt2).bins([_BIN]).execute()
        try:
            assert sorted(_user_keys_from_stream(stream2)) == ["cdt_ctx_lo"]
        finally:
            stream2.close()
    finally:
        try:
            _run_async(client, _admin_drop(pac, _NS, _SET, _INDEX))
        except Exception:
            pass
        _cleanup_records(session, keys)


def test_query_filter_equal_single_map_key_context(client):
    """``Filter.equal(bin, value).context([CTX.map_key(...)])`` on a scalar under one map key."""
    _require_filter_context()

    ds = DataSet.of(_NS, _SET)
    key_match = ds.id("cdt_ctx_flat_a")
    key_other = ds.id("cdt_ctx_flat_b")
    keys = (key_match, key_other)

    session = client.create_session()
    ac = client._ensure_connected()
    pac = ac.underlying_client
    index_name = f"{_INDEX}_flat"

    _cleanup_records(session, keys)
    try:
        _run_async(client, _admin_drop(pac, _NS, _SET, index_name))
    except Exception:
        pass

    try:
        _run_async(client, _admin_create_flat(pac, index_name))
    except Exception as e:
        pytest.skip(f"Could not create CDT-path numeric index: {e}")

    time.sleep(0.75)
    val = 5150

    try:
        (
            session.upsert(key_match)
            .put({_BIN: {_INNER: val, "other": 1}})
            .execute()
        )
        (
            session.upsert(key_other)
            .put({_BIN: {_INNER: val + 1, "other": 2}})
            .execute()
        )

        flt = Filter.equal(_BIN, val).context([CTX.map_key(_INNER)])
        stream = client.query(_NS, _SET).filter(flt).bins([_BIN]).execute()
        try:
            assert sorted(_user_keys_from_stream(stream)) == ["cdt_ctx_flat_a"]
        finally:
            stream.close()
    finally:
        try:
            _run_async(client, _admin_drop(pac, _NS, _SET, index_name))
        except Exception:
            pass
        _cleanup_records(session, keys)
