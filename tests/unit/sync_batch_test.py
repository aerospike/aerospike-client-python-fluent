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

"""Unit tests for synchronous batch operation wrappers."""

import inspect
from unittest.mock import MagicMock

import pytest
from aerospike_async import Key

from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder,
    BatchKeyOperationBuilder,
    BatchOperationBuilder,
    BatchOpType,
)
from aerospike_sdk.record_stream import RecordStream
from aerospike_sdk.sync.operations.batch import (
    SyncBatchBinBuilder,
    SyncBatchKeyOperationBuilder,
    SyncBatchOperationBuilder,
)


@pytest.fixture
def loop_manager():
    fake_stream = MagicMock(spec=RecordStream)
    m = MagicMock()

    def _run_async(maybe_coro):
        # Sync wrappers pass coroutines from async builders; close them so
        # CPython does not emit "coroutine was never awaited" at GC time.
        if inspect.iscoroutine(maybe_coro):
            maybe_coro.close()
        return fake_stream

    m.run_async = MagicMock(side_effect=_run_async)
    return m


@pytest.fixture
def async_batch():
    return BatchOperationBuilder(client=MagicMock())


def test_sync_batch_operation_builder_wraps_verbs(async_batch, loop_manager):
    sync = SyncBatchOperationBuilder(async_batch, loop_manager)
    k = Key("test", "s", "k1")
    sk = sync.insert(k)
    assert isinstance(sk, SyncBatchKeyOperationBuilder)
    assert sk._inner is async_batch._key_operations[-1]


def test_sync_batch_key_chains_to_sync_wrappers(async_batch, loop_manager):
    sync = SyncBatchOperationBuilder(async_batch, loop_manager)
    k1 = Key("test", "s", "a")
    k2 = Key("test", "s", "b")
    chain = sync.insert(k1).bin("n").set_to(1).update(k2)
    assert isinstance(chain, SyncBatchKeyOperationBuilder)
    assert chain._inner._key == k2


def test_sync_batch_bin_delegates_and_wraps_parent(async_batch, loop_manager):
    k = Key("test", "s", "z")
    key_inner = BatchKeyOperationBuilder(async_batch, k, BatchOpType.INSERT)
    bin_inner = BatchBinBuilder(key_inner, "x")
    sb = SyncBatchBinBuilder(bin_inner, loop_manager)
    out = sb.set_to(99)
    assert isinstance(out, SyncBatchKeyOperationBuilder)
    assert out._inner._bins.get("x") == 99


def test_sync_batch_execute_uses_loop_manager(async_batch, loop_manager):
    k = Key("test", "s", "e")
    sync = SyncBatchOperationBuilder(async_batch, loop_manager)
    sync.insert(k).bin("v").set_to(1)
    stream = sync.execute()
    loop_manager.run_async.assert_called()
    assert stream._loop_manager is loop_manager


def test_sync_batch_key_execute_uses_loop_manager(async_batch, loop_manager):
    k = Key("test", "s", "e2")
    sync = SyncBatchOperationBuilder(async_batch, loop_manager)
    sk = sync.insert(k).bin("v").set_to(1)
    stream = sk.execute()
    loop_manager.run_async.assert_called()
    assert stream._loop_manager is loop_manager
