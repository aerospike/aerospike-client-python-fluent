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

"""Async and sync benchmark worker loops."""

from __future__ import annotations

import asyncio
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, List, Tuple, Union

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.session import Session
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.error_strategy import ErrorStrategy
from aerospike_sdk.exceptions import TimeoutError as AsTimeoutError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync.session import SyncSession

from ._env import client_policy_from_config
from .config import WorkloadConfig, WorkloadKind
from .record_spec import (
    BinField,
    first_integer_bin,
    full_bins,
    pick_bin_index,
    single_bin_put,
)
from .stats import StatsCollector


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, AsTimeoutError):
        return True
    return isinstance(exc, TimeoutError)


def _is_not_found(exc: BaseException) -> bool:
    """Detect a key-not-found result raised by the fast-path session.get.

    The builder path returns ``RecordResult(record=None, is_ok=True)`` for
    missing keys (no exception). Only :meth:`Session.get` and
    :meth:`Session.put` propagate the error, so this is the one place we
    need to translate it back into the bench's not-found counter.
    """
    rc = getattr(exc, "result_code", None)
    if rc is None:
        return False
    return rc == ResultCode.KEY_NOT_FOUND_ERROR


def _classify_exc(exc: BaseException) -> Tuple[bool, bool, bool]:
    """Return ``(is_timeout, is_error, is_not_found)``."""
    if _is_timeout(exc):
        return True, False, False
    if _is_not_found(exc):
        return False, False, True
    return False, True, False


def _make_keys(
    dataset: DataSet,
    key_count: int,
    rng: random.Random,
    batch_size: int,
) -> Union[Key, List[Key]]:
    if batch_size <= 1:
        return dataset.id(rng.randint(1, key_count))
    return [dataset.id(rng.randint(1, key_count)) for _ in range(batch_size)]


class _BenchState:
    __slots__ = ("insert_seq", "lock")

    def __init__(self) -> None:
        self.insert_seq = 0
        self.lock = threading.Lock()

    def next_insert_key(self) -> int:
        with self.lock:
            self.insert_seq += 1
            return self.insert_seq


async def _drain_async(stream: Any, batch: int, count_misses: bool = False) -> int:
    """Drain the stream; return count of not-found rows for reads.

    For single-key reads, the builder path signals not-found by raising
    ``StopAsyncIteration`` from :meth:`RecordStream.first_or_raise` when
    the stream is empty (no row was produced for a missing key). Catch
    that explicitly and report it as a miss instead of letting it bubble
    up as an error. For batch reads, missing keys appear in-stream as
    rows with a non-OK ``KEY_NOT_FOUND_ERROR`` result code.
    """
    if batch > 1:
        rows = await stream.collect()
        if not count_misses:
            return 0
        nf = 0
        for r in rows:
            if r.record is None:
                nf += 1
        return nf
    if not count_misses:
        await stream.first_or_raise()
        return 0
    try:
        await stream.first_or_raise()
    except StopAsyncIteration:
        return 1
    return 0


def _drain_sync(stream: Any, batch: int, count_misses: bool = False) -> int:
    if batch > 1:
        rows = stream.collect()
        if not count_misses:
            return 0
        nf = 0
        for r in rows:
            if r.record is None:
                nf += 1
        return nf
    if not count_misses:
        stream.first_or_raise()
        return 0
    try:
        stream.first_or_raise()
    except (StopIteration, StopAsyncIteration):
        return 1
    return 0


async def _one_op_async(
    session: Session,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    rng: random.Random,
    bench: _BenchState,
    decision: List[int],
) -> None:
    """Execute one workload op.

    ``decision`` is a 2-element scratch list reused across calls to avoid
    per-op allocation: ``decision[0]`` flags whether the op was a read,
    ``decision[1]`` is the number of not-found rows observed (always 0 for
    writes; possibly >0 for batch reads).
    """
    keys = _make_keys(dataset, cfg.key_count, rng, cfg.batch_size)
    bsz = max(1, cfg.batch_size)

    if cfg.workload == WorkloadKind.INSERT:
        decision[0] = 0
        kid = bench.next_insert_key()
        stream = await session.insert(dataset.id(kid)).put(full_bins(fields)).execute()
        await _drain_async(stream, bsz)
        return

    if cfg.workload == WorkloadKind.READ_UPDATE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = 1 if is_read else 0
        if is_read:
            if rng.randint(1, 100) <= cfg.read_all_bins_percent:
                if bsz > 1:
                    assert isinstance(keys, list)
                    stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
                    decision[1] = await _drain_async(stream, bsz, count_misses=True)
                elif cfg.fast_path:
                    assert isinstance(keys, Key)
                    await session.get(keys)
                else:
                    assert isinstance(keys, Key)
                    stream = await session.query(keys).execute()
                    decision[1] = await _drain_async(stream, bsz, count_misses=True)
            else:
                assert isinstance(keys, Key)
                bi = pick_bin_index(rng, len(fields))
                stream = await session.query(keys).bin(fields[bi].name).get().execute()
                decision[1] = await _drain_async(stream, 1, count_misses=True)
        else:
            if rng.randint(1, 100) <= cfg.write_all_bins_percent:
                bins = full_bins(fields)
            else:
                bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur: Any = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            elif cfg.fast_path:
                assert isinstance(keys, Key)
                await session.put(keys, bins)
            else:
                assert isinstance(keys, Key)
                stream = await session.upsert(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_REPLACE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            decision[1] = await _drain_async(stream, bsz, count_misses=True)
        else:
            bins = full_bins(fields)
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.replace_if_exists(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = await session.replace_if_exists(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_UPDATE:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            decision[1] = await _drain_async(stream, bsz, count_misses=True)
        else:
            bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = await session.upsert(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    int_bin = first_integer_bin(fields)
    if cfg.workload == WorkloadKind.READ_MODIFY_INCREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            decision[1] = await _drain_async(stream, bsz, count_misses=True)
        else:
            assert isinstance(keys, Key)
            stream = await session.upsert(keys).add(int_bin, 1).execute()
            await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_DECREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            decision[1] = await _drain_async(stream, bsz, count_misses=True)
        else:
            assert isinstance(keys, Key)
            stream = await session.upsert(keys).add(int_bin, -1).execute()
            await _drain_async(stream, 1)
        return

    raise NotImplementedError(cfg.workload)


def _one_op_sync(
    session: SyncSession,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    rng: random.Random,
    bench: _BenchState,
    decision: List[int],
) -> None:
    keys = _make_keys(dataset, cfg.key_count, rng, cfg.batch_size)
    bsz = max(1, cfg.batch_size)

    if cfg.workload == WorkloadKind.INSERT:
        decision[0] = 0
        kid = bench.next_insert_key()
        stream = session.insert(dataset.id(kid)).put(full_bins(fields)).execute()
        _drain_sync(stream, bsz)
        return

    if cfg.workload == WorkloadKind.READ_UPDATE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = 1 if is_read else 0
        if is_read:
            if rng.randint(1, 100) <= cfg.read_all_bins_percent:
                if bsz > 1:
                    assert isinstance(keys, list)
                    stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
                else:
                    assert isinstance(keys, Key)
                    stream = session.query(keys).execute()
                decision[1] = _drain_sync(stream, bsz, count_misses=True)
            else:
                assert isinstance(keys, Key)
                bi = pick_bin_index(rng, len(fields))
                stream = session.query(keys).bin(fields[bi].name).get().execute()
                decision[1] = _drain_sync(stream, 1, count_misses=True)
        else:
            if rng.randint(1, 100) <= cfg.write_all_bins_percent:
                bins = full_bins(fields)
            else:
                bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur: Any = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = session.upsert(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_REPLACE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            decision[1] = _drain_sync(stream, bsz, count_misses=True)
        else:
            bins = full_bins(fields)
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.replace_if_exists(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = session.replace_if_exists(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_UPDATE:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            decision[1] = _drain_sync(stream, bsz, count_misses=True)
        else:
            bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = session.upsert(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    int_bin = first_integer_bin(fields)
    if cfg.workload == WorkloadKind.READ_MODIFY_INCREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = 1 if is_read else 0
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            decision[1] = _drain_sync(stream, bsz, count_misses=True)
        else:
            assert isinstance(keys, Key)
            stream = session.upsert(keys).add(int_bin, 1).execute()
            _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_DECREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            _drain_sync(stream, bsz)
        else:
            assert isinstance(keys, Key)
            stream = session.upsert(keys).add(int_bin, -1).execute()
            _drain_sync(stream, 1)
        return

    raise NotImplementedError(cfg.workload)


async def run_async(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: asyncio.Event,
    connected: asyncio.Event | None = None,
) -> None:
    if cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        first_integer_bin(cfg.bin_fields)

    bench_state = _BenchState()
    policy = client_policy_from_config(cfg)
    async with Client(cfg.seeds, policy=policy) as client:
        # Signal that the connection succeeded so the caller can start the
        # ticker.  Without this, the ticker prints empty intervals while the
        # client is still trying to connect (or timing out).
        if connected is not None:
            connected.set()

        session = client.create_session(Behavior.DEFAULT)
        dataset = DataSet.of(cfg.namespace, cfg.set_name)
        fields = list(cfg.bin_fields)

        async def worker(worker_id: int) -> None:
            seed = (cfg.seed + worker_id + 1) % (2**32)
            rng = random.Random(seed)
            # decision[0] = is_read flag, decision[1] = not-found count.
            decision: List[int] = [0, 0]
            has_limit = cfg.max_ops is not None
            while not stop.is_set():
                if has_limit and stats.total_ops() >= cfg.max_ops:
                    return
                include_lat = stats.include_latency_sample()
                t0 = time.perf_counter()
                decision[0] = 0
                decision[1] = 0
                try:
                    await _one_op_async(
                        session, cfg, dataset, fields, rng, bench_state, decision,
                    )
                except BaseException as exc:
                    dt = (time.perf_counter() - t0) * 1000.0
                    to, er, nf = _classify_exc(exc)
                    stats.record(
                        is_read=bool(decision[0]),
                        latency_ms=dt,
                        is_timeout=to,
                        is_error=er,
                        not_found_count=1 if nf else 0,
                        include_in_summary_latency=include_lat and nf,
                    )
                    if not isinstance(exc, Exception):
                        raise
                else:
                    dt = (time.perf_counter() - t0) * 1000.0
                    stats.record(
                        is_read=bool(decision[0]),
                        latency_ms=dt,
                        is_timeout=False,
                        is_error=False,
                        not_found_count=decision[1],
                        include_in_summary_latency=include_lat,
                    )

        tasks = [asyncio.create_task(worker(i)) for i in range(cfg.async_tasks)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def run_sync(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: threading.Event,
    connected: threading.Event | None = None,
) -> None:
    if cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        first_integer_bin(cfg.bin_fields)

    bench_state = _BenchState()
    policy = client_policy_from_config(cfg)

    def thread_main(worker_id: int) -> None:
        seed = (cfg.seed + worker_id + 1) % (2**32)
        rng = random.Random(seed)
        with SyncClient(cfg.seeds, policy=policy) as client:
            # Signal that at least one worker connected successfully.
            if connected is not None:
                connected.set()
            session = client.create_session(Behavior.DEFAULT)
            dataset = DataSet.of(cfg.namespace, cfg.set_name)
            fields = list(cfg.bin_fields)
            decision: List[int] = [0, 0]
            has_limit = cfg.max_ops is not None
            while not stop.is_set():
                if has_limit and stats.total_ops() >= cfg.max_ops:
                    return
                include_lat = stats.include_latency_sample()
                t0 = time.perf_counter()
                decision[0] = 0
                decision[1] = 0
                try:
                    _one_op_sync(
                        session, cfg, dataset, fields, rng, bench_state, decision,
                    )
                except BaseException as exc:
                    dt = (time.perf_counter() - t0) * 1000.0
                    to, er, nf = _classify_exc(exc)
                    stats.record(
                        is_read=bool(decision[0]),
                        latency_ms=dt,
                        is_timeout=to,
                        is_error=er,
                        not_found_count=1 if nf else 0,
                        include_in_summary_latency=include_lat and nf,
                    )
                    if not isinstance(exc, Exception):
                        raise
                else:
                    dt = (time.perf_counter() - t0) * 1000.0
                    stats.record(
                        is_read=bool(decision[0]),
                        latency_ms=dt,
                        is_timeout=False,
                        is_error=False,
                        not_found_count=decision[1],
                        include_in_summary_latency=include_lat,
                    )

    with ThreadPoolExecutor(max_workers=cfg.threads) as pool:
        futures = [pool.submit(thread_main, i) for i in range(cfg.threads)]
        while not stop.is_set():
            time.sleep(0.05)
        wait(futures)
        for f in futures:
            f.result()


