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

"""Throughput counters, latency histograms, and interval summaries."""

from __future__ import annotations

import array
import math
import resource
import threading
import tracemalloc
from dataclasses import dataclass, field
from typing import List

try:
    import psutil

    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def latency_threshold_ms(column_index: int, shift: int) -> float:
    """Exclusive lower bound in ms for the ``>`` column at *column_index* (>= 1)."""
    if column_index <= 0:
        return 0.0
    if column_index == 1:
        return 1.0
    return float((2**shift) ** (column_index - 1))


def latency_column_labels(columns: int, shift: int) -> List[str]:
    """Human-readable column headers matching ``performance.txt``."""
    labels: List[str] = ["<=1ms", ">1ms"]
    for i in range(2, columns):
        ms = latency_threshold_ms(i, shift)
        if ms == int(ms):
            labels.append(f">{int(ms)}ms")
        else:
            labels.append(f">{ms:g}ms")
    return labels


def _format_latency_us(us: float) -> str:
    """Format a latency value with human-readable units (us or ms)."""
    if us < 1000.0:
        return f"{int(us)}us"
    return f"{us / 1000.0:.1f}ms"


# ---------------------------------------------------------------------------
# Per-operation-type YCSB tracker (used inside StatsCollector)
# ---------------------------------------------------------------------------

class _YcsbTracker:
    """Per-op-type latency tracker mirroring Java ``LatencyManagerYcsb``.

    Uses 100us-granularity buckets (0-99.9ms in 1000 slots) for better
    sub-millisecond percentile resolution than the Java 1ms histogram,
    plus windowed and cumulative counters for avg / min / max / percentile
    reporting.
    """

    _BUCKETS = 1000
    _US_PER_BUCKET = 100

    __slots__ = (
        "_hist", "_overflow", "_ops", "_total_us",
        "_win_ops", "_win_total_us", "_min_us", "_max_us",
    )

    def __init__(self) -> None:
        self._hist = [0] * self._BUCKETS
        self._overflow = 0
        self._ops = 0
        self._total_us = 0
        self._win_ops = 0
        self._win_total_us = 0
        self._min_us = -1
        self._max_us = -1

    def add(self, latency_us: int) -> None:
        bucket = latency_us // self._US_PER_BUCKET
        if bucket >= self._BUCKETS:
            self._overflow += 1
        else:
            self._hist[bucket] += 1
        self._ops += 1
        self._total_us += latency_us
        self._win_ops += 1
        self._win_total_us += latency_us
        if self._min_us < 0 or latency_us < self._min_us:
            self._min_us = latency_us
        if latency_us > self._max_us:
            self._max_us = latency_us

    def _percentile_us(self, p: float) -> float:
        """Return the *p*-th percentile latency in microseconds."""
        if self._ops == 0:
            return 0.0
        cum = 0
        for i, count in enumerate(self._hist):
            cum += count
            if cum / self._ops >= p:
                return float(i * self._US_PER_BUCKET)
        return float(self._BUCKETS * self._US_PER_BUCKET)

    def format_period_total(self, name: str) -> str:
        win_avg = (
            self._win_total_us / self._win_ops if self._win_ops else 0
        )
        tot_avg = self._total_us / self._ops if self._ops else 0

        p95 = _format_latency_us(self._percentile_us(0.95))
        p99 = _format_latency_us(self._percentile_us(0.99))

        return (
            f"{name}: Period[Ops:{self._win_ops}"
            f" Avg Latency:{_format_latency_us(win_avg)}]"
            f" Total[Ops:{self._ops}"
            f" Latency:(avg:{_format_latency_us(tot_avg)}"
            f" Min:{_format_latency_us(max(0, self._min_us))}"
            f" Max:{_format_latency_us(max(0, self._max_us))})"
            f" 95th%:{p95} 99th%:{p99}]"
        )

    def reset_window(self) -> None:
        self._win_ops = 0
        self._win_total_us = 0


@dataclass
class IntervalSnapshot:
    """Aggregates for one reporting second."""

    index: int
    reads: int = 0
    writes: int = 0
    read_timeouts: int = 0
    write_timeouts: int = 0
    read_errors: int = 0
    write_errors: int = 0
    read_le1: int = 0
    write_le1: int = 0
    read_gt: List[int] = field(default_factory=list)
    write_gt: List[int] = field(default_factory=list)


class StatsCollector:
    """Thread-safe counters and histograms for benchmark workers."""

    def __init__(
        self,
        columns: int,
        shift: int,
        warmup_intervals: int,
        cooldown_intervals: int,
        *,
        latency_style: str = "columns",
    ) -> None:
        self._columns = columns
        self._shift = shift
        self._warmup = warmup_intervals
        self._cooldown = cooldown_intervals
        self._latency_style = latency_style
        self._lock = threading.Lock()
        self._reads = 0
        self._writes = 0
        self._rtimeouts = 0
        self._wtimeouts = 0
        self._rerrs = 0
        self._werrs = 0
        self._r_le1 = 0
        self._w_le1 = 0
        self._r_gt = [0] * max(0, columns - 1)
        self._w_gt = [0] * max(0, columns - 1)
        self._thresholds = tuple(
            latency_threshold_ms(j + 1, shift) for j in range(max(0, columns - 1))
        )
        self._prev_reads = 0
        self._prev_writes = 0
        self._prev_rt = 0
        self._prev_wt = 0
        self._prev_re = 0
        self._prev_we = 0
        self._interval_idx = 0
        self._intervals: List[IntervalSnapshot] = []
        self._lat_summary: array.array[float] = array.array("f")
        self._proc = psutil.Process() if _HAS_PSUTIL else None
        self._peak_rss = 0
        self._cpu_samples: List[float] = []
        self._current_interval = 0
        self._planned_intervals = 0
        # YCSB per-op-type trackers
        self._ycsb_read = _YcsbTracker()
        self._ycsb_write = _YcsbTracker()

    def set_planned_intervals(self, n: int) -> None:
        with self._lock:
            self._planned_intervals = max(0, n)

    def set_interval(self, idx: int) -> None:
        with self._lock:
            self._current_interval = idx

    def include_latency_sample(self) -> bool:
        with self._lock:
            if self._planned_intervals <= 0:
                return False
            hi = self._planned_intervals - self._cooldown
            return self._warmup <= self._current_interval < hi

    def total_ops(self) -> int:
        with self._lock:
            return self._reads + self._writes

    def record(
        self,
        *,
        is_read: bool,
        latency_ms: float,
        is_timeout: bool,
        is_error: bool,
        include_in_summary_latency: bool,
    ) -> None:
        thresholds = self._thresholds
        with self._lock:
            if is_read:
                self._reads += 1
                if is_timeout:
                    self._rtimeouts += 1
                elif is_error:
                    self._rerrs += 1
                else:
                    if latency_ms <= 1.0:
                        self._r_le1 += 1
                    r_gt = self._r_gt
                    for j, thresh in enumerate(thresholds):
                        if latency_ms > thresh:
                            r_gt[j] += 1
            else:
                self._writes += 1
                if is_timeout:
                    self._wtimeouts += 1
                elif is_error:
                    self._werrs += 1
                else:
                    if latency_ms <= 1.0:
                        self._w_le1 += 1
                    w_gt = self._w_gt
                    for j, thresh in enumerate(thresholds):
                        if latency_ms > thresh:
                            w_gt[j] += 1
            if include_in_summary_latency and not is_error and not is_timeout:
                self._lat_summary.append(latency_ms)
                latency_us = int(latency_ms * 1000.0)
                if is_read:
                    self._ycsb_read.add(latency_us)
                else:
                    self._ycsb_write.add(latency_us)

    def sample_cpu(self) -> None:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        rss = ru.ru_maxrss
        if rss > self._peak_rss:
            self._peak_rss = rss
        if self._proc is not None:
            self._cpu_samples.append(self._proc.cpu_percent(interval=None))

    def end_interval(self) -> IntervalSnapshot:
        """Close the current 1-second window and return its deltas."""
        with self._lock:
            snap = IntervalSnapshot(index=self._interval_idx)
            snap.reads = self._reads - self._prev_reads
            snap.writes = self._writes - self._prev_writes
            snap.read_timeouts = self._rtimeouts - self._prev_rt
            snap.write_timeouts = self._wtimeouts - self._prev_wt
            snap.read_errors = self._rerrs - self._prev_re
            snap.write_errors = self._werrs - self._prev_we
            snap.read_le1 = self._r_le1
            snap.write_le1 = self._w_le1
            snap.read_gt = list(self._r_gt)
            snap.write_gt = list(self._w_gt)
            self._prev_reads = self._reads
            self._prev_writes = self._writes
            self._prev_rt = self._rtimeouts
            self._prev_wt = self._wtimeouts
            self._prev_re = self._rerrs
            self._prev_we = self._werrs
            self._r_le1 = 0
            self._w_le1 = 0
            self._r_gt = [0] * max(0, self._columns - 1)
            self._w_gt = [0] * max(0, self._columns - 1)
            self._interval_idx += 1
            self._intervals.append(snap)
            return snap

    def rss_mb_macos_linux(self) -> float:
        """Peak RSS from ``getrusage`` in megabytes (platform-specific unit)."""
        import sys

        rss = float(self._peak_rss)
        if sys.platform == "darwin":
            return rss / (1024 * 1024)
        return rss / 1024.0

    @staticmethod
    def _hist_percent_row(le1: int, gt: List[int], total: int) -> List[str]:
        if total <= 0:
            return ["0%"] * (1 + len(gt))
        row = [f"{100.0 * le1 / total:.0f}%"]
        for c in gt:
            row.append(f"{100.0 * c / total:.0f}%")
        return row

    def _format_tps_line(self, snap: IntervalSnapshot) -> str:
        total = snap.reads + snap.writes
        return (
            f"write(tps={snap.writes} timeouts={snap.write_timeouts} "
            f"errors={snap.write_errors}) read(tps={snap.reads} "
            f"timeouts={snap.read_timeouts} errors={snap.read_errors}) "
            f"total(tps={total} timeouts={snap.read_timeouts + snap.write_timeouts} "
            f"errors={snap.read_errors + snap.write_errors})"
        )

    def format_interval_lines(
        self,
        snap: IntervalSnapshot,
        stamp: str,
        labels: List[str],
    ) -> str:
        tps_line = f"{stamp} {self._format_tps_line(snap)}"

        if self._latency_style == "ycsb":
            lines = [tps_line]
            with self._lock:
                lines.append(self._ycsb_write.format_period_total("write"))
                lines.append(self._ycsb_read.format_period_total("read"))
                self._ycsb_write.reset_window()
                self._ycsb_read.reset_window()
            return "\n".join(lines)

        lines = [
            tps_line,
            "      " + " ".join(f"{lb:>7}" for lb in labels),
        ]
        lines.append(
            "write "
            + " ".join(
                f"{p:>6}"
                for p in self._hist_percent_row(
                    snap.write_le1, snap.write_gt, snap.writes,
                )
            )
        )
        lines.append(
            "read  "
            + " ".join(
                f"{p:>6}"
                for p in self._hist_percent_row(
                    snap.read_le1, snap.read_gt, snap.reads,
                )
            )
        )
        return "\n".join(lines)

    def summary_lines(
        self,
        labels: List[str],
    ) -> List[str]:
        """TPS averages and latency percentiles excluding warmup/cooldown."""
        del labels
        ivs = self._intervals
        if not ivs:
            return ["No intervals recorded."]
        n = len(ivs)
        lo = self._warmup
        hi = n - self._cooldown
        mid = ivs[lo:hi] if hi > lo else ivs

        def avg(xs: List[int]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        def median(xs: List[int]) -> float:
            if not xs:
                return 0.0
            ys = sorted(xs)
            m = len(ys) // 2
            if len(ys) % 2:
                return float(ys[m])
            return (ys[m - 1] + ys[m]) / 2.0

        r_tps = [x.reads for x in mid]
        w_tps = [x.writes for x in mid]
        t_tps = [x.reads + x.writes for x in mid]

        lat = sorted(self._lat_summary.tolist())
        pct_lines: List[str] = []
        if lat:

            def nearest_rank(p: float) -> float:
                n = len(lat)
                k = max(1, int(math.ceil(p / 100.0 * n)))
                return lat[k - 1]

            pct_lines = [
                f"  Latency p50={nearest_rank(50):.1f}ms  p90={nearest_rank(90):.1f}ms  "
                f"p99={nearest_rank(99):.1f}ms  p99.9={nearest_rank(99.9):.1f}ms  "
                f"max={lat[-1]:.1f}ms",
            ]
        lines = [
            f"Summary (excluding {self._warmup} warmup + {self._cooldown} cooldown intervals):",
            f"  Read  TPS: avg={avg(r_tps):.0f}  median={median(r_tps):.0f}",
            f"  Write TPS: avg={avg(w_tps):.0f}  median={median(w_tps):.0f}",
            f"  Total TPS: avg={avg(t_tps):.0f}  median={median(t_tps):.0f}",
            *pct_lines,
            f"  Peak RSS: {self.rss_mb_macos_linux():.1f} MB",
        ]
        if tracemalloc.is_tracing():
            _cur, peak = tracemalloc.get_traced_memory()
            lines.append(f"  Peak tracemalloc: {peak / (1024 * 1024):.1f} MB")
        if self._cpu_samples:
            mx = max(self._cpu_samples)
            lines.append(f"  Peak sampled CPU (process): {mx:.1f}%")
        return lines
