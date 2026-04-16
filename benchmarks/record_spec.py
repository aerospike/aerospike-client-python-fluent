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

"""Bin layout parsing and value generation for benchmark records."""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Sequence

# Module-level PRNG for bin value generation.  Cryptographic randomness
# (secrets/os.urandom) is unnecessary for synthetic benchmark data and
# costs ~0.44s per 10s run due to syscall overhead.
_rng = random.Random()

_TOKEN_RE = re.compile(r"^([ISB])(\d+)$", re.IGNORECASE)


BinKind = Literal["int", "str", "bytes"]


@dataclass(frozen=True)
class BinField:
    """One bin in the benchmark record shape."""

    name: str
    kind: BinKind
    size: int

    def random_value(self) -> Any:
        if self.kind == "int":
            return _rng.randrange(1 << 30)
        if self.kind == "str":
            return _rng.randbytes(max(1, (self.size + 1) // 2)).hex()[: self.size]
        return _rng.randbytes(self.size)


def parse_bin_spec(spec: str) -> List[BinField]:
    """Parse a comma-separated bin specification such as ``I1,S128,B1024``.

    Args:
        spec: Tokens ``I<n>`` (integer stored in ``n`` bits of entropy via int),
            ``S<n>`` (``n``-character hex string), ``B<n>`` (``n`` random bytes).

    Returns:
        Field list ``b0``, ``b1``, ... in token order.

    Raises:
        ValueError: If the string is empty or any token is invalid.
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("bin spec must not be empty")
    fields: List[BinField] = []
    for i, tok in enumerate(spec.split(",")):
        tok = tok.strip()
        m = _TOKEN_RE.match(tok)
        if not m:
            raise ValueError(f"invalid bin token {tok!r}")
        kind_ch, n_s = m.group(1).upper(), m.group(2)
        n = int(n_s)
        if n <= 0:
            raise ValueError(f"size must be positive in token {tok!r}")
        if kind_ch == "I":
            fields.append(BinField(f"b{i}", "int", n))
        elif kind_ch == "S":
            fields.append(BinField(f"b{i}", "str", n))
        else:
            fields.append(BinField(f"b{i}", "bytes", n))
    return fields


def full_bins(fields: Sequence[BinField]) -> Dict[str, Any]:
    """Build a mapping of bin name to freshly generated value for every field."""
    return {f.name: f.random_value() for f in fields}


def single_bin_put(fields: Sequence[BinField], idx: int) -> Dict[str, Any]:
    """Return a one-bin put payload for ``fields[idx]``."""
    f = fields[idx % len(fields)]
    return {f.name: f.random_value()}


def pick_bin_index(rng: Any, n: int) -> int:
    """Uniform index in ``[0, n)`` using ``rng.randrange``."""
    return int(rng.randrange(0, n))


def first_integer_bin(fields: Sequence[BinField]) -> str:
    """Return the name of the first integer bin, for increment workloads.

    Raises:
        ValueError: If no integer bin exists.
    """
    for f in fields:
        if f.kind == "int":
            return f.name
    raise ValueError("workload requires at least one I<n> bin in -o spec")
