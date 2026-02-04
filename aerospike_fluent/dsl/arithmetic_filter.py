"""Arithmetic-to-filter extraction for DSL expressions.

Parses single comparisons like ($.apples + 5) > 10 and produces Filter.range(bin, min, max)
when the expression can be reduced to a bin range. Matches JFC VisitorUtils logic.
"""

import math
import re
from dataclasses import dataclass
from typing import Optional, Tuple

MIN_LONG = -(2**63)
MAX_LONG = 2**63 - 1


@dataclass
class _ArithExpr:
    """Parsed arithmetic expression: one bin and one integer constant."""
    bin_name: str
    op: str  # '+', '-', '*', '/'
    constant: int  # the literal in the expression
    bin_on_left: bool  # True if $.bin op const, False if const op $.bin


@dataclass
class _ArithmeticFilterResult:
    """Result of parsing an arithmetic comparison."""
    bin_name: str
    range_min: int
    range_max: int


def _closest_long_to_the_right(value: float) -> int:
    """Smallest integer > value; if value is integral, value+1 (JFC getClosestLongToTheRight)."""
    floored = math.floor(value)
    if value == floored:
        return int(floored) + 1
    return int(math.ceil(value))


def _closest_long_to_the_left(value: float) -> int:
    """Largest integer < value; if value is integral, value-1 (JFC getClosestLongToTheLeft)."""
    floored = math.floor(value)
    if value == floored:
        return int(floored) - 1
    return int(floored)


# Regex-based parser for single arithmetic comparison.
_BIN_NAME = r'\$\.([a-zA-Z_][a-zA-Z0-9_]*)'
_INT = r'(-?\d+)'
_OP_REL = r'(>=|<=|>|<)'
# Inner: ( $.bin op int ) or ( int op $.bin )
_RE_ARITH_LEFT = re.compile(
    r'^\s*\(\s*' + _BIN_NAME + r'\s*([+\-*/])\s*' + _INT + r'\s*\)\s*' + _OP_REL + r'\s*' + _INT + r'\s*$'
)
_RE_ARITH_RIGHT = re.compile(
    r'^\s*' + _INT + r'\s*' + _OP_REL + r'\s*\(\s*' + _BIN_NAME + r'\s*([+\-*/])\s*' + _INT + r'\s*\)\s*$'
)
_RE_ARITH_LEFT_SWAP = re.compile(
    r'^\s*\(\s*' + _INT + r'\s*([+\-*/])\s*' + _BIN_NAME + r'\s*\)\s*' + _OP_REL + r'\s*' + _INT + r'\s*$'
)
_RE_ARITH_RIGHT_SWAP = re.compile(
    r'^\s*' + _INT + r'\s*' + _OP_REL + r'\s*\(\s*' + _INT + r'\s*([+\-*/])\s*' + _BIN_NAME + r'\s*\)\s*$'
)


def _parse_arithmetic_comparison(dsl: str) -> Optional[Tuple[_ArithExpr, str, int]]:
    """
    If dsl is a single comparison of the form (arithmetic) rel const or const rel (arithmetic),
    return (arith_expr, rel_op, comparison_const). Otherwise return None.
    rel_op is one of '>', '>=', '<', '<='.
    """
    dsl = dsl.strip()
    for pattern, bin_left, c1_first in [
        (_RE_ARITH_LEFT, True, True),   # ($.bin op c) rel k
        (_RE_ARITH_LEFT_SWAP, True, False),  # (c op $.bin) rel k
        (_RE_ARITH_RIGHT, False, True),  # k rel ($.bin op c)
        (_RE_ARITH_RIGHT_SWAP, False, False),  # k rel (c op $.bin)
    ]:
        m = pattern.match(dsl)
        if not m:
            continue
        if bin_left:
            if c1_first:
                bin_name, op, const, rel, k = m.group(1), m.group(2), int(m.group(3)), m.group(4), int(m.group(5))
                return _ArithExpr(bin_name=bin_name, op=op, constant=const, bin_on_left=True), rel, k
            else:
                const, op, bin_name, rel, k = int(m.group(1)), m.group(2), m.group(3), m.group(4), int(m.group(5))
                return _ArithExpr(bin_name=bin_name, op=op, constant=const, bin_on_left=False), rel, k
        else:
            if c1_first:
                k, rel, bin_name, op, const = int(m.group(1)), m.group(2), m.group(3), m.group(4), int(m.group(5))
                return _ArithExpr(bin_name=bin_name, op=op, constant=const, bin_on_left=True), rel, k
            else:
                k, rel, const, op, bin_name = int(m.group(1)), m.group(2), int(m.group(3)), m.group(4), m.group(5)
                return _ArithExpr(bin_name=bin_name, op=op, constant=const, bin_on_left=False), rel, k
    return None


def _solve_add(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    """(bin + c) rel k or (c + bin) rel k => bin rel (k - c)."""
    threshold = k - expr.constant
    if rel == '>':
        return _ArithmeticFilterResult(expr.bin_name, _closest_long_to_the_right(threshold), MAX_LONG)
    if rel == '>=':
        return _ArithmeticFilterResult(expr.bin_name, int(threshold), MAX_LONG)
    if rel == '<':
        return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, _closest_long_to_the_left(threshold))
    if rel == '<=':
        return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, int(threshold))
    return None


def _solve_sub_const_minus_bin(rel: str, c: int, k: int, bin_name: str) -> Optional[_ArithmeticFilterResult]:
    """(c - bin) rel k => bin (invert rel) (c - k)."""
    threshold = c - k
    if rel == '>':   # (c - bin) > k  => bin < c - k
        return _ArithmeticFilterResult(bin_name, MIN_LONG, _closest_long_to_the_left(threshold))
    if rel == '>=':
        return _ArithmeticFilterResult(bin_name, MIN_LONG, int(threshold))
    if rel == '<':   # (c - bin) < k  => bin > c - k
        return _ArithmeticFilterResult(bin_name, _closest_long_to_the_right(threshold), MAX_LONG)
    if rel == '<=':
        return _ArithmeticFilterResult(bin_name, int(threshold), MAX_LONG)
    return None


def _solve_sub(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    """(bin - c) rel k => bin rel (k + c). (c - bin) rel k => invert and bin rel (c - k)."""
    if expr.bin_on_left:
        threshold = k + expr.constant
        if rel == '>':
            return _ArithmeticFilterResult(expr.bin_name, _closest_long_to_the_right(threshold), MAX_LONG)
        if rel == '>=':
            return _ArithmeticFilterResult(expr.bin_name, int(threshold), MAX_LONG)
        if rel == '<':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, _closest_long_to_the_left(threshold))
        if rel == '<=':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, int(threshold))
        return None
    else:
        return _solve_sub_const_minus_bin(rel, expr.constant, k, expr.bin_name)


def _solve_mul(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    """(bin * c) rel k. If c > 0: bin rel k/c. If c < 0: invert rel and negate k. If c == 0: return None. Uses integer division to match JFC."""
    c = expr.constant
    if c == 0:
        return None
    if c < 0:
        inv = {'>': '<', '>=': '<=', '<': '>', '<=': '>='}
        rel = inv[rel]
        c = -c
        k = -k
    threshold = k // c
    if rel == '>':
        return _ArithmeticFilterResult(expr.bin_name, threshold + 1, MAX_LONG)
    if rel == '>=':
        return _ArithmeticFilterResult(expr.bin_name, threshold, MAX_LONG)
    if rel == '<':
        return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, threshold - 1)
    if rel == '<=':
        return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, threshold)
    return None


def _solve_div_dividend(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    """bin is dividend: (bin / c) rel k => bin rel (k * c) with sign rules."""
    c = expr.constant
    if c == 0:
        return None
    # JFC: LimitsForBinDividend. For positive divisor: GT -> (left*right+1, MAX), GTEQ -> (left*right, MAX), LT -> (MIN, left*right-1), LTEQ -> (MIN, left*right).
    # left is "result of comparison" (k), right is divisor (c). So bin/c > k => bin > k*c => range(k*c+1, MAX).
    if c > 0:
        if rel == '>':
            return _ArithmeticFilterResult(expr.bin_name, k * c + 1, MAX_LONG)
        if rel == '>=':
            return _ArithmeticFilterResult(expr.bin_name, k * c, MAX_LONG)
        if rel == '<':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, k * c - 1)
        if rel == '<=':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, k * c)
    else:
        # c < 0: JFC getLimitsForBinDividendWithLeftNumberNegative: GT -> (MIN, left*right-1), GTEQ -> (MIN, left*right), LT -> (left*right+1, MAX), LTEQ -> (left*right, MAX)
        if rel == '>':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, k * c - 1)
        if rel == '>=':
            return _ArithmeticFilterResult(expr.bin_name, MIN_LONG, k * c)
        if rel == '<':
            return _ArithmeticFilterResult(expr.bin_name, k * c + 1, MAX_LONG)
        if rel == '<=':
            return _ArithmeticFilterResult(expr.bin_name, k * c, MAX_LONG)
    return None


def _solve_div_divisor(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    """bin is divisor: (c / bin) rel k. JFC getLimitsForBinDivisor. Returns None when no integer solutions (e.g. single-point range that does not satisfy)."""
    c = expr.constant
    if c == 0:
        return None
    left = c
    right = k
    res: Optional[_ArithmeticFilterResult] = None
    if left > 0 and right > 0:
        if rel == '>':
            res = _ArithmeticFilterResult(expr.bin_name, 1, _closest_long_to_the_left((float(left)) / right))
        elif rel == '>=':
            res = _ArithmeticFilterResult(expr.bin_name, 1, left // right)
    elif left < 0 and right < 0:
        if rel == '<':
            res = _ArithmeticFilterResult(expr.bin_name, 1, _closest_long_to_the_left((float(left)) / right))
        elif rel == '<=':
            res = _ArithmeticFilterResult(expr.bin_name, 1, left // right)
    elif left > 0 and right < 0:
        if rel == '<':
            res = _ArithmeticFilterResult(expr.bin_name, _closest_long_to_the_right((float(left)) / right), -1)
        elif rel == '<=':
            res = _ArithmeticFilterResult(expr.bin_name, left // right, -1)
    elif left < 0 and right > 0:
        if rel == '>':
            res = _ArithmeticFilterResult(expr.bin_name, _closest_long_to_the_right((float(left)) / right), -1)
        elif rel == '>=':
            res = _ArithmeticFilterResult(expr.bin_name, left // right, -1)
    if res is not None and res.range_min == res.range_max:
        bin_val = res.range_min
        if bin_val == 0:
            return None
        q = left // bin_val
        if rel == '>' and q > right:
            return res
        if rel == '>=' and q >= right:
            return res
        if rel == '<' and q < right:
            return res
        if rel == '<=' and q <= right:
            return res
        return None
    return res


def _solve_div(expr: _ArithExpr, rel: str, k: int) -> Optional[_ArithmeticFilterResult]:
    if expr.bin_on_left:
        return _solve_div_dividend(expr, rel, k)
    return _solve_div_divisor(expr, rel, k)


def try_arithmetic_filter(
    dsl: str,
) -> Optional[_ArithmeticFilterResult]:
    """
    If dsl is a single arithmetic comparison (e.g. ($.apples + 5) > 10) that can be
    reduced to a bin range, return (bin_name, range_min, range_max). Otherwise return None.
    Float operands or unsupported forms return None.
    """
    parsed = _parse_arithmetic_comparison(dsl)
    if not parsed:
        return None
    expr, rel, k = parsed
    if expr.op == '+':
        return _solve_add(expr, rel, k)
    if expr.op == '-':
        return _solve_sub(expr, rel, k)
    if expr.op == '*':
        return _solve_mul(expr, rel, k)
    if expr.op == '/':
        return _solve_div(expr, rel, k)
    return None
