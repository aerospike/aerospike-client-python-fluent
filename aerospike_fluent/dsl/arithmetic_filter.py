"""Arithmetic-to-filter extraction for DSL expressions.

Builds Filter.range(bin, min, max) or Filter.equal from arithmetic comparison node data
(e.g. ($.apples + 5) > 10) when the expression can be reduced to a bin range.
Tree-driven via filter_from_arithmetic_node; no string parsing.
"""

import math
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from aerospike_async import Filter

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
    """Smallest integer > value; if value is integral, value+1."""
    floored = math.floor(value)
    if value == floored:
        return int(floored) + 1
    return int(math.ceil(value))


def _closest_long_to_the_left(value: float) -> int:
    """Largest integer < value; if value is integral, value-1."""
    floored = math.floor(value)
    if value == floored:
        return int(floored) - 1
    return int(floored)


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
    """(bin * c) rel k. If c > 0: bin rel k/c. If c < 0: invert rel and negate k. If c == 0: return None. Uses integer division."""
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
    # For positive divisor: GT -> (left*right+1, MAX), GTEQ -> (left*right, MAX), LT -> (MIN, left*right-1), LTEQ -> (MIN, left*right).
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
        # c < 0: GT -> (MIN, left*right-1), GTEQ -> (MIN, left*right), LT -> (left*right+1, MAX), LTEQ -> (left*right, MAX)
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
    """bin is divisor: (c / bin) rel k. Returns None when no integer solutions (e.g. single-point range that does not satisfy)."""
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


def filter_from_arithmetic_node(
    bin_name: str,
    arith_op: str,
    arith_constant: int,
    bin_on_left: bool,
    rel_op: Any,
    comparison_value: int,
    ctx: Optional[List[Any]] = None,
) -> Optional[Filter]:
    """
    Build a Filter from arithmetic comparison node data (tree-driven, no string parsing).
    rel_op: comparison operator as string '>', '>=', '<', '<=', or '=='; or OperationType.
    """
    op_to_rel = {">": ">", ">=": ">=", "<": "<", "<=": "<=", "==": "==", "GT": ">", "GE": ">=", "LT": "<", "LE": "<=", "EQ": "=="}
    rel = op_to_rel.get(rel_op) if isinstance(rel_op, str) else op_to_rel.get(getattr(rel_op, "value", None))
    if rel is None:
        return None
    if rel == "==":
        eq_val: Optional[int] = None
        if arith_op == "+":
            eq_val = comparison_value - arith_constant
        elif arith_op == "-":
            eq_val = comparison_value + arith_constant if bin_on_left else arith_constant - comparison_value
        elif arith_op == "*":
            if arith_constant != 0 and comparison_value % arith_constant == 0:
                eq_val = comparison_value // arith_constant
        elif arith_op == "/":
            if arith_constant != 0 and bin_on_left:
                eq_val = comparison_value * arith_constant
            elif comparison_value != 0 and not bin_on_left and arith_constant % comparison_value == 0:
                eq_val = arith_constant // comparison_value
        if eq_val is not None:
            if ctx:
                try:
                    return Filter.equal(bin_name, eq_val, *ctx)
                except TypeError:
                    return Filter.equal(bin_name, eq_val)
            return Filter.equal(bin_name, eq_val)
        return None
    expr = _ArithExpr(bin_name=bin_name, op=arith_op, constant=arith_constant, bin_on_left=bin_on_left)
    result: Optional[_ArithmeticFilterResult] = None
    if arith_op == "+":
        result = _solve_add(expr, rel, comparison_value)
    elif arith_op == "-":
        result = _solve_sub(expr, rel, comparison_value)
    elif arith_op == "*":
        result = _solve_mul(expr, rel, comparison_value)
    elif arith_op == "/":
        result = _solve_div(expr, rel, comparison_value)
    if result is None or result.range_min > result.range_max:
        return None
    try:
        return Filter.range(bin_name, result.range_min, result.range_max, *ctx) if ctx else Filter.range(bin_name, result.range_min, result.range_max)
    except TypeError:
        return Filter.range(bin_name, result.range_min, result.range_max)
