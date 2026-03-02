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

"""Unit tests for DSL arithmetic expressions.

"""

import pytest

from aerospike_fluent import DslParseException, Exp, parse_dsl


class TestArithmeticExpressions:
    """Test arithmetic expressions"""

    # --- add ---
    def test_add_1(self):
        """add() scenario 1: two bins."""
        expected = Exp.gt(
            Exp.num_add([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples + $.bananas) > 10")
        assert result == expected

    def test_add_2(self):
        """add() scenario 2: bin and literal."""
        expected = Exp.gt(
            Exp.num_add([Exp.int_bin("apples"), Exp.int_val(5)]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples + 5) > 10")
        assert result == expected

    def test_add_3(self):
        """add() scenario 3: float (literal and bin)."""
        expected = Exp.gt(
            Exp.num_add([Exp.float_val(5.2), Exp.float_bin("bananas")]),
            Exp.float_val(10.2)
        )
        result = parse_dsl("(5.2 + $.bananas) > 10.2")
        assert result == expected

    # --- subtract ---
    def test_subtract_1(self):
        """subtract() scenario 1: two bins."""
        expected = Exp.eq(
            Exp.num_sub([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples - $.bananas) == 10")
        assert result == expected

    def test_subtract_2(self):
        """subtract() scenario 2: bin and literal."""
        expected = Exp.eq(
            Exp.num_sub([Exp.int_bin("apples"), Exp.int_val(3)]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples - 3) == 10")
        assert result == expected

    def test_subtract_3(self):
        """subtract() scenario 3: literal and bin."""
        expected = Exp.eq(
            Exp.num_sub([Exp.int_val(100), Exp.int_bin("apples")]),
            Exp.int_val(10)
        )
        result = parse_dsl("(100 - $.apples) == 10")
        assert result == expected

    # --- multiply ---
    def test_multiply_1(self):
        """multiply() scenario 1: two bins."""
        expected = Exp.ne(
            Exp.num_mul([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples * $.bananas) != 10")
        assert result == expected

    def test_multiply_2(self):
        """multiply() scenario 2: bin and literal."""
        expected = Exp.ne(
            Exp.num_mul([Exp.int_bin("apples"), Exp.int_val(2)]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples * 2) != 10")
        assert result == expected

    def test_multiply_3(self):
        """multiply() scenario 3: literal and bin."""
        expected = Exp.ne(
            Exp.num_mul([Exp.int_val(3), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("(3 * $.bananas) != 10")
        assert result == expected

    # --- divide ---
    def test_divide_1(self):
        """divide() scenario 1: two bins."""
        expected = Exp.le(
            Exp.num_div([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples / $.bananas) <= 10")
        assert result == expected

    def test_divide_2(self):
        """divide() scenario 2: bin and literal."""
        expected = Exp.le(
            Exp.num_div([Exp.int_bin("apples"), Exp.int_val(2)]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples / 2) <= 10")
        assert result == expected

    def test_divide_3(self):
        """divide() scenario 3: literal and bin."""
        expected = Exp.le(
            Exp.num_div([Exp.int_val(100), Exp.int_bin("apples")]),
            Exp.int_val(10)
        )
        result = parse_dsl("(100 / $.apples) <= 10")
        assert result == expected

    # --- modulo ---
    def test_modulo_1(self):
        """mod() scenario 1: two bins."""
        expected = Exp.ne(
            Exp.num_mod(Exp.int_bin("apples"), Exp.int_bin("bananas")),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples % $.bananas) != 10")
        assert result == expected

    def test_modulo_2(self):
        """mod() scenario 2: bin and literal."""
        expected = Exp.ne(
            Exp.num_mod(Exp.int_bin("apples"), Exp.int_val(7)),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples % 7) != 10")
        assert result == expected

    def test_modulo_3(self):
        """mod() scenario 3: literal and bin."""
        expected = Exp.ne(
            Exp.num_mod(Exp.int_val(100), Exp.int_bin("apples")),
            Exp.int_val(10)
        )
        result = parse_dsl("(100 % $.apples) != 10")
        assert result == expected

    # --- intAnd (bitwise and) ---
    def test_int_and_1(self):
        """intAnd() scenario 1: two bins."""
        expected = Exp.ne(
            Exp.int_and([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples & $.bananas) != 10")
        assert result == expected

    def test_int_and_2(self):
        """intAnd() scenario 2: bin and literal."""
        expected = Exp.ne(
            Exp.int_and([Exp.int_bin("apples"), Exp.int_val(0xFF)]),
            Exp.int_val(0)
        )
        result = parse_dsl("($.apples & 255) != 0")
        assert result == expected

    def test_int_and_3(self):
        """intAnd() scenario 3: literal and bin."""
        expected = Exp.ne(
            Exp.int_and([Exp.int_val(0xFF), Exp.int_bin("flags")]),
            Exp.int_val(0)
        )
        result = parse_dsl("(255 & $.flags) != 0")
        assert result == expected

    # --- intOr (bitwise or) ---
    def test_int_or_1(self):
        """intOr() scenario 1: two bins."""
        expected = Exp.ne(
            Exp.int_or([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples | $.bananas) != 10")
        assert result == expected

    def test_int_or_2(self):
        """intOr() scenario 2: bin and literal."""
        expected = Exp.ne(
            Exp.int_or([Exp.int_bin("flags"), Exp.int_val(1)]),
            Exp.int_val(0)
        )
        result = parse_dsl("($.flags | 1) != 0")
        assert result == expected

    def test_int_or_3(self):
        """intOr() scenario 3: literal and bin."""
        expected = Exp.ne(
            Exp.int_or([Exp.int_val(1), Exp.int_bin("flags")]),
            Exp.int_val(0)
        )
        result = parse_dsl("(1 | $.flags) != 0")
        assert result == expected

    # --- intXor (bitwise xor) ---
    def test_int_xor_1(self):
        """intXor() scenario 1: two bins."""
        expected = Exp.ne(
            Exp.int_xor([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples ^ $.bananas) != 10")
        assert result == expected

    def test_int_xor_2(self):
        """intXor() scenario 2: bin and literal."""
        expected = Exp.ne(
            Exp.int_xor([Exp.int_bin("mask"), Exp.int_val(0xFF)]),
            Exp.int_val(0)
        )
        result = parse_dsl("($.mask ^ 255) != 0")
        assert result == expected

    def test_int_xor_3(self):
        """intXor() scenario 3: literal and bin."""
        expected = Exp.ne(
            Exp.int_xor([Exp.int_val(0xFF), Exp.int_bin("mask")]),
            Exp.int_val(0)
        )
        result = parse_dsl("(255 ^ $.mask) != 0")
        assert result == expected

    # --- intNot (bitwise not) ---
    def test_int_not_1(self):
        """intNot() scenario 1: not bin."""
        expected = Exp.ne(
            Exp.int_not(Exp.int_bin("apples")),
            Exp.int_val(10)
        )
        result = parse_dsl("(~$.apples) != 10")
        assert result == expected

    def test_int_not_2(self):
        """intNot() scenario 2: not in compound expression."""
        expected = Exp.eq(
            Exp.int_not(Exp.int_bin("flags")),
            Exp.int_val(0)
        )
        result = parse_dsl("~$.flags == 0")
        assert result == expected

    # --- left shift ---
    def test_left_shift_1(self):
        """leftShift() scenario 1: bin and literal."""
        expected = Exp.int_lshift(Exp.int_bin("visits"), Exp.int_val(1))
        result = parse_dsl("$.visits << 1")
        assert result == expected

    def test_left_shift_2(self):
        """leftShift() scenario 2: literal and bin."""
        expected = Exp.int_lshift(Exp.int_val(1), Exp.int_bin("n"))
        result = parse_dsl("1 << $.n")
        assert result == expected

    def test_left_shift_3(self):
        """leftShift() scenario 3: two bins."""
        expected = Exp.int_lshift(Exp.int_bin("a"), Exp.int_bin("b"))
        result = parse_dsl("$.a << $.b")
        assert result == expected

    # --- arithmetic right shift (>>) ---
    def test_arithmetic_right_shift_1(self):
        """>> scenario 1: bin and literal."""
        expected = Exp.int_arshift(Exp.int_bin("flags"), Exp.int_val(6))
        result = parse_dsl("$.flags >> 6")
        assert result == expected

    def test_arithmetic_right_shift_2(self):
        """>> scenario 2: compound (bin >> literal) & literal."""
        expected = Exp.eq(
            Exp.int_and([
                Exp.int_arshift(Exp.int_bin("flags"), Exp.int_val(6)),
                Exp.int_val(1)
            ]),
            Exp.int_val(1)
        )
        result = parse_dsl("(($.flags >> 6) & 1) == 1")
        assert result == expected

    def test_arithmetic_right_shift_3(self):
        """>> scenario 3: literal and bin."""
        expected = Exp.int_arshift(Exp.int_val(256), Exp.int_bin("n"))
        result = parse_dsl("256 >> $.n")
        assert result == expected

    # --- logical right shift (>>>) ---
    def test_logical_right_shift_1(self):
        """>>> scenario 1: bin and literal."""
        expected = Exp.int_rshift(Exp.int_bin("flags"), Exp.int_val(6))
        result = parse_dsl("$.flags >>> 6")
        assert result == expected

    def test_logical_right_shift_2(self):
        """>>> scenario 2: compound (bin >>> literal) & literal."""
        expected = Exp.eq(
            Exp.int_and([
                Exp.int_rshift(Exp.int_bin("flags"), Exp.int_val(6)),
                Exp.int_val(1)
            ]),
            Exp.int_val(1)
        )
        result = parse_dsl("(($.flags >>> 6) & 1) == 1")
        assert result == expected

    def test_logical_right_shift_3(self):
        """>>> scenario 3: literal and bin."""
        expected = Exp.int_rshift(Exp.int_val(256), Exp.int_bin("n"))
        result = parse_dsl("256 >>> $.n")
        assert result == expected

    # --- >> vs >>> produce different expressions ---
    def test_arithmetic_vs_logical_rshift(self):
        """>> and >>> map to different PAC methods."""
        arith = parse_dsl("$.x >> 1")
        logical = parse_dsl("$.x >>> 1")
        assert arith != logical

    # --- negative (type mismatch) ---
    def test_arithmetic_negative_type_mismatch(self):
        """12 + negative — arithmetic with number + non-numeric raises (negative type-mismatch)."""
        with pytest.raises(DslParseException, match="numeric"):
            parse_dsl('(12 + "x") > 0')


class TestArithmeticFunctions:
    """Test arithmetic function calls: abs, ceil, floor, log, pow, max, min."""

    # --- abs ---
    def test_abs_literal(self):
        expected = Exp.eq(Exp.num_abs(Exp.int_val(-12)), Exp.int_val(12))
        result = parse_dsl("abs(-12) == 12")
        assert result == expected

    def test_abs_bin(self):
        expected = Exp.gt(Exp.num_abs(Exp.int_bin("delta")), Exp.int_val(0))
        result = parse_dsl("abs($.delta) > 0")
        assert result == expected

    def test_abs_nested(self):
        expected = Exp.lt(
            Exp.num_abs(Exp.num_sub([Exp.int_bin("a"), Exp.int_bin("b")])),
            Exp.int_val(10)
        )
        result = parse_dsl("abs($.a - $.b) < 10")
        assert result == expected

    def test_abs_float(self):
        expected = Exp.eq(
            Exp.num_abs(Exp.float_val(-3.14)),
            Exp.float_val(3.14)
        )
        result = parse_dsl("abs(-3.14) == 3.14")
        assert result == expected

    # --- ceil ---
    def test_ceil_literal(self):
        expected = Exp.eq(Exp.num_ceil(Exp.float_val(12.34)), Exp.float_val(13.0))
        result = parse_dsl("ceil(12.34) == 13.0")
        assert result == expected

    def test_ceil_bin(self):
        expected = Exp.eq(
            Exp.num_ceil(Exp.float_bin("price")),
            Exp.float_val(100.0)
        )
        result = parse_dsl("ceil($.price) == 100.0")
        assert result == expected

    # --- floor ---
    def test_floor_literal(self):
        expected = Exp.eq(Exp.num_floor(Exp.float_val(12.99)), Exp.float_val(12.0))
        result = parse_dsl("floor(12.99) == 12.0")
        assert result == expected

    def test_floor_bin(self):
        expected = Exp.lt(
            Exp.num_floor(Exp.float_bin("score")),
            Exp.float_val(50.0)
        )
        result = parse_dsl("floor($.score) < 50.0")
        assert result == expected

    # --- log ---
    def test_log_literals(self):
        expected = Exp.eq(
            Exp.num_log(Exp.float_val(1024.0), Exp.float_val(2.0)),
            Exp.float_val(10.0)
        )
        result = parse_dsl("log(1024.0, 2.0) == 10.0")
        assert result == expected

    def test_log_bin(self):
        expected = Exp.gt(
            Exp.num_log(Exp.float_bin("val"), Exp.float_val(10.0)),
            Exp.float_val(3.0)
        )
        result = parse_dsl("log($.val, 10.0) > 3.0")
        assert result == expected

    # --- pow ---
    def test_pow_literals(self):
        expected = Exp.eq(
            Exp.num_pow(Exp.float_val(2.0), Exp.float_val(10.0)),
            Exp.float_val(1024.0)
        )
        result = parse_dsl("pow(2.0, 10.0) == 1024.0")
        assert result == expected

    def test_pow_bin(self):
        expected = Exp.gt(
            Exp.num_pow(Exp.float_val(2.0), Exp.float_bin("exponent")),
            Exp.float_val(100.0)
        )
        result = parse_dsl("pow(2.0, $.exponent) > 100.0")
        assert result == expected

    # --- max ---
    def test_max_two_args(self):
        expected = Exp.gt(
            Exp.max([Exp.int_bin("a"), Exp.int_bin("b")]),
            Exp.int_val(10)
        )
        result = parse_dsl("max($.a, $.b) > 10")
        assert result == expected

    def test_max_three_args(self):
        expected = Exp.gt(
            Exp.max([Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")]),
            Exp.int_val(10)
        )
        result = parse_dsl("max($.a, $.b, $.c) > 10")
        assert result == expected

    def test_max_literals(self):
        expected = Exp.eq(
            Exp.max([Exp.int_val(1), Exp.int_val(5), Exp.int_val(3)]),
            Exp.int_val(5)
        )
        result = parse_dsl("max(1, 5, 3) == 5")
        assert result == expected

    def test_max_float(self):
        expected = Exp.gt(
            Exp.max([Exp.float_bin("x"), Exp.float_bin("y")]),
            Exp.float_val(0.0)
        )
        result = parse_dsl("max($.x, $.y) > 0.0")
        assert result == expected

    # --- min ---
    def test_min_two_args(self):
        expected = Exp.lt(
            Exp.min([Exp.int_bin("a"), Exp.int_bin("b")]),
            Exp.int_val(10)
        )
        result = parse_dsl("min($.a, $.b) < 10")
        assert result == expected

    def test_min_three_args(self):
        expected = Exp.lt(
            Exp.min([Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")]),
            Exp.int_val(10)
        )
        result = parse_dsl("min($.a, $.b, $.c) < 10")
        assert result == expected

    def test_min_literals(self):
        expected = Exp.eq(
            Exp.min([Exp.int_val(1), Exp.int_val(5), Exp.int_val(3)]),
            Exp.int_val(1)
        )
        result = parse_dsl("min(1, 5, 3) == 1")
        assert result == expected

    # --- nested functions ---
    def test_nested_abs_in_max(self):
        expected = Exp.gt(
            Exp.max([Exp.num_abs(Exp.int_bin("a")), Exp.num_abs(Exp.int_bin("b"))]),
            Exp.int_val(5)
        )
        result = parse_dsl("max(abs($.a), abs($.b)) > 5")
        assert result == expected

    def test_pow_with_ceil(self):
        expected = Exp.eq(
            Exp.num_ceil(Exp.num_pow(Exp.float_val(2.0), Exp.float_val(0.5))),
            Exp.float_val(2.0)
        )
        result = parse_dsl("ceil(pow(2.0, 0.5)) == 2.0")
        assert result == expected


class TestPowerInfixOperator:
    """Test ** (power) infix operator: right-associative, float-only."""

    def test_pow_infix_literals(self):
        """Spec example: 2**7 = 128 (float-only)."""
        expected = Exp.eq(
            Exp.num_pow(Exp.float_val(2.0), Exp.float_val(7.0)),
            Exp.float_val(128.0)
        )
        result = parse_dsl("2.0 ** 7.0 == 128.0")
        assert result == expected

    def test_pow_infix_bin(self):
        expected = Exp.gt(
            Exp.num_pow(Exp.float_val(2.0), Exp.float_bin("exponent")),
            Exp.float_val(100.0)
        )
        result = parse_dsl("2.0 ** $.exponent > 100.0")
        assert result == expected

    def test_pow_infix_right_associative(self):
        """4.0 ** 3.0 ** 2.0 should parse as 4.0 ** (3.0 ** 2.0), not (4.0 ** 3.0) ** 2.0."""
        expected = Exp.eq(
            Exp.num_pow(
                Exp.float_val(4.0),
                Exp.num_pow(Exp.float_val(3.0), Exp.float_val(2.0))
            ),
            Exp.float_val(262144.0)
        )
        result = parse_dsl("4.0 ** 3.0 ** 2.0 == 262144.0")
        assert result == expected

    def test_pow_infix_higher_precedence_than_multiply(self):
        """2.0 * 3.0 ** 2.0 should parse as 2.0 * (3.0 ** 2.0) = 18."""
        expected = Exp.eq(
            Exp.num_mul([Exp.float_val(2.0), Exp.num_pow(Exp.float_val(3.0), Exp.float_val(2.0))]),
            Exp.float_val(18.0)
        )
        result = parse_dsl("2.0 * 3.0 ** 2.0 == 18.0")
        assert result == expected

    def test_pow_infix_with_parens(self):
        """(2.0 ** 3.0) ** 2.0 with explicit parens overrides associativity."""
        expected = Exp.eq(
            Exp.num_pow(
                Exp.num_pow(Exp.float_val(2.0), Exp.float_val(3.0)),
                Exp.float_val(2.0)
            ),
            Exp.float_val(64.0)
        )
        result = parse_dsl("(2.0 ** 3.0) ** 2.0 == 64.0")
        assert result == expected

    def test_pow_infix_with_addition(self):
        """1.0 + 2.0 ** 3.0 should parse as 1.0 + (2.0 ** 3.0) = 9."""
        expected = Exp.eq(
            Exp.num_add([Exp.float_val(1.0), Exp.num_pow(Exp.float_val(2.0), Exp.float_val(3.0))]),
            Exp.float_val(9.0)
        )
        result = parse_dsl("1.0 + 2.0 ** 3.0 == 9.0")
        assert result == expected


class TestNumberLiterals:
    """Test hex and binary integer literals."""

    # --- hex ---
    def test_hex_literal_comparison(self):
        expected = Exp.eq(Exp.int_val(0xFF), Exp.int_val(255))
        result = parse_dsl("0xFF == 255")
        assert result == expected

    def test_hex_literal_with_bin(self):
        expected = Exp.gt(Exp.int_bin("flags"), Exp.int_val(0xA))
        result = parse_dsl("$.flags > 0xA")
        assert result == expected

    def test_hex_in_bitwise_and(self):
        expected = Exp.ne(
            Exp.int_and([Exp.int_bin("flags"), Exp.int_val(0xFF)]),
            Exp.int_val(0)
        )
        result = parse_dsl("($.flags & 0xFF) != 0")
        assert result == expected

    def test_negative_hex(self):
        expected = Exp.eq(Exp.int_val(-0xFF), Exp.int_val(-255))
        result = parse_dsl("-0xFF == -255")
        assert result == expected

    def test_hex_lowercase(self):
        expected = Exp.eq(Exp.int_val(0xff), Exp.int_val(255))
        result = parse_dsl("0xff == 255")
        assert result == expected

    def test_hex_mixed_case(self):
        expected = Exp.eq(Exp.int_val(0xAbCd), Exp.int_val(43981))
        result = parse_dsl("0xAbCd == 43981")
        assert result == expected

    # --- binary ---
    def test_binary_literal_comparison(self):
        expected = Exp.eq(Exp.int_val(0b1010), Exp.int_val(10))
        result = parse_dsl("0b1010 == 10")
        assert result == expected

    def test_binary_in_bitwise_and(self):
        expected = Exp.eq(
            Exp.int_and([Exp.int_bin("flags"), Exp.int_val(0b1111)]),
            Exp.int_val(0b0101)
        )
        result = parse_dsl("($.flags & 0b1111) == 0b0101")
        assert result == expected

    def test_negative_binary(self):
        expected = Exp.eq(Exp.int_val(-0b1010), Exp.int_val(-10))
        result = parse_dsl("-0b1010 == -10")
        assert result == expected

    # --- mixed ---
    def test_hex_and_binary_in_expression(self):
        expected = Exp.eq(
            Exp.int_or([Exp.int_val(0xFF), Exp.int_val(0b1010)]),
            Exp.int_val(255)
        )
        result = parse_dsl("(0xFF | 0b1010) == 255")
        assert result == expected

    def test_hex_in_shift(self):
        expected = Exp.eq(
            Exp.int_lshift(Exp.int_val(0x1), Exp.int_val(8)),
            Exp.int_val(0x100)
        )
        result = parse_dsl("(0x1 << 8) == 0x100")
        assert result == expected
