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

    # --- right shift ---
    def test_right_shift_1(self):
        """rightShift() scenario 1: bin and literal."""
        expected = Exp.int_rshift(Exp.int_bin("flags"), Exp.int_val(6))
        result = parse_dsl("$.flags >> 6")
        assert result == expected

    def test_right_shift_2(self):
        """rightShift() scenario 2: compound (bin >> literal) & literal."""
        expected = Exp.eq(
            Exp.int_and([
                Exp.int_rshift(Exp.int_bin("flags"), Exp.int_val(6)),
                Exp.int_val(1)
            ]),
            Exp.int_val(1)
        )
        result = parse_dsl("(($.flags >> 6) & 1) == 1")
        assert result == expected

    def test_right_shift_3(self):
        """rightShift() scenario 3: literal and bin."""
        expected = Exp.int_rshift(Exp.int_val(256), Exp.int_bin("n"))
        result = parse_dsl("256 >> $.n")
        assert result == expected

    # --- negative (type mismatch) ---
    def test_arithmetic_negative_type_mismatch(self):
        """12 + negative — arithmetic with number + non-numeric raises (negative type-mismatch)."""
        with pytest.raises(DslParseException, match="numeric"):
            parse_dsl('(12 + "x") > 0')
