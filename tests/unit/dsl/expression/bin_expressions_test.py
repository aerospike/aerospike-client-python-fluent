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

"""Unit tests for DSL bin expressions."""

import pytest
from aerospike_fluent import DslParseException, Exp, parse_dsl


class TestBinExpressions:
    """Test basic bin comparison expressions."""

    def test_bin_gt(self):
        """$.A > 10 (int bin greater-than)."""
        expected = Exp.gt(Exp.int_bin("A"), Exp.int_val(10))
        result = parse_dsl("$.A > 10")
        assert result == expected

    def test_bin_ge(self):
        """>= int and string."""
        expected = Exp.ge(Exp.int_bin("A"), Exp.int_val(100))
        result = parse_dsl("$.A >= 100")
        assert result == expected
        result = parse_dsl("$.name >= 'text'")
        assert result == Exp.ge(Exp.string_bin("name"), Exp.string_val("text"))
        result = parse_dsl('$.name >= "text"')
        assert result == Exp.ge(Exp.string_bin("name"), Exp.string_val("text"))

    def test_bin_lt(self):
        """< int and string."""
        expected = Exp.lt(Exp.int_bin("A"), Exp.int_val(10))
        result = parse_dsl("$.A < 10")
        assert result == expected
        result = parse_dsl("$.name < 'text'")
        assert result == Exp.lt(Exp.string_bin("name"), Exp.string_val("text"))

    def test_bin_le(self):
        """<= int and string."""
        expected = Exp.le(Exp.int_bin("A"), Exp.int_val(100))
        result = parse_dsl("$.A <= 100")
        assert result == expected
        result = parse_dsl("$.name <= 'text'")
        assert result == Exp.le(Exp.string_bin("name"), Exp.string_val("text"))
        result = parse_dsl('$.name <= "text"')
        assert result == Exp.le(Exp.string_bin("name"), Exp.string_val("text"))

    def test_bin_equals(self):
        """== int, string, reversed operands."""
        expected = Exp.eq(Exp.int_bin("A"), Exp.int_val(1))
        result = parse_dsl("$.A == 1")
        assert result == expected
        result = parse_dsl("$.name == 'Alice'")
        assert result == Exp.eq(Exp.string_bin("name"), Exp.string_val("Alice"))
        result = parse_dsl("100 == $.intBin1")
        assert result == Exp.eq(Exp.int_val(100), Exp.int_bin("intBin1"))
        result = parse_dsl("'yes' == $.strBin")
        assert result == Exp.eq(Exp.string_val("yes"), Exp.string_bin("strBin"))
        result = parse_dsl('"yes" == $.strBin')
        assert result == Exp.eq(Exp.string_val("yes"), Exp.string_bin("strBin"))

    def test_bin_not_equals(self):
        """!= int and string."""
        expected = Exp.ne(Exp.int_bin("A"), Exp.int_val(1))
        result = parse_dsl("$.A != 1")
        assert result == expected
        result = parse_dsl("$.strBin != 'yes'")
        assert result == Exp.ne(Exp.string_bin("strBin"), Exp.string_val("yes"))

    def test_negative_string_bin_equals_unquoted_raises(self):
        """$.strBin == yes (unquoted) raises DslParseException."""
        with pytest.raises(DslParseException, match=r"operand|mismatched input|line \d+:\d+"):
            parse_dsl("$.strBin == yes")

    def test_reversed_operands_literal_on_left(self):
        """Literal on left: 100 < $.intBin1, 'text' < $.stringBin1."""
        result = parse_dsl("100 < $.intBin1")
        assert result == Exp.lt(Exp.int_val(100), Exp.int_bin("intBin1"))
        result = parse_dsl("'text' < $.stringBin1")
        assert result == Exp.lt(Exp.string_val("text"), Exp.string_bin("stringBin1"))
        result = parse_dsl('"text" < $.stringBin1')
        assert result == Exp.lt(Exp.string_val("text"), Exp.string_bin("stringBin1"))

    def test_float_comparison(self):
        """Test $.price > 19.99 produces correct expression."""
        expected = Exp.gt(Exp.float_bin("price"), Exp.float_val(19.99))
        result = parse_dsl("$.price > 19.99")
        assert result == expected

    def test_bool_equals_true(self):
        """Test $.active == true parses correctly."""
        expected = Exp.eq(Exp.bool_bin("active"), Exp.bool_val(True))
        result = parse_dsl("$.active == true")
        assert result == expected

    def test_bool_equals_false(self):
        """Test $.active == false parses correctly."""
        expected = Exp.eq(Exp.bool_bin("active"), Exp.bool_val(False))
        result = parse_dsl("$.active == false")
        assert result == expected
