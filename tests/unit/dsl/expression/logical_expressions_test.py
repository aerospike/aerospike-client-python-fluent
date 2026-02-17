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

"""Unit tests for DSL logical expressions."""

import pytest
from aerospike_fluent import DslParseException, Exp, parse_dsl


class TestLogicalExpressions:
    """Test logical operations."""

    def test_bin_logical_and_or_combinations(self):
        """and/or combinations and precedence; parentheses change grouping."""
        expected1 = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        result = parse_dsl("$.intBin1 > 100 and $.intBin2 > 100")
        assert result == expected1

        expected2 = Exp.or_([
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
            Exp.lt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        result = parse_dsl("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100")
        assert result == expected2
        result = parse_dsl("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100")
        assert result == expected2
        result = parse_dsl("(($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100)")
        assert result == expected2

        expected3 = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.lt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            ]),
        ])
        result = parse_dsl("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))")
        assert result == expected3
        result = parse_dsl("$.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100)")
        assert result == expected3

        # Different grouping produces different expression; parsed result must not equal expected2
        result_wrong_grouping = parse_dsl(
            "($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))"
        )
        assert result_wrong_grouping == expected3
        assert result_wrong_grouping != expected2

    def test_logical_not(self):
        """not(expr) with keyExists() and with comparison."""
        result = parse_dsl("not($.keyExists())")
        assert result == Exp.not_(Exp.key_exists())

        expected = Exp.not_(Exp.eq(Exp.int_bin("A"), Exp.int_val(1)))
        result = parse_dsl("not ($.A == 1)")
        assert result == expected

    def test_bin_logical_exclusive(self):
        """exclusive(expr1, expr2, ...) with 2 and 4 expressions."""
        result = parse_dsl('exclusive($.hand == "hook", $.leg == "peg")')
        expected = Exp.xor([
            Exp.eq(Exp.string_bin("hand"), Exp.string_val("hook")),
            Exp.eq(Exp.string_bin("leg"), Exp.string_val("peg")),
        ])
        assert result == expected
        result = parse_dsl("exclusive($.hand == 'hook', $.leg == 'peg')")
        assert result == expected

        result = parse_dsl(
            'exclusive($.a == "aVal", $.b == "bVal", $.c == "cVal", $.d == 4)'
        )
        expected_multi = Exp.xor([
            Exp.xor([
                Exp.xor([
                    Exp.eq(Exp.string_bin("a"), Exp.string_val("aVal")),
                    Exp.eq(Exp.string_bin("b"), Exp.string_val("bVal")),
                ]),
                Exp.eq(Exp.string_bin("c"), Exp.string_val("cVal")),
            ]),
            Exp.eq(Exp.int_bin("d"), Exp.int_val(4)),
        ])
        assert result == expected_multi

    def test_flat_hierarchy_and(self):
        """Four-way and: a and b and c and d."""
        result = parse_dsl(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 < 100 and $.intBin4 < 100"
        )
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.lt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.lt(Exp.int_bin("intBin4"), Exp.int_val(100)),
        ])
        assert result == expected

    def test_or_expression(self):
        """Or of two comparisons."""
        expected = Exp.or_([
            Exp.eq(Exp.int_bin("A"), Exp.int_val(1)),
            Exp.eq(Exp.int_bin("A"), Exp.int_val(2)),
        ])
        result = parse_dsl("$.A == 1 or $.A == 2")
        assert result == expected

    def test_negative_syntax_logical_operators(self):
        """Malformed logical expressions raise DslParseException."""
        with pytest.raises(DslParseException, match=r"Could not parse|parse|line \d+:\d+|mismatched|extraneous"):
            parse_dsl("($.intBin1 > 100 and ($.intBin2 > 100) or)")

        with pytest.raises(DslParseException, match=r"Could not parse|parse|line \d+:\d+|mismatched|extraneous"):
            parse_dsl("and ($.intBin1 > 100 and ($.intBin2 > 100))")

        with pytest.raises(DslParseException, match=r"Could not parse|parse|line \d+:\d+|mismatched|extraneous"):
            parse_dsl("($.intBin1 > 100 and ($.intBin2 > 100) not)")

        with pytest.raises(DslParseException, match=r"Could not parse|parse|line \d+:\d+|mismatched|extraneous"):
            parse_dsl("($.intBin1 > 100 and ($.intBin2 > 100) exclusive)")

    def test_negative_bin_logical_exclusive_with_one_param(self):
        """exclusive() with one expression raises DslParseException (grammar or visitor)."""
        with pytest.raises(
            DslParseException,
            match=r"at least 2 expressions|mismatched input.*expecting ','",
        ):
            parse_dsl('exclusive($.hand == "hook")')
