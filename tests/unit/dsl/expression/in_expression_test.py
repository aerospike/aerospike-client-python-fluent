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

"""Unit tests for the DSL IN operator."""

import pytest
from aerospike_async import CTX, ExpType, ListReturnType, MapReturnType
from aerospike_fluent import Exp, parse_dsl
from aerospike_fluent.dsl.exceptions import DslParseException


class TestInExpression:
    """Test the IN operator: expression in expression → boolean."""

    def test_string_literal_in_bin(self):
        """'gold' in $.allowedStatuses"""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_bin("allowedStatuses"),
            [],
        )
        result = parse_dsl('"gold" in $.allowedStatuses')
        assert result == expected

    def test_bin_in_string_list(self):
        """$.name in ['Bob', 'Mary', 'Richard']"""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob", "Mary", "Richard"]),
            [],
        )
        result = parse_dsl('$.name in ["Bob", "Mary", "Richard"]')
        assert result == expected

    def test_bin_in_bin(self):
        """$.itemType in $.allowedItems"""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("itemType"),
            Exp.list_bin("allowedItems"),
            [],
        )
        result = parse_dsl("$.itemType in $.allowedItems")
        assert result == expected

    def test_nested_path_in_string_list(self):
        """$.rooms.room1.rates.rateType in ['RACK_RATE', 'DISCOUNT']"""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("rateType"),
                Exp.map_bin("rooms"),
                [CTX.map_key("room1"), CTX.map_key("rates")],
            ),
            Exp.list_val(["RACK_RATE", "DISCOUNT"]),
            [],
        )
        result = parse_dsl('$.rooms.room1.rates.rateType in ["RACK_RATE", "DISCOUNT"]')
        assert result == expected

    def test_int_literal_in_int_list(self):
        """5 in [1, 2, 3, 5, 8]"""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(5),
            Exp.list_val([1, 2, 3, 5, 8]),
            [],
        )
        result = parse_dsl("5 in [1, 2, 3, 5, 8]")
        assert result == expected

    def test_bin_in_int_list_infers_int(self):
        """$.code in [1, 2, 3] — left bin type inferred as INT from list elements."""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("code"),
            Exp.list_val([1, 2, 3]),
            [],
        )
        result = parse_dsl("$.code in [1, 2, 3]")
        assert result == expected

    def test_bin_in_float_list_infers_float(self):
        """$.score in [1.0, 2.5, 3.7] — left bin type inferred as FLOAT."""
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("score"),
            Exp.list_val([1.0, 2.5, 3.7]),
            [],
        )
        result = parse_dsl("$.score in [1.0, 2.5, 3.7]")
        assert result == expected


class TestInTypeValidation:
    """Test IN operator type validation."""

    def test_mixed_types_in_list_raises(self):
        """$.x in [1, 'two'] — mixed types in list should raise."""
        with pytest.raises(DslParseException, match="same type"):
            parse_dsl('$.x in [1, "two"]')


class TestInWithLogicalOperators:
    """Test IN operator combined with logical operators (and/or).

    IN has higher precedence than logical operators.
    """

    def test_in_with_and(self):
        """$.cost > 50 and $.status in $.allowedStatuses"""
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("cost"), Exp.int_val(50)),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("status"),
                Exp.list_bin("allowedStatuses"),
                [],
            ),
        ])
        result = parse_dsl('$.cost > 50 and $.status in $.allowedStatuses')
        assert result == expected

    def test_in_with_and_and_in(self):
        """$.cost > 50 and $.status in $.allowedStatuses and 'available' in $.bookableStates"""
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("cost"), Exp.int_val(50)),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("status"),
                Exp.list_bin("allowedStatuses"),
                [],
            ),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_val("available"),
                Exp.list_bin("bookableStates"),
                [],
            ),
        ])
        result = parse_dsl(
            '$.cost > 50 and $.status in $.allowedStatuses and "available" in $.bookableStates'
        )
        assert result == expected

    def test_in_with_or(self):
        """$.status in ['active', 'pending'] or $.override == true"""
        expected = Exp.or_([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("status"),
                Exp.list_val(["active", "pending"]),
                [],
            ),
            Exp.eq(Exp.bool_bin("override"), Exp.bool_val(True)),
        ])
        result = parse_dsl('$.status in ["active", "pending"] or $.override == true')
        assert result == expected


class TestInMapKeyHandling:
    """Test that 'in' can still be used as a map key name."""

    def test_in_as_nested_map_key(self):
        """$.mapbin.in == 'x' — unquoted 'in' as a map key inside a map bin."""
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("in"),
                Exp.map_bin("mapbin"),
                [],
            ),
            Exp.string_val("x"),
        )
        result = parse_dsl('$.mapbin.in == "x"')
        assert result == expected

    def test_quoted_in_as_nested_map_key(self):
        """$.mapbin."in" == 'x' — quoted 'in' as a map key."""
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("in"),
                Exp.map_bin("mapbin"),
                [],
            ),
            Exp.string_val("x"),
        )
        result = parse_dsl('$.mapbin."in" == "x"')
        assert result == expected
