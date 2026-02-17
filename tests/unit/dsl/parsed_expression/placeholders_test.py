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

"""Unit tests for DSL placeholder expressions. Order matches PlaceholdersTests.

"""

import pytest

from aerospike_async import CTX, ExpType, Filter, ListReturnType, MapReturnType
from aerospike_fluent import (
    DslParseException,
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_dsl,
    parse_dsl_with_index,
    PlaceholderValues,
)

NAMESPACE = "test"


class TestPlaceholders:
    """Test placeholder expressions."""

    def test_int_placeholder(self):
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        result = parse_dsl("$.intBin1 > ?0", PlaceholderValues(100))
        assert result == expected

    def test_string_placeholder(self):
        expected = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val("str"))
        result = parse_dsl("$.strBin1 > ?0", PlaceholderValues("str"))
        assert result == expected

        expected_quoted = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val("'str'"))
        result = parse_dsl("$.strBin1 > ?0", PlaceholderValues("'str'"))
        assert result == expected_quoted

        expected_double = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val('"str"'))
        result = parse_dsl("$.strBin1 > ?0", PlaceholderValues('"str"'))
        assert result == expected_double

    def test_blob_placeholder(self):
        data = bytes([1, 2, 3])
        expected = Exp.gt(
            Exp.blob_bin("blobBin1"),
            Exp.blob_val(list(data)),
        )
        result = parse_dsl("$.blobBin1.get(type: BLOB) > ?0", PlaceholderValues(data))
        assert result == expected

    def test_float_placeholder(self):
        expected = Exp.gt(Exp.float_bin("floatBin"), Exp.float_val(3.14))
        result = parse_dsl("$.floatBin > ?0", PlaceholderValues(3.14))
        assert result == expected

    def test_bool_placeholder(self):
        expected = Exp.eq(Exp.bool_bin("active"), Exp.bool_val(True))
        result = parse_dsl("$.active == ?0", PlaceholderValues(True))
        assert result == expected

    def test_multiple_placeholders(self):
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(200))
        ])
        result = parse_dsl("$.intBin1 > ?0 and $.intBin2 > ?1", PlaceholderValues(100, 200))
        assert result == expected

    def test_placeholder_in_arithmetic(self):
        expected = Exp.gt(
            Exp.num_add([Exp.int_bin("apples"), Exp.int_val(5)]),
            Exp.int_val(10)
        )
        result = parse_dsl("($.apples + ?0) > ?1", PlaceholderValues(5, 10))
        assert result == expected

    def test_placeholder_with_metadata(self):
        expected = Exp.le(Exp.ttl(), Exp.int_val(86400))
        result = parse_dsl("$.ttl() <= ?0", PlaceholderValues(86400))
        assert result == expected

    def test_placeholder_in_when(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.eq(Exp.int_bin("who"), Exp.int_val(2)),
            Exp.string_val("fred"),
            Exp.string_val("other")
        ])
        result = parse_dsl(
            "when ($.who == ?0 => ?1, $.who == ?2 => ?3, default => ?4)",
            PlaceholderValues(1, "bob", 2, "fred", "other")
        )
        assert result == expected

    def test_placeholder_of_factory(self):
        values = PlaceholderValues.of(100, "hello", 3.14)
        assert values.get(0) == 100
        assert values.get(1) == "hello"
        assert values.get(2) == 3.14

    def test_missing_placeholder_value_raises_error(self):
        with pytest.raises(DslParseException, match="Missing value for placeholder"):
            parse_dsl("$.intBin1 > ?0", PlaceholderValues())

    def test_missing_second_placeholder_raises_error(self):
        with pytest.raises(DslParseException, match="Missing value for placeholder \\?1"):
            parse_dsl("$.intBin1 > ?0 and $.intBin2 > ?1", PlaceholderValues(100))

    def test_placeholder_without_values_raises_error(self):
        with pytest.raises(DslParseException, match="no PlaceholderValues provided"):
            parse_dsl("$.intBin1 > ?0")

    def test_extra_placeholder_values_ignored(self):
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        result = parse_dsl("$.intBin1 > ?0", PlaceholderValues(100, 200))
        assert result == expected

    def test_placeholder_repr(self):
        values = PlaceholderValues(100, "hello")
        assert repr(values) == "PlaceholderValues(100, 'hello')"

    def test_int_bin_gt_has_index(self):
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_dsl_with_index("$.intBin1 > ?0", ctx, PlaceholderValues(100))
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin1", 101, 2**63 - 1))
        assert result.exp is None

    def test_int_bin_gt_and_all_indexes(self):
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_dsl_with_index(
            "$.intBin1 > ?0 and $.intBin2 > ?1",
            ctx,
            PlaceholderValues(100, 100),
        )
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin2", 101, 2**63 - 1))
        expected_exp = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_arithmetic_expression_with_index(self):
        indexes = [
            Index(bin="apples", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
            Index(bin="bananas", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_dsl_with_index(
            "($.apples + ?0) > ?1",
            ctx,
            PlaceholderValues(5, 10),
        )
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("apples", 6, 2**63 - 1))
        assert result.exp is None

    def test_map_nested_exp(self):
        expected_int = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200),
        )
        result = parse_dsl("$.mapBin1.a.bb.bcc > ?0", PlaceholderValues(200))
        assert result == expected_int
        result = parse_dsl("$.mapBin1.a.bb.bcc.get(type: INT) > ?0", PlaceholderValues(200))
        assert result == expected_int
        result = parse_dsl("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > ?0", PlaceholderValues(200))
        assert result == expected_int

        expected_str = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_dsl('$.mapBin1.a.bb.bcc == ?0', PlaceholderValues("stringVal"))
        assert result == expected_str
        result = parse_dsl('$.mapBin1.a.bb.bcc.get(type: STRING) == ?0', PlaceholderValues("stringVal"))
        assert result == expected_str
        result = parse_dsl(
            '$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == ?0',
            PlaceholderValues("stringVal"),
        )
        assert result == expected_str

    def test_nested_lists_exp_with_different_context_types(self):
        expected_rank = Exp.eq(
            Exp.list_get_by_rank(
                ListReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(-1),
                Exp.list_bin("listBin1"),
                [CTX.list_index(5)],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_dsl('$.listBin1.[5].[#-1] == ?0', PlaceholderValues("stringVal"))
        assert result == expected_rank
        result = parse_dsl('$.listBin1.[5].[#-1].get(type: STRING) == ?0', PlaceholderValues("stringVal"))
        assert result == expected_rank

        expected_value = Exp.eq(
            Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.int_val(100),
                Exp.list_bin("listBin1"),
                [CTX.list_index(5), CTX.list_rank(-1)],
            ),
            Exp.int_val(200),
        )
        result = parse_dsl("$.listBin1.[5].[#-1].[=100] == ?0", PlaceholderValues(200))
        assert result == expected_value

    def test_fourth_degree_complicated_explicit_float(self):
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.num_add([Exp.float_bin("oranges"), Exp.float_bin("acai")]),
            ]),
            Exp.float_val(10.5),
        )
        result = parse_dsl(
            "(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))"
            " + ($.oranges.get(type: FLOAT) + $.acai.get(type: FLOAT))) > ?0",
            PlaceholderValues(10.5),
        )
        assert result == expected

    def test_complicated_when_explicit_type_string(self):
        expected = Exp.eq(
            Exp.string_bin("a"),
            Exp.cond([
                Exp.eq(Exp.int_bin("b"), Exp.int_val(1)), Exp.string_bin("a1"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(2)), Exp.string_bin("a2"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(3)), Exp.string_bin("a3"),
                Exp.string_val("hello"),
            ]),
        )
        result = parse_dsl(
            "$.a.get(type: STRING) == "
            "(when($.b == ?0 => $.a1.get(type: STRING),"
            " $.b == ?1 => $.a2.get(type: STRING),"
            " $.b == ?2 => $.a3.get(type: STRING),"
            " default => ?3))",
            PlaceholderValues(1, 2, 3, "hello"),
        )
        assert result == expected

    def test_exclusive_no_indexes(self):
        expected = Exp.xor([
            Exp.eq(Exp.string_bin("hand"), Exp.string_val("stand")),
            Exp.eq(Exp.string_bin("pun"), Exp.string_val("done")),
        ])
        result = parse_dsl(
            'exclusive($.hand == ?0, $.pun == ?1)',
            PlaceholderValues("stand", "done"),
        )
        assert result == expected
