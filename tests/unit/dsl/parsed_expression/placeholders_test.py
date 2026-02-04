"""Unit tests for DSL placeholder expressions."""

import pytest
from aerospike_fluent import (
    DslParseException,
    Exp,
    parse_dsl,
    PlaceholderValues,
)


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
