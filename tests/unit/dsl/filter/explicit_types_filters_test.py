"""Unit tests for explicit types filter generation."""

import pytest

from aerospike_async import Filter
from aerospike_fluent import (
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_dsl_with_index,
)

MAX = 2**63 - 1


def _index_ctx():
    """Index context with intBin1 (numeric) and stringBin1 (string). JFC INDEX_FILTER_INPUT."""
    indexes = [
        Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=1),
        Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace="test", bin_values_ratio=1),
    ]
    return IndexContext.of("test", indexes)


def _assert_range_filter(result, bin_name: str, range_min: int, range_max: int) -> None:
    """Assert result.filter equals Filter.range(bin_name, range_min, range_max) by value."""
    assert result.filter is not None
    expected = Filter.range(bin_name, range_min, range_max)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


def _assert_equal_filter(result, bin_name: str, value) -> None:
    """Assert result.filter equals Filter.equal(bin_name, value) by value."""
    assert result.filter is not None
    expected = Filter.equal(bin_name, value)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


class TestExplicitTypesFilters:
    """Test filter generation with explicit type expressions."""

    def test_explicit_int_parsed(self):
        """$.intBin1.get(type: INT) > 5 parses to correct Exp."""
        result = parse_dsl_with_index("$.intBin1.get(type: INT) > 5")
        assert result.filter is None
        assert result.exp == Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(5))


class TestIntegerComparison:
    """JFC integerComparison() — explicit type INT with range filter."""

    def test_integer_comparison_no_index(self):
        """Namespace and indexes must be given to create a Filter; no index context → filter None."""
        result = parse_dsl_with_index("$.intBin1.get(type: INT) > 5")
        assert result.filter is None
        assert result.exp is not None

    def test_integer_comparison_with_index(self):
        """$.intBin1.get(type: INT) > 5 with index context → range(intBin1, 6, MAX)."""
        result = parse_dsl_with_index("$.intBin1.get(type: INT) > 5", _index_ctx())
        _assert_range_filter(result, "intBin1", 6, MAX)
        assert result.exp is None

    def test_integer_comparison_value_op_bin(self):
        """5 < $.intBin1.get(type: INT) with index context → range(intBin1, 6, MAX)."""
        result = parse_dsl_with_index("5 < $.intBin1.get(type: INT)", _index_ctx())
        _assert_range_filter(result, "intBin1", 6, MAX)
        assert result.exp is None


class TestStringComparison:
    """JFC stringComparison() — explicit type STRING with equal filter."""

    def test_string_comparison_double_quotes(self):
        """$.stringBin1.get(type: STRING) == \"yes\" with index context → equal(stringBin1, 'yes')."""
        result = parse_dsl_with_index('$.stringBin1.get(type: STRING) == "yes"', _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_single_quotes(self):
        """$.stringBin1.get(type: STRING) == 'yes' with index context → equal(stringBin1, 'yes')."""
        result = parse_dsl_with_index("$.stringBin1.get(type: STRING) == 'yes'", _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_value_op_bin_double_quotes(self):
        """\"yes\" == $.stringBin1.get(type: STRING) with index context → equal(stringBin1, 'yes')."""
        result = parse_dsl_with_index('"yes" == $.stringBin1.get(type: STRING)', _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_value_op_bin_single_quotes(self):
        """'yes' == $.stringBin1.get(type: STRING) with index context → equal(stringBin1, 'yes')."""
        result = parse_dsl_with_index("'yes' == $.stringBin1.get(type: STRING)", _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None
