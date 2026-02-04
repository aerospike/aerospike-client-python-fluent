"""Unit tests for implicit types filter generation."""

from aerospike_fluent import Exp, parse_dsl_with_index


class TestImplicitTypesFilters:
    """Test filter generation with implicit type inference."""

    def test_implicit_int_no_filter_without_index(self):
        """$.intBin1 > 100 without index returns no Filter."""
        result = parse_dsl_with_index("$.intBin1 > 100")
        assert result.filter is None
        assert result.exp == Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
