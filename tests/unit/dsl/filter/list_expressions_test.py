"""Unit tests for list expressions in filter context."""

from aerospike_fluent import Exp, parse_dsl_with_index


class TestListExpressionsFilters:
    """Test filter generation for list expressions."""

    def test_list_expression_no_filter(self):
        """List CDT expressions do not produce secondary index Filters."""
        result = parse_dsl_with_index("$.listBin.[0] == 100")
        assert result.filter is None
        assert result.exp is not None
