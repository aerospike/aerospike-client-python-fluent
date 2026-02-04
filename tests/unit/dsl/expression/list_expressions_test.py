"""Unit tests for DSL list expressions."""

from aerospike_async import ExpType, ListReturnType
from aerospike_fluent import Exp, parse_dsl


class TestListExpressions:
    """Test list expressions."""

    def test_list_by_index(self):
        """Test $.listBin.[0] == 100."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin"),
                [],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[0] == 100")
        assert result == expected

    def test_list_by_rank_largest(self):
        """Test $.listBin.[#-1] == 100."""
        expected = Exp.eq(
            Exp.list_get_by_rank(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(-1),
                Exp.list_bin("listBin"),
                [],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[#-1] == 100")
        assert result == expected

    def test_list_index_range(self):
        """Test $.listBin.[1:3]."""
        expected = Exp.list_get_by_index_range_count(
            ListReturnType.VALUE,
            Exp.int_val(1),
            Exp.int_val(2),
            Exp.list_bin("listBin"),
            [],
        )
        result = parse_dsl("$.listBin.[1:3]")
        assert result == expected

    def test_list_rank_range(self):
        """Test $.listBin.[#0:3]."""
        expected = Exp.list_get_by_rank_range_count(
            ListReturnType.VALUE,
            Exp.int_val(0),
            Exp.int_val(3),
            Exp.list_bin("listBin"),
            [],
        )
        result = parse_dsl("$.listBin.[#0:3]")
        assert result == expected

    def test_list_value_range(self):
        """Test $.listBin.[=111:334]."""
        expected = Exp.list_get_by_value_range(
            ListReturnType.VALUE,
            Exp.int_val(111),
            Exp.int_val(334),
            Exp.list_bin("listBin"),
            [],
        )
        result = parse_dsl("$.listBin.[=111:334]")
        assert result == expected

    def test_list_rank_range_relative(self):
        """Test $.listBin.[#-3:-1~b]."""
        expected = Exp.list_get_by_value_relative_rank_range_count(
            ListReturnType.VALUE,
            Exp.string_val("b"),
            Exp.int_val(-3),
            Exp.int_val(2),
            Exp.list_bin("listBin"),
            [],
        )
        result = parse_dsl("$.listBin.[#-3:-1~b]")
        assert result == expected


class TestInvertedListOperations:
    """Test inverted list operations."""

    def test_list_index_range_inverted(self):
        """Test $.listBin.[!2:4]."""
        result = parse_dsl("$.listBin.[!2:4]")
        assert result is not None

    def test_list_rank_range_relative_inverted(self):
        """Test $.listBin.[!#-3:-1~b]."""
        result = parse_dsl("$.listBin.[!#-3:-1~b]")
        assert result is not None
