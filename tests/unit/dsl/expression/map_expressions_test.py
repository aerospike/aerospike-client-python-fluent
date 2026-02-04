"""Unit tests for DSL map expressions."""

from aerospike_async import ExpType, MapReturnType
from aerospike_fluent import Exp, parse_dsl


class TestMapExpressions:
    """Test map expressions."""

    def test_map_by_key(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("key"),
                Exp.map_bin("mapBin"),
                [],
            ),
            Exp.int_val(200)
        )
        result = parse_dsl("$.mapBin.key == 200")
        assert result == expected

    def test_map_by_index(self):
        expected = Exp.eq(
            Exp.map_get_by_index(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin"),
                [],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.{0} == 100")
        assert result == expected

    def test_map_by_rank(self):
        expected = Exp.eq(
            Exp.map_get_by_rank(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(-1),
                Exp.map_bin("mapBin"),
                [],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.{#-1} == 100")
        assert result == expected

    def test_map_index_range(self):
        expected = Exp.map_get_by_index_range_count(
            MapReturnType.VALUE,
            Exp.int_val(1),
            Exp.int_val(2),
            Exp.map_bin("mapBin"),
            [],
        )
        result = parse_dsl("$.mapBin.{1:3}")
        assert result == expected

    def test_map_key_range(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.string_val("c"),
            Exp.map_bin("mapBin"),
            [],
        )
        result = parse_dsl("$.mapBin.{a-c}")
        assert result == expected

    def test_map_rank_range_relative(self):
        expected = Exp.map_get_by_value_relative_rank_range_count(
            MapReturnType.VALUE,
            Exp.int_val(10),
            Exp.int_val(-1),
            Exp.int_val(2),
            Exp.map_bin("mapBin"),
            [],
        )
        result = parse_dsl("$.mapBin.{#-1:1~10}")
        assert result == expected

    def test_map_index_range_relative(self):
        expected = Exp.map_get_by_key_relative_index_range_count(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.int_val(0),
            Exp.int_val(1),
            Exp.map_bin("mapBin"),
            [],
        )
        result = parse_dsl("$.mapBin.{0:1~a}")
        assert result == expected


class TestInvertedMapOperations:
    """Test inverted map operations."""

    def test_map_key_range_inverted(self):
        result = parse_dsl("$.mapBin.{!a-c}")
        assert result is not None

    def test_map_rank_range_relative_inverted(self):
        result = parse_dsl("$.mapBin.{!#-1:1~10}")
        assert result is not None

    def test_map_index_range_relative_inverted(self):
        result = parse_dsl("$.mapBin.{!0:1~a}")
        assert result is not None
