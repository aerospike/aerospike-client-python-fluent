"""Unit tests for DSL map and list combined expressions."""

from aerospike_async import CTX, ExpType, ListReturnType, MapReturnType
from aerospike_fluent import Exp, parse_dsl


class TestNestedExpressions:
    """Test nested CDT expressions."""

    def test_nested_list_access(self):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin"),
                [CTX.list_index(0)],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[0].[0] == 100")
        assert result == expected

    def test_nested_map_access(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("ccc"),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200)
        )
        result = parse_dsl("$.mapBin.a.bb.ccc == 200")
        assert result == expected


class TestMapAndListExpressions:
    """Test complex nested CDT expressions."""

    def test_list_inside_map(self):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a")],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.a.[0] == 100")
        assert result == expected

    def test_map_inside_list(self):
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.list_bin("listBin"),
                [CTX.list_index(2)],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[2].cc > 100")
        assert result == expected

    def test_map_list_list(self):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a"), CTX.list_index(0)],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.a.[0].[0] == 100")
        assert result == expected

    def test_list_map_map(self):
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.list_bin("listBin"),
                [CTX.list_index(2), CTX.map_key("aa")],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[2].aa.cc > 100")
        assert result == expected

    def test_list_map_list(self):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin"),
                [CTX.list_index(1), CTX.map_key("a")],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[1].a.[0] == 100")
        assert result == expected

    def test_deep_nesting(self):
        expected = Exp.gt(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(2),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a"), CTX.map_key("cc")],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.a.cc.[2] > 100")
        assert result == expected

    def test_map_list_map(self):
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a"), CTX.list_index(0)],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.mapBin.a.[0].cc > 100")
        assert result == expected

    def test_list_map_list_size(self):
        expected = Exp.eq(
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.LIST,
                    Exp.int_val(0),
                    Exp.list_bin("listBin"),
                    [CTX.list_index(1), CTX.map_key("a")],
                ),
                [],
            ),
            Exp.int_val(100)
        )
        result = parse_dsl("$.listBin.[1].a.[0].count() == 100")
        assert result == expected
