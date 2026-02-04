"""Unit tests for DSL CTX parsing.

One test per scenario (test_<category>_1, test_<category>_2, ...) for
clear failure isolation and consistent structure.

CTX comparison uses to_tuple() (value-based) so tests pass regardless
of module load (e.g. IDE vs command line). Ctx id values match aerospike-core.
"""

import io
import contextlib
import pytest

from aerospike_fluent import DslParseException, parse_ctx

# Path -> list of (CTX static method name, args) to build expected from result's class.
PATH_CTX_BUILDERS = {
    "$.listBin.[0]": [("list_index", (0,))],
    "$.listBin.[=100]": [("list_value", (100,))],
    "$.listBin.[#-1]": [("list_rank", (-1,))],
    "$.listBin.[0].[1]": [("list_index", (0,)), ("list_index", (1,))],
    "$.listBin.[0].[1].[2]": [("list_index", (0,)), ("list_index", (1,)), ("list_index", (2,))],
    "$.mapBin.a": [("map_key", ("a",))],
    "$.mapBin.{0}": [("map_index", (0,))],
    "$.mapBin.{#-1}": [("map_rank", (-1,))],
    "$.mapBin.{=100}": [("map_value", (100,))],
    "$.mapBin.a.bb": [("map_key", ("a",)), ("map_key", ("bb",))],
    "$.mapBin.{0}.a.{#-1}": [("map_index", (0,)), ("map_key", ("a",)), ("map_rank", (-1,))],
    "$.mapBin.{0}.a.{#-1}.[=100]": [
        ("map_index", (0,)), ("map_key", ("a",)), ("map_rank", (-1,)), ("list_value", (100,)),
    ],
    "$.listBin.[0].[=100].a.{0}": [
        ("list_index", (0,)), ("list_value", (100,)), ("map_key", ("a",)), ("map_index", (0,)),
    ],
}

# CtxType ids from aerospike-core cdt_context.rs (for expected value comparison)
_LIST_INDEX = 0x10
_LIST_RANK = 0x11
_LIST_VALUE = 0x13
_MAP_INDEX = 0x20
_MAP_RANK = 0x21
_MAP_KEY = 0x22
_MAP_VALUE = 0x23

# Expected (ctx_id, ctx_value) for each path; comparison is value-based.
EXPECTED_CTX_TUPLES = {
    "$.listBin.[0]": [(_LIST_INDEX, 0)],
    "$.listBin.[=100]": [(_LIST_VALUE, 100)],
    "$.listBin.[#-1]": [(_LIST_RANK, -1)],
    "$.listBin.[0].[1]": [(_LIST_INDEX, 0), (_LIST_INDEX, 1)],
    "$.listBin.[0].[1].[2]": [(_LIST_INDEX, 0), (_LIST_INDEX, 1), (_LIST_INDEX, 2)],
    "$.mapBin.a": [(_MAP_KEY, "a")],
    "$.mapBin.{0}": [(_MAP_INDEX, 0)],
    "$.mapBin.{#-1}": [(_MAP_RANK, -1)],
    "$.mapBin.{=100}": [(_MAP_VALUE, 100)],
    "$.mapBin.a.bb": [(_MAP_KEY, "a"), (_MAP_KEY, "bb")],
    "$.mapBin.{0}.a.{#-1}": [(_MAP_INDEX, 0), (_MAP_KEY, "a"), (_MAP_RANK, -1)],
    "$.mapBin.{0}.a.{#-1}.[=100]": [
        (_MAP_INDEX, 0),
        (_MAP_KEY, "a"),
        (_MAP_RANK, -1),
        (_LIST_VALUE, 100),
    ],
    "$.listBin.[0].[=100].a.{0}": [
        (_LIST_INDEX, 0),
        (_LIST_VALUE, 100),
        (_MAP_KEY, "a"),
        (_MAP_INDEX, 0),
    ],
}


def _assert_ctx_list_matches(path, result):
    """Compare result to expected: use to_tuple when available, else build expected from result's class."""
    if result and getattr(result[0], "to_tuple", None) is not None:
        tuples = [ctx.to_tuple() for ctx in result]
        assert tuples == EXPECTED_CTX_TUPLES[path]
    else:
        cls = result[0].__class__
        expected = [getattr(cls, name)(*args) for name, args in PATH_CTX_BUILDERS[path]]
        assert result == expected


def _parse_ctx_quiet(path: str):
    """Parse path and suppress parser/lexer stderr (e.g. ANTLR errors)."""
    with contextlib.redirect_stderr(io.StringIO()):
        return parse_ctx(path)


class TestCtxParsing:
    """Test CTX parsing from DSL paths."""

    def test_list_expression_only_bin_no_ctx_1(self):
        """Path with list bin but no CDT access raises."""
        with pytest.raises(DslParseException, match="CDT context is not provided"):
            parse_ctx("$.listBin")

    def test_empty_or_malformed_1(self):
        """Null path raises."""
        with pytest.raises(DslParseException, match="Path must not be null or empty"):
            parse_ctx(None)

    def test_empty_or_malformed_2(self):
        """Empty path raises."""
        with pytest.raises(DslParseException, match="Path must not be null or empty"):
            parse_ctx("")

    def test_malformed_path_syntax_1(self):
        """Double dot in path raises."""
        with pytest.raises(DslParseException, match="Could not parse"):
            _parse_ctx_quiet("$..listBin1")

    def test_malformed_path_syntax_2(self):
        """Missing dot after $ raises."""
        with pytest.raises(DslParseException, match="Could not parse"):
            _parse_ctx_quiet("$listBin1")

    def test_list_expression_one_level_1(self):
        """List index: $.listBin.[0] produces CTX.list_index(0)."""
        path = "$.listBin.[0]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_list_expression_one_level_2(self):
        """List value: $.listBin.[=100] produces CTX.list_value(100)."""
        path = "$.listBin.[=100]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_list_expression_one_level_3(self):
        """List rank: $.listBin.[#-1] produces CTX.list_rank(-1)."""
        path = "$.listBin.[#-1]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_list_expression_two_levels_1(self):
        """Two list indices: $.listBin.[0].[1] produces 2 CTX."""
        path = "$.listBin.[0].[1]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_list_expression_three_levels_1(self):
        """Three list indices: $.listBin.[0].[1].[2] produces 3 CTX."""
        path = "$.listBin.[0].[1].[2]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_only_bin_no_ctx_1(self):
        """Path with map bin but no CDT access raises."""
        with pytest.raises(DslParseException, match="CDT context is not provided"):
            parse_ctx("$.mapBin")

    def test_map_expression_one_level_1(self):
        """Map key: $.mapBin.a produces CTX.map_key('a')."""
        path = "$.mapBin.a"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_one_level_2(self):
        """Map index: $.mapBin.{0} produces CTX.map_index(0)."""
        path = "$.mapBin.{0}"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_one_level_3(self):
        """Map rank: $.mapBin.{#-1} produces CTX.map_rank(-1)."""
        path = "$.mapBin.{#-1}"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_one_level_4(self):
        """Map value: $.mapBin.{=100} produces CTX.map_value(100)."""
        path = "$.mapBin.{=100}"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_two_levels_1(self):
        """Two map keys: $.mapBin.a.bb produces 2 CTX."""
        path = "$.mapBin.a.bb"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_map_expression_three_levels_1(self):
        """Map index, key, rank: $.mapBin.{0}.a.{#-1} produces 3 CTX."""
        path = "$.mapBin.{0}.a.{#-1}"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_combined_list_map_1(self):
        """Map then list: $.mapBin.{0}.a.{#-1}.[=100] produces 4 CTX."""
        path = "$.mapBin.{0}.a.{#-1}.[=100]"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_combined_list_map_2(self):
        """List then map: $.listBin.[0].[=100].a.{0} produces 4 CTX."""
        path = "$.listBin.[0].[=100].a.{0}"
        _assert_ctx_list_matches(path, parse_ctx(path))

    def test_path_function_raises_1(self):
        """Path with get() raises."""
        with pytest.raises(DslParseException, match="Path function is unsupported"):
            parse_ctx("$.listBin.[0].get(type: INT)")

    def test_path_function_raises_2(self):
        """Path with asInt() raises."""
        with pytest.raises(DslParseException, match="Path function is unsupported"):
            parse_ctx("$.mapBin.a.asInt()")

    def test_full_expression_raises_1(self):
        """Path with comparison (==) raises."""
        with pytest.raises(DslParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.listBin.[0] == 100")

    def test_full_expression_raises_2(self):
        """Path with comparison (>) raises."""
        with pytest.raises(DslParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin.a > 50")
