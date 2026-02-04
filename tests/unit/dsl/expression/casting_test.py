"""Unit tests for DSL casting expressions."""

from aerospike_fluent import Exp, parse_dsl


class TestCasting:
    """Test casting expressions."""

    def test_float_to_int_cast(self):
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_bin("floatBin1"))
        result = parse_dsl("$.intBin1 > $.floatBin1.asInt()")
        assert result == expected

    def test_int_to_float_cast(self):
        expected = Exp.gt(Exp.float_bin("floatBin1"), Exp.float_bin("intBin1"))
        result = parse_dsl("$.floatBin1 > $.intBin1.asFloat()")
        assert result == expected
