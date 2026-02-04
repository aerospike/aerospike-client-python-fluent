"""Unit tests for DSL logical expressions."""

from aerospike_fluent import Exp, parse_dsl


class TestLogicalExpressions:
    """Test logical operations. Order matches JFC LogicalExpressionsTests."""

    def test_bin_logical_and_or_combinations(self):
        """JFC: binLogicalAndOrCombinations - and/or combinations."""
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("A"), Exp.int_val(1)),
            Exp.lt(Exp.int_bin("B"), Exp.int_val(10))
        ])
        result = parse_dsl("$.A > 1 and $.B < 10")
        assert result == expected

    def test_logical_not(self):
        """JFC: logicalNot - not(...)."""
        expected = Exp.not_(Exp.eq(Exp.int_bin("A"), Exp.int_val(1)))
        result = parse_dsl("not $.A == 1")
        assert result == expected

    def test_or_expression(self):
        """JFC: (exclusive is separate in JFC) - or expression."""
        expected = Exp.or_([
            Exp.eq(Exp.int_bin("A"), Exp.int_val(1)),
            Exp.eq(Exp.int_bin("A"), Exp.int_val(2))
        ])
        result = parse_dsl("$.A == 1 or $.A == 2")
        assert result == expected
