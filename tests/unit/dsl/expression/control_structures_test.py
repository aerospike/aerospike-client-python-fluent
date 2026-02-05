"""Unit tests for DSL control structures (when/with)."""

from aerospike_fluent import Exp, parse_dsl


class TestWhenExpressions:
    """Test when (conditional) expressions."""

    def test_when_single_condition(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_dsl('when ($.who == 1 => "bob", default => "other")')
        assert result == expected

    def test_when_single_condition_no_spaces(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_dsl('when($.who == 1 => "bob", default => "other")')
        assert result == expected

    def test_when_single_quotes(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_dsl("when($.who == 1 => 'bob', default => 'other')")
        assert result == expected

    def test_when_using_the_result_string_bin_equals_when(self):
        """String bin equals when expression: $.stringBin1.get(type: STRING) == (when (...))."""
        expected = Exp.eq(
            Exp.string_bin("stringBin1"),
            Exp.cond([
                Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
                Exp.string_val("bob"),
                Exp.string_val("other")
            ])
        )
        result = parse_dsl(
            '$.stringBin1.get(type: STRING) == (when ($.who == 1 => "bob", default => "other"))'
        )
        assert result == expected

    def test_when_using_the_result_numeric_in_comparison(self):
        """When expression in comparison: (when (...) => 10, 20) > 15."""
        expected = Exp.gt(
            Exp.cond([
                Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
                Exp.int_val(10),
                Exp.int_val(20)
            ]),
            Exp.int_val(15)
        )
        result = parse_dsl('(when($.who == 1 => 10, default => 20)) > 15')
        assert result == expected

    def test_when_multiple_conditions(self):
        """Multiple condition/result pairs plus default."""
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.eq(Exp.int_bin("who"), Exp.int_val(2)),
            Exp.string_val("fred"),
            Exp.string_val("other")
        ])
        result = parse_dsl('when ($.who == 1 => "bob", $.who == 2 => "fred", default => "other")')
        assert result == expected

    def test_when_numeric_actions(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("category"), Exp.int_val(1)),
            Exp.int_val(100),
            Exp.eq(Exp.int_bin("category"), Exp.int_val(2)),
            Exp.int_val(200),
            Exp.int_val(0)
        ])
        result = parse_dsl("when ($.category == 1 => 100, $.category == 2 => 200, default => 0)")
        assert result == expected

class TestWithExpressions:
    """Test with/do (variable binding) expressions."""

    def test_with_single_variable(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.num_add([Exp.var("x"), Exp.int_val(1)])
        ])
        result = parse_dsl('with ("x" = 1) do (${x} + 1)')
        assert result == expected

    def test_with_multiple_variables(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.def_("y", Exp.num_add([Exp.var("x"), Exp.int_val(1)])),
            Exp.num_add([Exp.var("x"), Exp.var("y")])
        ])
        result = parse_dsl('with ("x" = 1, "y" = ${x} + 1) do (${x} + ${y})')
        assert result == expected

    def test_with_no_spaces(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.def_("y", Exp.num_add([Exp.var("x"), Exp.int_val(1)])),
            Exp.num_add([Exp.var("x"), Exp.var("y")])
        ])
        result = parse_dsl('with("x" = 1, "y" = ${x}+1) do(${x}+${y})')
        assert result == expected

    def test_with_bin_reference(self):
        expected = Exp.exp_let([
            Exp.def_("total", Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")])),
            Exp.num_mul([Exp.var("total"), Exp.int_val(2)])
        ])
        result = parse_dsl('with ("total" = $.a + $.b) do (${total} * 2)')
        assert result == expected

    def test_with_in_comparison(self):
        expected = Exp.gt(
            Exp.exp_let([
                Exp.def_("sum", Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")])),
                Exp.var("sum")
            ]),
            Exp.int_val(100)
        )
        result = parse_dsl('(with ("sum" = $.a + $.b) do (${sum})) > 100')
        assert result == expected

    def test_variable_reference_syntax(self):
        expected = Exp.exp_let([
            Exp.def_("myVar", Exp.int_val(42)),
            Exp.var("myVar")
        ])
        result = parse_dsl('with ("myVar" = 42) do (${myVar})')
        assert result == expected
