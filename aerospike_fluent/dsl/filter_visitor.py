# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Filter tree visitor: builds ExpressionNode tree from ANTLR parse tree.

Used by FilterGenerator so filter selection uses a single parse (no hand-written parser).
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

from aerospike_async import CTX
from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser
from aerospike_fluent.dsl.antlr4.generated.ConditionVisitor import ConditionVisitor
from aerospike_fluent.dsl.exceptions import DslParseException
from aerospike_fluent.dsl.filter_gen import (
    ExpressionNode,
    IndexTypeEnum,
    OperationType,
)


@dataclass
class _BinRef:
    """Simple bin reference: $.binName or $.binName.[5].get(type: X)."""
    bin_name: str
    explicit_type: Optional[str] = None
    ctx: Optional[List[CTX]] = None


@dataclass
class _Constant:
    """Simple constant value for filter comparison."""
    value: Any
    value_type: Optional[IndexTypeEnum] = None


_SimpleOperand = Union[_BinRef, _Constant]


def _unquote(s: str) -> str:
    """Remove surrounding single or double quotes."""
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    return s


class FilterTreeVisitor(ConditionVisitor):
    """Builds ExpressionNode tree from parse tree for filter generation.

    Only handles AND, OR, NOT, and simple comparisons (bin op constant / constant op bin).
    Returns None for unsupported expressions (exclusive, with, when, arithmetic, etc.).
    When placeholder_values is provided, placeholder operands (?0, ?1, ...) are resolved
    to constants so filter generation can use them.
    """

    def __init__(self, dsl_string: str, placeholder_values: Optional[Any] = None):
        self._dsl = dsl_string
        self._placeholder_values = placeholder_values

    def visitParse(self, ctx: ConditionParser.ParseContext) -> Optional[ExpressionNode]:
        return self.visit(ctx.expression()) if ctx.expression() else None

    def visitExpression(self, ctx: ConditionParser.ExpressionContext) -> Optional[ExpressionNode]:
        return self.visit(ctx.logicalOrExpression()) if ctx.logicalOrExpression() else None

    def visitOrExpression(self, ctx: ConditionParser.OrExpressionContext) -> Optional[ExpressionNode]:
        and_list = ctx.logicalAndExpression()
        if not and_list:
            return None
        nodes: List[ExpressionNode] = []
        for and_ctx in and_list:
            child = self.visit(and_ctx)
            if child is None:
                return None
            nodes.append(child)
        if len(nodes) == 1:
            return nodes[0]
        result = nodes[-1]
        for i in range(len(nodes) - 2, -1, -1):
            result = ExpressionNode(op=OperationType.OR, left=nodes[i], right=result)
        return result

    def visitAndExpression(self, ctx: ConditionParser.AndExpressionContext) -> Optional[ExpressionNode]:
        basic_list = ctx.basicExpression()
        if not basic_list:
            return None
        nodes: List[ExpressionNode] = []
        for basic_ctx in basic_list:
            child = self.visit(basic_ctx)
            if child is None:
                return None
            nodes.append(child)
        if len(nodes) == 1:
            return nodes[0]
        result = nodes[-1]
        for i in range(len(nodes) - 2, -1, -1):
            result = ExpressionNode(op=OperationType.AND, left=nodes[i], right=result)
        return result

    def visitNotExpression(self, ctx: ConditionParser.NotExpressionContext) -> Optional[ExpressionNode]:
        if not ctx.expression():
            return None
        child = self.visit(ctx.expression())
        if child is None:
            return None
        return ExpressionNode(op=OperationType.NOT, left=child)

    def visitExclusiveExpression(self, ctx: ConditionParser.ExclusiveExpressionContext) -> Optional[ExpressionNode]:
        return None

    def visitWithExpression(self, ctx: ConditionParser.WithExpressionContext) -> Optional[ExpressionNode]:
        return None

    def visitWhenExpression(self, ctx: ConditionParser.WhenExpressionContext) -> Optional[ExpressionNode]:
        return None

    def visitComparisonExpressionWrapper(
        self, ctx: ConditionParser.ComparisonExpressionWrapperContext
    ) -> Optional[ExpressionNode]:
        return self.visit(ctx.comparisonExpression()) if ctx.comparisonExpression() else None

    def visitEqualityExpression(self, ctx: ConditionParser.EqualityExpressionContext) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.EQ)

    def visitInequalityExpression(self, ctx: ConditionParser.InequalityExpressionContext) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.NE)

    def visitGreaterThanExpression(self, ctx: ConditionParser.GreaterThanExpressionContext) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.GT)

    def visitGreaterThanOrEqualExpression(
        self, ctx: ConditionParser.GreaterThanOrEqualExpressionContext
    ) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.GE)

    def visitLessThanExpression(self, ctx: ConditionParser.LessThanExpressionContext) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.LT)

    def visitLessThanOrEqualExpression(
        self, ctx: ConditionParser.LessThanOrEqualExpressionContext
    ) -> Optional[ExpressionNode]:
        return self._visit_comparison(ctx, OperationType.LE)

    def visitAdditiveExpressionWrapper(
        self, ctx: ConditionParser.AdditiveExpressionWrapperContext
    ) -> Optional[ExpressionNode]:
        # Delegate to child so parenthesized expressions (e.g. (a or b)) are visited
        return self.visitChildren(ctx)

    def visitOperand(self, ctx: ConditionParser.OperandContext) -> Optional[ExpressionNode]:
        # Parenthesized expression: ( expr ) -> return the inner expression tree
        if ctx.expression() is not None:
            return self.visit(ctx.expression())
        return None

    def _visit_comparison(
        self,
        ctx: Any,
        op: OperationType,
    ) -> Optional[ExpressionNode]:
        add_left = ctx.additiveExpression(0)
        add_right = ctx.additiveExpression(1)
        left_op = self._try_simple_operand(add_left)
        right_op = self._try_simple_operand(add_right)
        if left_op is not None and right_op is not None:
            bin_ref: Optional[_BinRef] = None
            const: Optional[_Constant] = None
            comp_op = op
            if isinstance(left_op, _BinRef) and isinstance(right_op, _Constant):
                bin_ref, const = left_op, right_op
            elif isinstance(left_op, _Constant) and isinstance(right_op, _BinRef):
                bin_ref, const = right_op, left_op
                comp_op = self._flip_op(op)
            if bin_ref is not None and const is not None:
                dsl_fragment = ctx.getText()
                return ExpressionNode(
                    op=comp_op,
                    bin_name=bin_ref.bin_name,
                    value=const.value,
                    value_type=const.value_type,
                    bin_explicit_type=bin_ref.explicit_type,
                    ctx=bin_ref.ctx,
                    dsl_fragment=dsl_fragment,
                )
        if op == OperationType.NE:
            return None
        left_arith = self._try_arithmetic_operand(add_left)
        right_simple = self._try_simple_operand(add_right)
        if left_arith is not None and right_simple is not None and isinstance(right_simple, _Constant):
            if isinstance(right_simple.value, int) and not isinstance(right_simple.value, bool):
                bin_ref, arith_op, arith_const, bin_on_left = left_arith
                return ExpressionNode(
                    op=op,
                    bin_name=bin_ref.bin_name,
                    value=right_simple.value,
                    value_type=IndexTypeEnum.NUMERIC,
                    bin_explicit_type=bin_ref.explicit_type,
                    ctx=bin_ref.ctx,
                    dsl_fragment=ctx.getText(),
                    arith_op=arith_op,
                    arith_constant=arith_const,
                    bin_on_left=bin_on_left,
                )
        right_arith = self._try_arithmetic_operand(add_right)
        left_simple = self._try_simple_operand(add_left)
        if right_arith is not None and left_simple is not None and isinstance(left_simple, _Constant):
            if isinstance(left_simple.value, int) and not isinstance(left_simple.value, bool):
                bin_ref, arith_op, arith_const, bin_on_left = right_arith
                comp_op = self._flip_op(op)
                return ExpressionNode(
                    op=comp_op,
                    bin_name=bin_ref.bin_name,
                    value=left_simple.value,
                    value_type=IndexTypeEnum.NUMERIC,
                    bin_explicit_type=bin_ref.explicit_type,
                    ctx=bin_ref.ctx,
                    dsl_fragment=ctx.getText(),
                    arith_op=arith_op,
                    arith_constant=arith_const,
                    bin_on_left=bin_on_left,
                )
        return None

    def _flip_op(self, op: OperationType) -> OperationType:
        if op == OperationType.LT:
            return OperationType.GT
        if op == OperationType.LE:
            return OperationType.GE
        if op == OperationType.GT:
            return OperationType.LT
        if op == OperationType.GE:
            return OperationType.LE
        return op

    def _get_operand_from_shift(self, shift_ctx: Any) -> Optional[_SimpleOperand]:
        if shift_ctx.getChildCount() != 1:
            return None
        return self._try_simple_operand_from_operand(shift_ctx.getChild(0))

    def _get_operand_from_bitwise(self, bitwise_ctx: Any) -> Optional[_SimpleOperand]:
        if bitwise_ctx.getChildCount() != 1:
            return None
        return self._get_operand_from_shift(bitwise_ctx.getChild(0))

    def _get_operand_from_multiplicative(self, mult_ctx: Any) -> Optional[_SimpleOperand]:
        if mult_ctx.getChildCount() != 1:
            return None
        return self._get_operand_from_bitwise(mult_ctx.getChild(0))

    def _get_operand_from_additive(self, add_ctx: Any) -> Optional[_SimpleOperand]:
        if add_ctx.getChildCount() != 1:
            return None
        return self._get_operand_from_multiplicative(add_ctx.getChild(0))

    def _unwrap_parenthesized_additive(self, add_ctx: Any) -> Optional[Any]:
        """If add_ctx is ( expr ) where expr is additive, return the inner additiveExpression context."""
        if add_ctx.getChildCount() != 1:
            return None
        mult_ctx = add_ctx.getChild(0)
        if mult_ctx.getChildCount() != 1:
            return None
        bitwise_ctx = mult_ctx.getChild(0)
        if bitwise_ctx.getChildCount() != 1:
            return None
        shift_ctx = bitwise_ctx.getChild(0)
        if shift_ctx.getChildCount() != 1:
            return None
        oper_ctx = shift_ctx.getChild(0)
        try:
            inner_expr = oper_ctx.expression()
        except AttributeError:
            return None
        if inner_expr is None:
            return None
        try:
            lor = inner_expr.logicalOrExpression()
            land = lor.logicalAndExpression(0)
            basic = land.basicExpression(0)
            comp = basic.comparisonExpression()
            if comp.getChildCount() >= 1:
                return comp.getChild(0)
            return None
        except (AttributeError, IndexError, TypeError) as e:
            return None

    def _try_arithmetic_operand(self, add_ctx: Any) -> Optional[Tuple[_BinRef, str, int, bool]]:
        """If additiveExpression is (bin op int) or (int op bin) with op in +-*/, return (bin_ref, op, int_const, bin_on_left)."""
        inner = self._unwrap_parenthesized_additive(add_ctx)
        if inner is not None:
            return self._try_arithmetic_operand(inner)
        if add_ctx.getChildCount() == 3:
            op_text = add_ctx.getChild(1).getText()
            if op_text in "+-":
                left = self._get_operand_from_additive(add_ctx.getChild(0))
                right = self._get_operand_from_multiplicative(add_ctx.getChild(2))
                if left is not None and right is not None:
                    if isinstance(left, _BinRef) and isinstance(right, _Constant) and isinstance(right.value, int) and not isinstance(right.value, bool):
                        return (left, op_text, int(right.value), True)
                    if isinstance(right, _BinRef) and isinstance(left, _Constant) and isinstance(left.value, int) and not isinstance(left.value, bool):
                        return (right, op_text, int(left.value), False)
        if add_ctx.getChildCount() == 1:
            mult_ctx = add_ctx.getChild(0)
            if mult_ctx.getChildCount() == 3:
                op_text = mult_ctx.getChild(1).getText()
                if op_text in "*/":
                    left = self._get_operand_from_multiplicative(mult_ctx.getChild(0))
                    right = self._get_operand_from_bitwise(mult_ctx.getChild(2))
                    if left is not None and right is not None:
                        if isinstance(left, _BinRef) and isinstance(right, _Constant) and isinstance(right.value, int) and not isinstance(right.value, bool):
                            return (left, op_text, int(right.value), True)
                        if isinstance(right, _BinRef) and isinstance(left, _Constant) and isinstance(left.value, int) and not isinstance(left.value, bool):
                            return (right, op_text, int(left.value), False)
        return None

    def _try_simple_operand(self, add_ctx: Any) -> Optional[_SimpleOperand]:
        """If additiveExpression is a single operand (no + - * / etc), return BinRef or Constant."""
        if add_ctx.getChildCount() != 1:
            return None
        mult_ctx = add_ctx.getChild(0)
        if mult_ctx.getChildCount() != 1:
            return None
        bitwise_ctx = mult_ctx.getChild(0)
        if bitwise_ctx.getChildCount() != 1:
            return None
        shift_ctx = bitwise_ctx.getChild(0)
        if shift_ctx.getChildCount() != 1:
            return None
        oper_ctx = shift_ctx.getChild(0)
        return self._try_simple_operand_from_operand(oper_ctx)

    def _try_simple_operand_from_operand(self, oper_ctx: Any) -> Optional[_SimpleOperand]:
        """Extract BinRef or Constant from operand context."""
        if oper_ctx.pathOrMetadata() is not None:
            return self._try_bin_ref_from_path_or_metadata(oper_ctx.pathOrMetadata())
        if oper_ctx.numberOperand() is not None:
            return self._constant_from_number(oper_ctx.numberOperand())
        if oper_ctx.stringOperand() is not None:
            return self._constant_from_string(oper_ctx.stringOperand())
        if oper_ctx.booleanOperand() is not None:
            return self._constant_from_boolean(oper_ctx.booleanOperand())
        if oper_ctx.listConstant() is not None:
            return _Constant([], None)
        if oper_ctx.orderedMapConstant() is not None:
            return _Constant({}, None)
        if oper_ctx.placeholder() is not None:
            return self._constant_from_placeholder(oper_ctx.placeholder()) if self._placeholder_values is not None else None
        if oper_ctx.variable() is not None:
            return None
        if oper_ctx.expression() is not None:
            return None
        return None

    def _try_bin_ref_from_path_or_metadata(self, pom_ctx: Any) -> Optional[_BinRef]:
        if pom_ctx.path() is None:
            return None
        return self._try_bin_ref_from_path(pom_ctx.path())

    def _try_bin_ref_from_path(self, path_ctx: Any) -> Optional[_BinRef]:
        base = path_ctx.basePath()
        if not base or not base.binPart():
            return None
        bin_name = base.binPart().getText()
        ctx_list: List[CTX] = []
        for i in range(len(base.listPart())):
            lp = base.listPart(i)
            if lp.listIndex() is not None:
                idx_ctx = lp.listIndex()
                int_text = idx_ctx.getChild(1).getText()
                ctx_list.append(CTX.list_index(int(int_text)))
            elif lp.listValue() is not None:
                val_ctx = lp.listValue()
                val_ident = val_ctx.valueIdentifier()
                val_text = val_ident.getText()
                ctx_list.append(CTX.list_value(self._parse_value_identifier(val_text)))
            elif lp.listRank() is not None:
                rank_ctx = lp.listRank()
                rank_text = rank_ctx.getChild(1).getText()
                ctx_list.append(CTX.list_rank(int(rank_text)))
        explicit_type: Optional[str] = None
        pf = path_ctx.pathFunction()
        if pf is not None:
            get_ctx = pf.pathFunctionGet()
            if get_ctx is not None:
                params = get_ctx.pathFunctionParams()
                if params is not None:
                    for param in params.pathFunctionParam():
                        if param.pathFunctionParamName() and param.pathFunctionParamValue():
                            name_text = param.pathFunctionParamName().getText()
                            if name_text and name_text.lower() == 'type':
                                type_ctx = param.pathFunctionParamValue().pathFunctionGetType()
                                if type_ctx is not None:
                                    explicit_type = type_ctx.getText().upper()
                                break
        return _BinRef(
            bin_name=bin_name,
            explicit_type=explicit_type,
            ctx=ctx_list if ctx_list else None,
        )

    def _constant_from_number(self, num_ctx: Any) -> _Constant:
        text = num_ctx.getText()
        if '.' in text:
            return _Constant(float(text), IndexTypeEnum.NUMERIC)
        return _Constant(int(text), IndexTypeEnum.NUMERIC)

    def _constant_from_string(self, str_ctx: Any) -> _Constant:
        text = str_ctx.getText()
        return _Constant(_unquote(text), IndexTypeEnum.STRING)

    def _constant_from_boolean(self, bool_ctx: Any) -> _Constant:
        text = bool_ctx.getText().lower()
        return _Constant(text == 'true', None)

    def _parse_value_identifier(self, text: str) -> Any:
        """Parse valueIdentifier text to int, float, or string for CTX.list_value."""
        text = text.strip()
        if not text:
            return text
        if text[0] in ('"', "'"):
            return _unquote(text)
        if '.' in text:
            try:
                return float(text)
            except ValueError:
                pass
        try:
            return int(text)
        except ValueError:
            pass
        return text

    def _constant_from_placeholder(self, placeholder_ctx: Any) -> _Constant:
        """Resolve ?0, ?1, ... to a constant using placeholder_values for filter generation."""
        text = placeholder_ctx.getText()
        try:
            index = int(text[1:])
        except ValueError:
            raise DslParseException(f"Invalid placeholder format: {text}")
        value = self._placeholder_values.get(index)
        if isinstance(value, bool):
            value_type = None
        elif isinstance(value, (int, float)):
            value_type = IndexTypeEnum.NUMERIC
        elif isinstance(value, str):
            value_type = IndexTypeEnum.STRING
        elif isinstance(value, bytes):
            value_type = IndexTypeEnum.BLOB
        else:
            value_type = None
        return _Constant(value, value_type)


def build_filter_tree_from_parse_tree(
    parse_tree: Any, dsl_string: str, placeholder_values: Optional[Any] = None
) -> Optional[ExpressionNode]:
    """Build ExpressionNode tree from ANTLR parse tree. Returns None if not filterable."""
    visitor = FilterTreeVisitor(dsl_string, placeholder_values)
    try:
        return visitor.visit(parse_tree)
    except (DslParseException, ValueError, TypeError):
        return None
