"""DSL Visitor - Converts ANTLR parse tree to FilterExpression objects.

This module contains the visitor that walks the ANTLR parse tree and converts
it into Aerospike FilterExpression objects.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from aerospike_async import FilterExpression

from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser
from aerospike_fluent.dsl.antlr4.generated.ConditionVisitor import ConditionVisitor
from aerospike_fluent.dsl.exceptions import DslParseException


def _unquote(text: str) -> str:
    """Remove quotes from a quoted string."""
    if (text.startswith("'") and text.endswith("'")) or (text.startswith('"') and text.endswith('"')):
        return text[1:-1]
    return text


class ExpressionConditionVisitor(ConditionVisitor):
    """Visitor that converts ANTLR parse tree nodes to FilterExpression objects."""

    def visitParse(self, ctx: ConditionParser.ParseContext) -> Optional[FilterExpression]:
        """Visit the root parse node."""
        return self.visit(ctx.expression())

    def visitExpression(self, ctx: ConditionParser.ExpressionContext) -> Optional[FilterExpression]:
        """Visit expression node."""
        return self.visit(ctx.logicalOrExpression())

    def visitOrExpression(self, ctx: ConditionParser.OrExpressionContext) -> Optional[FilterExpression]:
        """Visit OR expression: expr1 or expr2 or expr3 ..."""
        if len(ctx.logicalAndExpression()) == 1:
            return self.visit(ctx.logicalAndExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.logicalAndExpression():
            expr = self.visit(expr_ctx)
            if expr is None:
                raise DslParseException("Failed to parse expression in OR clause")
            expressions.append(expr)

        if not expressions:
            raise DslParseException("OR expression requires at least one expression")
        return FilterExpression.or_(expressions)

    def visitAndExpression(self, ctx: ConditionParser.AndExpressionContext) -> Optional[FilterExpression]:
        """Visit AND expression: expr1 and expr2 and expr3 ..."""
        if len(ctx.basicExpression()) == 1:
            return self.visit(ctx.basicExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.basicExpression():
            expr = self.visit(expr_ctx)
            if expr is None:
                raise DslParseException("Failed to parse expression in AND clause")
            expressions.append(expr)

        if not expressions:
            raise DslParseException("AND expression requires at least one expression")
        return FilterExpression.and_(expressions)

    def visitNotExpression(self, ctx: ConditionParser.NotExpressionContext) -> Optional[FilterExpression]:
        """Visit NOT expression: not (expr)"""
        expr = self.visit(ctx.expression())
        if expr is None:
            raise DslParseException("Failed to parse expression in NOT clause")
        return FilterExpression.not_(expr)

    def visitEqualityExpression(self, ctx: ConditionParser.EqualityExpressionContext) -> Optional[FilterExpression]:
        """Visit equality expression: left == right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in equality expression")
        return FilterExpression.eq(left, right)

    def visitInequalityExpression(self, ctx: ConditionParser.InequalityExpressionContext) -> Optional[FilterExpression]:
        """Visit inequality expression: left != right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in inequality expression")
        return FilterExpression.ne(left, right)

    def visitGreaterThanExpression(self, ctx: ConditionParser.GreaterThanExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than expression: left > right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in greater than expression")
        return FilterExpression.gt(left, right)

    def visitGreaterThanOrEqualExpression(self, ctx: ConditionParser.GreaterThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than or equal expression: left >= right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in greater than or equal expression")
        return FilterExpression.ge(left, right)

    def visitLessThanExpression(self, ctx: ConditionParser.LessThanExpressionContext) -> Optional[FilterExpression]:
        """Visit less than expression: left < right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in less than expression")
        return FilterExpression.lt(left, right)

    def visitLessThanOrEqualExpression(self, ctx: ConditionParser.LessThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit less than or equal expression: left <= right"""
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in less than or equal expression")
        return FilterExpression.le(left, right)

    def visitComparisonExpressionWrapper(self, ctx: ConditionParser.ComparisonExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.comparisonExpression())

    def visitAdditiveExpressionWrapper(self, ctx: ConditionParser.AdditiveExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.additiveExpression())

    def visitMultiplicativeExpressionWrapper(self, ctx: ConditionParser.MultiplicativeExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.multiplicativeExpression())

    def visitBitwiseExpressionWrapper(self, ctx: ConditionParser.BitwiseExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.bitwiseExpression())

    def visitShiftExpressionWrapper(self, ctx: ConditionParser.ShiftExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.shiftExpression())

    def visitOperandExpression(self, ctx: ConditionParser.OperandExpressionContext) -> Optional[FilterExpression]:
        """Visit operand expression."""
        return self.visit(ctx.operand())

    def visitBinPart(self, ctx: ConditionParser.BinPartContext) -> Optional[FilterExpression]:
        """Visit bin part: bin name identifier.
        
        Returns a string_bin expression by default. Type will be inferred
        from usage context or can be explicitly cast.
        """
        bin_name = ctx.NAME_IDENTIFIER().getText()
        return FilterExpression.string_bin(bin_name)

    def visitPath(self, ctx: ConditionParser.PathContext) -> Optional[FilterExpression]:
        """Visit path: $.binName or $.binName.pathFunction()"""
        base_path = self.visit(ctx.basePath())
        if base_path is None:
            raise DslParseException("Failed to parse base path")
        
        # Handle path functions like asInt(), asFloat(), exists(), count()
        if ctx.pathFunction() is not None:
            path_func_ctx = ctx.pathFunction()
            
            # Check for cast functions (asInt, asFloat, etc.)
            if hasattr(path_func_ctx, 'pathFunctionCast') and path_func_ctx.pathFunctionCast() is not None:
                cast_ctx = path_func_ctx.pathFunctionCast()
                cast_text = cast_ctx.getText().lower()
                
                if cast_text == "asint()":
                    return FilterExpression.to_int(base_path)
                elif cast_text == "asfloat()":
                    return FilterExpression.to_float(base_path)
                # Other casts may need different handling
            
            # Check for exists()
            if hasattr(path_func_ctx, 'pathFunctionExists') and path_func_ctx.pathFunctionExists() is not None:
                # Exists() typically returns a boolean - may need special handling
                # For now, return base_path
                pass
            
            # Check for count()
            if hasattr(path_func_ctx, 'pathFunctionCount') and path_func_ctx.pathFunctionCount() is not None:
                # Count() returns size - may need special handling
                # For now, return base_path
                pass
            
            # Check for get()
            if hasattr(path_func_ctx, 'pathFunctionGet') and path_func_ctx.pathFunctionGet() is not None:
                # Get() is complex - may need special handling
                pass
        
        return base_path

    def visitBasePath(self, ctx: ConditionParser.BasePathContext) -> Optional[FilterExpression]:
        """Visit base path: binPart with optional CDT parts."""
        # Find the bin part (should be first)
        bin_part = None
        for child in ctx.children:
            if hasattr(child, 'NAME_IDENTIFIER'):
                bin_name = child.NAME_IDENTIFIER().getText()
                bin_part = FilterExpression.string_bin(bin_name)
                break
        
        if bin_part is None:
            raise DslParseException("Base path must start with a bin name")
        
        # TODO: Handle CDT parts (list/map access like .[0], .key, etc.)
        # For now, return just the bin part
        return bin_part

    def visitStringOperand(self, ctx: ConditionParser.StringOperandContext) -> Optional[FilterExpression]:
        """Visit string operand: 'value' or "value" """
        text = ctx.getText()
        unquoted = _unquote(text)
        return FilterExpression.string_val(unquoted)

    def visitIntOperand(self, ctx: ConditionParser.IntOperandContext) -> Optional[FilterExpression]:
        """Visit integer operand: 123"""
        text = ctx.INT().getText()
        value = int(text)
        return FilterExpression.int_val(value)

    def visitFloatOperand(self, ctx: ConditionParser.FloatOperandContext) -> Optional[FilterExpression]:
        """Visit float operand: 123.45"""
        text = ctx.FLOAT().getText()
        value = float(text)
        return FilterExpression.float_val(value)

    def visitBooleanOperand(self, ctx: ConditionParser.BooleanOperandContext) -> Optional[FilterExpression]:
        """Visit boolean operand: true or false"""
        text = ctx.getText().lower()
        value = text == "true"
        return FilterExpression.bool_val(value)

    def visitNumberOperand(self, ctx: ConditionParser.NumberOperandContext) -> Optional[FilterExpression]:
        """Visit number operand - delegates to int or float."""
        return self.visitChildren(ctx)

    def visitOperand(self, ctx: ConditionParser.OperandContext) -> Optional[FilterExpression]:
        """Visit operand - can be number, string, boolean, path, variable, etc."""
        # Check for path ($.binName)
        if ctx.pathOrMetadata() is not None:
            return self.visit(ctx.pathOrMetadata())
        
        # Check for number
        if ctx.numberOperand() is not None:
            return self.visit(ctx.numberOperand())
        
        # Check for boolean
        if ctx.booleanOperand() is not None:
            return self.visit(ctx.booleanOperand())
        
        # Check for string
        if ctx.stringOperand() is not None:
            return self.visit(ctx.stringOperand())
        
        # Check for parenthesized expression
        if ctx.expression() is not None:
            return self.visit(ctx.expression())
        
        # TODO: Handle listConstant, orderedMapConstant, variable, placeholder
        raise DslParseException(f"Unsupported operand type: {ctx.getText()}")

    def visitPathOrMetadata(self, ctx: ConditionParser.PathOrMetadataContext) -> Optional[FilterExpression]:
        """Visit path or metadata: $.binName or metadata function."""
        if ctx.path() is not None:
            return self.visit(ctx.path())
        if ctx.metadata() is not None:
            return self.visit(ctx.metadata())
        raise DslParseException("PathOrMetadata must contain either path or metadata")

    def visitMetadata(self, ctx: ConditionParser.MetadataContext) -> Optional[FilterExpression]:
        """Visit metadata function: deviceSize(), ttl(), etc."""
        text = ctx.METADATA_FUNCTION().getText()
        
        # Map metadata functions to FilterExpression methods
        if text == "deviceSize()":
            return FilterExpression.device_size()
        elif text == "memorySize()":
            return FilterExpression.memory_size()
        elif text == "recordSize()":
            return FilterExpression.record_size()
        elif text == "isTombstone()":
            return FilterExpression.is_tombstone()
        elif text == "keyExists()":
            return FilterExpression.key_exists()
        elif text == "lastUpdate()":
            return FilterExpression.last_update()
        elif text == "sinceUpdate()":
            return FilterExpression.since_update()
        elif text == "setName()":
            return FilterExpression.set_name()
        elif text == "ttl()":
            return FilterExpression.ttl()
        elif text == "voidTime()":
            return FilterExpression.void_time()
        elif text.startswith("digestModulo(") and text.endswith(")"):
            # Extract integer parameter
            match = re.search(r"digestModulo\((\d+)\)", text)
            if match:
                value = int(match.group(1))
                return FilterExpression.digest_modulo(value)
        
        raise DslParseException(f"Unsupported metadata function: {text}")

    def visitExclusiveExpression(self, ctx: ConditionParser.ExclusiveExpressionContext) -> Optional[FilterExpression]:
        """Visit exclusive expression: exclusive(expr1, expr2, ...)"""
        # Exclusive means exactly one must be true (XOR of all)
        if len(ctx.expression()) < 2:
            raise DslParseException("Exclusive expression requires at least 2 expressions")
        
        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.expression():
            expr = self.visit(expr_ctx)
            if expr is None:
                raise DslParseException("Failed to parse expression in exclusive clause")
            expressions.append(expr)
        
        # Exclusive is equivalent to XOR of all expressions
        # For now, implement as: (expr1 XOR expr2) XOR expr3 ...
        result = expressions[0]
        for expr in expressions[1:]:
            result = FilterExpression.xor([result, expr])
        return result

    def visitWithExpression(self, ctx: ConditionParser.WithExpressionContext) -> Optional[FilterExpression]:
        """Visit with expression: with(var1=expr1, var2=expr2) do (action)"""
        # TODO: Implement variable binding
        raise DslParseException("With expressions are not yet supported")

    def visitWhenExpression(self, ctx: ConditionParser.WhenExpressionContext) -> Optional[FilterExpression]:
        """Visit when expression: when(cond1=>action1, cond2=>action2, default=>action)"""
        # TODO: Implement conditional expressions
        raise DslParseException("When expressions are not yet supported")

    def visitComparisonExpressionWrapper(self, ctx: ConditionParser.ComparisonExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.comparisonExpression())

    def visitAdditiveExpressionWrapper(self, ctx: ConditionParser.AdditiveExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.additiveExpression())

    def visitMultiplicativeExpressionWrapper(self, ctx: ConditionParser.MultiplicativeExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.multiplicativeExpression())

    def visitBitwiseExpressionWrapper(self, ctx: ConditionParser.BitwiseExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.bitwiseExpression())

    def visitShiftExpressionWrapper(self, ctx: ConditionParser.ShiftExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.shiftExpression())

    def visitAddExpression(self, ctx: ConditionParser.AddExpressionContext) -> Optional[FilterExpression]:
        """Visit add expression: left + right"""
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in add expression")
        return FilterExpression.num_add([left, right])

    def visitSubExpression(self, ctx: ConditionParser.SubExpressionContext) -> Optional[FilterExpression]:
        """Visit subtract expression: left - right"""
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in subtract expression")
        return FilterExpression.num_sub([left, right])

    def visitMulExpression(self, ctx: ConditionParser.MulExpressionContext) -> Optional[FilterExpression]:
        """Visit multiply expression: left * right"""
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.bitwiseExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in multiply expression")
        return FilterExpression.num_mul([left, right])

    def visitDivExpression(self, ctx: ConditionParser.DivExpressionContext) -> Optional[FilterExpression]:
        """Visit divide expression: left / right"""
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.bitwiseExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in divide expression")
        return FilterExpression.num_div([left, right])

    def visitModExpression(self, ctx: ConditionParser.ModExpressionContext) -> Optional[FilterExpression]:
        """Visit modulo expression: left % right"""
        left = self.visit(ctx.shiftExpression(0))
        right = self.visit(ctx.shiftExpression(1))
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in modulo expression")
        return FilterExpression.num_mod(left, right)

    def visitIntAndExpression(self, ctx: ConditionParser.IntAndExpressionContext) -> Optional[FilterExpression]:
        """Visit integer AND expression: left & right"""
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.shiftExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in integer AND expression")
        return FilterExpression.int_and([left, right])

    def visitIntOrExpression(self, ctx: ConditionParser.IntOrExpressionContext) -> Optional[FilterExpression]:
        """Visit integer OR expression: left | right"""
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.shiftExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in integer OR expression")
        return FilterExpression.int_or([left, right])

    def visitIntXorExpression(self, ctx: ConditionParser.IntXorExpressionContext) -> Optional[FilterExpression]:
        """Visit integer XOR expression: left ^ right"""
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.shiftExpression())
        if left is None or right is None:
            raise DslParseException("Failed to parse operands in integer XOR expression")
        return FilterExpression.int_xor([left, right])

    def visitIntNotExpression(self, ctx: ConditionParser.IntNotExpressionContext) -> Optional[FilterExpression]:
        """Visit integer NOT expression: ~expr"""
        expr = self.visit(ctx.shiftExpression())
        if expr is None:
            raise DslParseException("Failed to parse operand in integer NOT expression")
        return FilterExpression.int_not(expr)

    def visitIntLShiftExpression(self, ctx: ConditionParser.IntLShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit left shift expression: left << right"""
        value = self.visit(ctx.shiftExpression(0))
        shift = self.visit(ctx.shiftExpression(1))
        if value is None or shift is None:
            raise DslParseException("Failed to parse operands in left shift expression")
        return FilterExpression.int_lshift(value, shift)

    def visitIntRShiftExpression(self, ctx: ConditionParser.IntRShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit right shift expression: left >> right"""
        value = self.visit(ctx.shiftExpression(0))
        shift = self.visit(ctx.shiftExpression(1))
        if value is None or shift is None:
            raise DslParseException("Failed to parse operands in right shift expression")
        return FilterExpression.int_rshift(value, shift)

    def visitListConstant(self, ctx: ConditionParser.ListConstantContext) -> Optional[FilterExpression]:
        """Visit list constant: [val1, val2, ...]"""
        values: List[Any] = []
        for operand_ctx in ctx.operand():
            expr = self.visit(operand_ctx)
            if expr is None:
                raise DslParseException("Failed to parse operand in list constant")
            # For now, we can't extract the actual value from FilterExpression
            # List constants need to be handled differently - they're typically used
            # in operations like "in" which aren't in the basic grammar
            # This is a placeholder that will need more work
            raise DslParseException("List constants in expressions are not yet fully supported")

    def visitOrderedMapConstant(self, ctx: ConditionParser.OrderedMapConstantContext) -> Optional[FilterExpression]:
        """Visit ordered map constant: {key1: val1, key2: val2, ...}"""
        # Map constants are complex - they need key-value pairs
        # This is a placeholder that will need more work
        raise DslParseException("Map constants in expressions are not yet fully supported")

    def visitVariable(self, ctx: ConditionParser.VariableContext) -> Optional[FilterExpression]:
        """Visit variable: ${varName}"""
        var_name = ctx.getText()
        # Remove ${ and }
        if var_name.startswith("${") and var_name.endswith("}"):
            var_name = var_name[2:-1]
        return FilterExpression.var(var_name)

    def visitPlaceholder(self, ctx: ConditionParser.PlaceholderContext) -> Optional[FilterExpression]:
        """Visit placeholder: ?0, ?1, etc."""
        # Placeholders are typically used for parameterized queries
        # They need to be resolved at query execution time, not during parsing
        # For now, raise an exception as this requires additional infrastructure
        raise DslParseException("Placeholders are not yet supported - use variables instead")

    def visitPathFunction(self, ctx: ConditionParser.PathFunctionContext) -> Optional[FilterExpression]:
        """Visit path function: asInt(), asFloat(), get(), etc."""
        # Delegate to specific path function visitors
        return self.visitChildren(ctx)

    def visitPathFunctionCast(self, ctx: ConditionParser.PathFunctionCastContext) -> Optional[FilterExpression]:
        """Visit path function cast: asInt(), asFloat(), etc.
        
        These functions are applied to the base path in visitPath().
        For now, we return a marker that visitPath() can use.
        """
        # Path function cast is handled in visitPath() by applying to_int or to_float
        # to the base path. We return None here and handle it in visitPath().
        return None

    def visitPathFunctionExists(self, ctx: ConditionParser.PathFunctionExistsContext) -> Optional[FilterExpression]:
        """Visit path function exists: exists()"""
        # Exists() is typically used in conditions, not as a value
        # For now, return None - this may need special handling
        return None

    def visitPathFunctionCount(self, ctx: ConditionParser.PathFunctionCountContext) -> Optional[FilterExpression]:
        """Visit path function count: count()"""
        # Count() returns the size of a list/map
        # This needs to be applied to the base path in visitPath()
        return None

    def visitPathFunctionGet(self, ctx: ConditionParser.PathFunctionGetContext) -> Optional[FilterExpression]:
        """Visit path function get: get(type:INT, return:VALUE)"""
        # Get() is used for CDT operations
        # This is complex and may need special handling
        return None
