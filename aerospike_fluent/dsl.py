"""DSL (Domain Specific Language) for building query expressions.

This module provides a code-based DSL for building query expressions,
allowing expressions to be built using method chaining like:
    Dsl.stringBin("name").eq("Tim").and(Dsl.longBin("age").gt(30))

These expressions are converted to Filter objects for use with query builders.
"""

from __future__ import annotations

from typing import Union, Optional
from aerospike_async import Filter


class _BinExpression:
    """Base class for bin expressions that support comparison operations."""
    
    def __init__(self, bin_name: str, bin_type: str):
        self._bin_name = bin_name
        self._bin_type = bin_type
    
    def eq(self, value: Union[str, int, float]) -> _Expression:
        """Create an equality expression: bin == value"""
        return _Expression(self._bin_name, "eq", value, self._bin_type)
    
    def ne(self, value: Union[str, int, float]) -> _Expression:
        """Create a not-equal expression: bin != value"""
        return _Expression(self._bin_name, "ne", value, self._bin_type)
    
    def gt(self, value: Union[int, float]) -> _Expression:
        """Create a greater-than expression: bin > value"""
        return _Expression(self._bin_name, "gt", value, self._bin_type)
    
    def gte(self, value: Union[int, float]) -> _Expression:
        """Create a greater-than-or-equal expression: bin >= value"""
        return _Expression(self._bin_name, "gte", value, self._bin_type)
    
    def lt(self, value: Union[int, float]) -> _Expression:
        """Create a less-than expression: bin < value"""
        return _Expression(self._bin_name, "lt", value, self._bin_type)
    
    def lte(self, value: Union[int, float]) -> _Expression:
        """Create a less-than-or-equal expression: bin <= value"""
        return _Expression(self._bin_name, "lte", value, self._bin_type)


class _Expression:
    """Represents a query expression that can be combined with logical operators."""
    
    def __init__(
        self,
        bin_name: str,
        operator: str,
        value: Union[str, int, float],
        bin_type: str,
        left: Optional[_Expression] = None,
        right: Optional[_Expression] = None,
        logical_op: Optional[str] = None
    ):
        self._bin_name = bin_name
        self._operator = operator
        self._value = value
        self._bin_type = bin_type
        self._left = left
        self._right = right
        self._logical_op = logical_op
    
    def and_(self, other: _Expression) -> _Expression:
        """Combine this expression with another using AND.
        
        Note: In Python, 'and' is a keyword, so we use 'and_' instead.
        """
        return _Expression(
            bin_name="",
            operator="",
            value=None,
            bin_type="",
            left=self,
            right=other,
            logical_op="and"
        )
    
    def or_(self, other: _Expression) -> _Expression:
        """Combine this expression with another using OR.
        
        Note: In Python, 'or' is a keyword, so we use 'or_' instead.
        """
        return _Expression(
            bin_name="",
            operator="",
            value=None,
            bin_type="",
            left=self,
            right=other,
            logical_op="or"
        )
    
    def to_filters(self) -> list[Filter]:
        """Convert this expression to a list of Filter objects.
        
        Note: Aerospike only supports a single Filter per query. For AND expressions
        with multiple conditions, only the first filter is returned. For full AND/OR
        support, FilterExpression would be needed.
        
        For OR expressions, this requires FilterExpression support.
        """
        if self._logical_op == "and":
            # For AND, Aerospike only supports one filter per query
            # We'll return the first filter as a limitation
            # TODO: Support FilterExpression for full AND/OR support
            if self._left:
                return self._left.to_filters()
            if self._right:
                return self._right.to_filters()
            raise ValueError("Invalid AND expression")
        elif self._logical_op == "or":
            # OR is more complex - would need FilterExpression support
            # For now, raise an error
            raise NotImplementedError("OR expressions require FilterExpression support")
        
        # Single expression - convert to filter
        return [self._to_single_filter()]
    
    def _to_single_filter(self) -> Filter:
        """Convert a single expression to a Filter object."""
        
        # Convert comparison operator to Filter
        if self._operator == "eq":
            # Equality: use Filter.equal() if available, otherwise Filter.range with same begin and end
            if hasattr(Filter, 'equal'):
                return Filter.equal(self._bin_name, self._value)
            else:
                return Filter.range(self._bin_name, self._value, self._value)
        elif self._operator == "gt":
            # Greater than: Filter.range(value, None) means >= value
            # For true >, we approximate with >= (gte) behavior
            # This is a limitation of Filter.range
            return Filter.range(self._bin_name, self._value, None)
        elif self._operator == "gte":
            # Greater than or equal: use Filter.range with begin = value, end = None
            return Filter.range(self._bin_name, self._value, None)
        elif self._operator == "lt":
            # Less than: Filter.range doesn't directly support <
            # We approximate with <= (lte) behavior
            # This is a limitation of Filter.range
            return Filter.range(self._bin_name, None, self._value)
        elif self._operator == "lte":
            # Less than or equal: use Filter.range with begin = None, end = value
            return Filter.range(self._bin_name, None, self._value)
        elif self._operator == "ne":
            # Not equal: Filter doesn't directly support !=
            raise NotImplementedError("Not equal (!=) is not directly supported by Filter")
        else:
            raise ValueError(f"Unsupported operator: {self._operator}")


class Dsl:
    """DSL class for building query expressions."""
    
    @staticmethod
    def stringBin(bin_name: str) -> _BinExpression:
        """Create a string bin expression builder."""
        return _BinExpression(bin_name, "string")
    
    @staticmethod
    def longBin(bin_name: str) -> _BinExpression:
        """Create a long (integer) bin expression builder."""
        return _BinExpression(bin_name, "long")
    
    @staticmethod
    def intBin(bin_name: str) -> _BinExpression:
        """Create an integer bin expression builder (alias for longBin)."""
        return _BinExpression(bin_name, "int")
    
    @staticmethod
    def floatBin(bin_name: str) -> _BinExpression:
        """Create a float bin expression builder."""
        return _BinExpression(bin_name, "float")
    
    @staticmethod
    def and_(*expressions: _Expression) -> _Expression:
        """Combine multiple expressions with AND."""
        if not expressions:
            raise ValueError("At least one expression required")
        result = expressions[0]
        for expr in expressions[1:]:
            result = result.and_(expr)
        return result

