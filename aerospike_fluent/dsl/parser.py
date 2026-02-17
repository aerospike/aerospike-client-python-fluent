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

"""DSL Parser - Parse text DSL strings into FilterExpression objects.

This module provides parsing of text-based DSL expressions like:
    "$.country == 'US' and $.order_total > 500"

into Aerospike FilterExpression objects.

Supports parameterized queries with placeholders:
    parse_dsl("$.age > ?0", PlaceholderValues(30))

Also supports parsing paths into CTX arrays:
    parse_ctx("$.listBin.[0].[1]")  # Returns [CTX.list_index(0), CTX.list_index(1)]

Also supports filter generation with index context:
    parse_dsl_with_index("$.intBin1 > 100 and $.intBin2 < 50", index_context)
    # Returns ParseResult with optimal Filter + remaining Exp
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple, Union

from aerospike_async import CTX, Filter, FilterExpression

from aerospike_fluent.dsl.filter_gen import Index, IndexContext, IndexTypeEnum, ParseResult

from antlr4 import InputStream, CommonTokenStream
from antlr4.error.ErrorListener import ErrorListener

from aerospike_fluent.dsl.antlr4.generated.ConditionLexer import ConditionLexer
from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser
from aerospike_fluent.dsl.exceptions import DslParseException


class _DSLParseErrorListener(ErrorListener):
    """Error listener that raises on first syntax error (no recovery)."""

    def syntaxError(self, recognizer, offending_symbol, line, column, msg, e):
        raise DslParseException(f"line {line}:{column} {msg}")
from aerospike_fluent.dsl.exp_visitor import (
    CDTPath,
    ExpressionConditionVisitor,
    ListIndexPart,
    ListRankPart,
    ListValuePart,
    MapIndexPart,
    MapKeyPart,
    MapRankPart,
    MapValuePart,
)


class PlaceholderValues:
    """Container for placeholder values used in parameterized DSL queries.
    
    Placeholders in DSL strings are written as ?0, ?1, ?2, etc.
    The index corresponds to the position in the values list.
    
    Example:
        # Using positional arguments
        values = PlaceholderValues(100, "hello", 3.14)
        parse_dsl("$.age > ?0 and $.name == ?1", values)
        
        # Using a list
        values = PlaceholderValues.of([100, "hello"])
        
    Supported value types:
        - int -> FilterExpression.int_val()
        - float -> FilterExpression.float_val()
        - str -> FilterExpression.string_val()
        - bool -> FilterExpression.bool_val()
        - bytes -> FilterExpression.blob_val()
        - list -> FilterExpression.list_val()
        - dict -> FilterExpression.map_val()
    """
    
    def __init__(self, *values: Any):
        """Create PlaceholderValues from positional arguments.
        
        Args:
            *values: Values for placeholders ?0, ?1, ?2, etc.
        """
        self._values: list[Any] = list(values)
    
    @classmethod
    def of(cls, *values: Any) -> "PlaceholderValues":
        """Create PlaceholderValues (alternative factory).
        
        Args:
            *values: Values for placeholders.
            
        Returns:
            PlaceholderValues instance.
        """
        return cls(*values)
    
    def get(self, index: int) -> Any:
        """Get the value for a placeholder index.
        
        Args:
            index: The placeholder index (e.g., 0 for ?0).
            
        Returns:
            The value at the given index.
            
        Raises:
            DslParseException: If the index is out of range.
        """
        if index < 0 or index >= len(self._values):
            raise DslParseException(f"Missing value for placeholder ?{index}")
        return self._values[index]
    
    def __len__(self) -> int:
        return len(self._values)
    
    def __repr__(self) -> str:
        return f"PlaceholderValues({', '.join(repr(v) for v in self._values)})"


class DSLParser:
    """Parser for Aerospike DSL expressions."""

    def parse(
        self, 
        dsl_string: str, 
        placeholder_values: Optional[PlaceholderValues] = None
    ) -> FilterExpression:
        """Parse a DSL string into a FilterExpression.

        Args:
            dsl_string: The DSL expression string (e.g., "$.country == 'US' and $.order_total > 500")
            placeholder_values: Optional values for placeholders (?0, ?1, etc.)

        Returns:
            A FilterExpression object that can be used in queries.

        Raises:
            DslParseException: If the DSL string cannot be parsed.
        """
        try:
            # 1. Create input stream from string
            input_stream = InputStream(dsl_string)

            # 2. Create lexer
            lexer = ConditionLexer(input_stream)

            # 3. Create token stream
            token_stream = CommonTokenStream(lexer)

            # 4. Create parser and install error listener so syntax errors raise
            parser = ConditionParser(token_stream)
            parser.removeErrorListeners()
            parser.addErrorListener(_DSLParseErrorListener())

            # 5. Parse to get parse tree
            parse_tree = parser.parse()

            # 6. Visit parse tree with visitor to build FilterExpression
            visitor = ExpressionConditionVisitor(placeholder_values=placeholder_values)
            result = visitor.visit(parse_tree)

            if result is None:
                raise DslParseException("Failed to parse DSL expression: visitor returned None")

            return result

        except Exception as e:
            if isinstance(e, DslParseException):
                raise
            raise DslParseException(f"Failed to parse DSL expression: {e}") from e


# Global parser instance
_parser: Optional[DSLParser] = None


def parse_dsl(
    dsl_string: str,
    placeholder_values: Optional[PlaceholderValues] = None
) -> FilterExpression:
    """Parse a DSL string into a FilterExpression.

    Convenience function that uses a global parser instance.

    Args:
        dsl_string: The DSL expression string.
        placeholder_values: Optional values for placeholders (?0, ?1, etc.)

    Returns:
        A FilterExpression object.

    Example:
        # Simple query
        expr = parse_dsl("$.age > 30")

        # Parameterized query
        expr = parse_dsl("$.age > ?0 and $.name == ?1", PlaceholderValues(30, "John"))
    """
    global _parser
    if _parser is None:
        _parser = DSLParser()
    return _parser.parse(dsl_string, placeholder_values)


def parse_ctx(path: str) -> List[CTX]:
    """Parse a DSL path into a list of CTX objects.

    Converts a DSL path like "$.listBin.[0].[1]" into a CTX array
    for use with secondary index context operations.

    Args:
        path: The DSL path string (e.g., "$.listBin.[0]", "$.mapBin.key.subkey")

    Returns:
        A list of CTX objects representing the path context.

    Raises:
        DslParseException: If the path is invalid or unsupported.

    Example:
        ctx = parse_ctx("$.listBin.[0].[1]")
        # Returns [CTX.list_index(0), CTX.list_index(1)]

        ctx = parse_ctx("$.mapBin.a.bb")
        # Returns [CTX.map_key("a"), CTX.map_key("bb")]
    """
    if not path:
        raise DslParseException("Path must not be null or empty")

    try:
        input_stream = InputStream(path)
        lexer = ConditionLexer(input_stream)
        token_stream = CommonTokenStream(lexer)
        parser = ConditionParser(token_stream)
        parser.removeErrorListeners()
        parser.addErrorListener(_DSLParseErrorListener())
        parse_tree = parser.parse()

        # Use visitor in ctx_only mode to preserve CDTPath without finalization
        visitor = ExpressionConditionVisitor(ctx_only=True)
        result = visitor.visit(parse_tree)

        # Check if result is a full expression (has comparison operator)
        if isinstance(result, FilterExpression):
            raise DslParseException(
                "Unsupported input expression type 'EXPRESSION_CONTAINER', "
                "please provide only path to convert to CTX[]"
            )

        # Handle DeferredBin (bare bin name like $.listBin)
        from aerospike_fluent.dsl.exp_visitor import DeferredBin
        if isinstance(result, DeferredBin):
            raise DslParseException("CDT context is not provided")

        # Check if result is a CDTPath
        if not isinstance(result, CDTPath):
            raise DslParseException("Could not parse the given DSL path input")

        # Check for path functions (get, asInt, etc.)
        if result.explicit_type is not None or result.has_path_function:
            raise DslParseException(
                "Path function is unsupported, please provide only path to convert to CTX[]"
            )

        # Check that CDT context is provided (not just a bin name)
        if not result.parts:
            raise DslParseException("CDT context is not provided")

        # Convert CDTPath parts to CTX list
        ctx_list: List[CTX] = []
        for part in result.parts:
            if isinstance(part, ListIndexPart):
                ctx_list.append(CTX.list_index(part.index))
            elif isinstance(part, ListRankPart):
                ctx_list.append(CTX.list_rank(part.rank))
            elif isinstance(part, ListValuePart):
                ctx_list.append(CTX.list_value(part.value))
            elif isinstance(part, MapKeyPart):
                ctx_list.append(CTX.map_key(part.key))
            elif isinstance(part, MapIndexPart):
                ctx_list.append(CTX.map_index(part.index))
            elif isinstance(part, MapRankPart):
                ctx_list.append(CTX.map_rank(part.rank))
            elif isinstance(part, MapValuePart):
                ctx_list.append(CTX.map_value(part.value))
            else:
                raise DslParseException(f"Unsupported CDT part type: {type(part).__name__}")

        return ctx_list

    except DslParseException:
        raise
    except Exception as e:
        raise DslParseException(f"Could not parse the given DSL path input: {e}") from e


def parse_dsl_with_index(
    dsl_string: str,
    index_context: Optional[IndexContext] = None,
    placeholder_values: Optional[PlaceholderValues] = None,
) -> ParseResult:
    """Parse a DSL string and generate optimal Filter + Exp based on available indexes.

    This function analyzes the DSL expression and available secondary indexes to:
    1. Extract parts that can use secondary index Filters (more efficient)
    2. Return remaining parts as filter Exp (for post-filtering)

    The algorithm:
    1. Build an expression tree that tracks filter eligibility
    2. Mark nodes under OR as "excluded from filter" (can't use secondary index)
    3. Collect all filterable expressions grouped by cardinality
    4. Choose the best by cardinality (or alphabetically if tied)
    5. Generate complementary Exp, skipping the part used for Filter

    Rules for filter generation:
    - AND expressions: One part can become Filter, rest becomes Exp
    - OR expressions: Cannot use Filter (need to evaluate both branches)
    - Nested AND inside OR: The AND parts are still excluded
    - AND(a, OR(b, c)): 'a' can become Filter, OR(b,c) becomes Exp
    - Only simple comparisons (==, >, <, >=, <=) on indexed bins can become Filters
    - String comparisons (>, <, etc.) are not supported by secondary index

    Args:
        dsl_string: The DSL expression string.
        index_context: IndexContext with available indexes. If None, only Exp is returned.
        placeholder_values: Optional values for placeholders (?0, ?1, etc.)

    Returns:
        ParseResult containing:
        - filter: Secondary index Filter (or None if not applicable)
        - exp: Filter expression for remaining parts (or None if fully covered)

    Example:
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, bin_values_ratio=1),
        ]
        ctx = IndexContext.of("test", indexes)

        result = parse_dsl_with_index("$.intBin1 > 100 and $.intBin2 < 1000", ctx)
        # result.filter = Filter.range("intBin2", MIN, 999)  # Higher cardinality chosen
        # result.exp = Exp.gt(Exp.int_bin("intBin1"), Exp.val(100))  # Remaining part
    """
    from aerospike_fluent.dsl.filter_gen import FilterGenerator
    
    generator = FilterGenerator(index_context)
    return generator.generate(dsl_string, placeholder_values)
