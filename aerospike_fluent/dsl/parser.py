"""DSL Parser - Parse text DSL strings into FilterExpression objects.

This module provides parsing of text-based DSL expressions like:
    "$.country == 'US' and $.order_total > 500"

into Aerospike FilterExpression objects.
"""

from __future__ import annotations

from typing import Optional

from aerospike_async import FilterExpression

from antlr4 import InputStream, CommonTokenStream

from aerospike_fluent.dsl.antlr4.generated.ConditionLexer import ConditionLexer
from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser
from aerospike_fluent.dsl.exceptions import DslParseException
from aerospike_fluent.dsl.visitor import ExpressionConditionVisitor


class DSLParser:
    """Parser for Aerospike DSL expressions."""

    def parse(self, dsl_string: str) -> FilterExpression:
        """Parse a DSL string into a FilterExpression.

        Args:
            dsl_string: The DSL expression string (e.g., "$.country == 'US' and $.order_total > 500")

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

            # 4. Create parser
            parser = ConditionParser(token_stream)

            # 5. Parse to get parse tree
            parse_tree = parser.parse()

            # 6. Visit parse tree with visitor to build FilterExpression
            visitor = ExpressionConditionVisitor()
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


def parse_dsl(dsl_string: str) -> FilterExpression:
    """Parse a DSL string into a FilterExpression.

    Convenience function that uses a global parser instance.

    Args:
        dsl_string: The DSL expression string.

    Returns:
        A FilterExpression object.
    """
    global _parser
    if _parser is None:
        _parser = DSLParser()
    return _parser.parse(dsl_string)
