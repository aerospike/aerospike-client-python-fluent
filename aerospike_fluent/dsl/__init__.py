"""DSL (Domain Specific Language) for building query expressions.

This module provides parsing of text-based DSL expressions into FilterExpression objects.
The DSL allows developers to write readable query conditions like:
    "$.country == 'US' and $.order_total > 500"

instead of using verbose Expression builder APIs.
"""

from __future__ import annotations

from aerospike_fluent.dsl.exceptions import DslParseException
from aerospike_fluent.dsl.parser import DSLParser, parse_dsl

__all__ = [
    "DslParseException",
    "DSLParser",
    "parse_dsl",
]
