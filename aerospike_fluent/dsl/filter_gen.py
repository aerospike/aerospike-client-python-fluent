"""Filter generation from DSL expressions.

This module provides functionality to generate optimal secondary index Filters
from DSL expressions based on available indexes. It splits expressions to use
secondary indexes where possible and filter expressions for the rest.

The implementation uses a tree-based approach:
1. Build an expression tree that tracks filter eligibility
2. Mark nodes under OR as "excluded from filter" (can't use secondary index)
3. Collect all filterable expressions grouped by cardinality
4. Choose the best by cardinality (or alphabetically if tied)
5. Generate complementary Exp, skipping the part used for Filter
"""

import base64
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from aerospike_async import (
    CTX,
    CollectionIndexType,
    Filter,
    FilterExpression,
    IndexType,
)

from aerospike_fluent.dsl.exceptions import DslParseException


def _substitute_placeholders(dsl_string: str, placeholder_values: Any) -> str:
    """Replace ?0, ?1, ... in dsl_string with DSL literal form for filter/arithmetic parsing."""
    # Find all ?N from end to start so indices stay valid
    pattern = re.compile(r"\?(\d+)")
    matches = list(pattern.finditer(dsl_string))
    if not matches:
        return dsl_string
    result = list(dsl_string)
    for m in reversed(matches):
        idx = int(m.group(1))
        try:
            value = placeholder_values.get(idx)
        except Exception:
            continue
        if isinstance(value, bool):
            repl = "true" if value else "false"
        elif isinstance(value, (int, float)):
            repl = str(value)
        elif isinstance(value, str):
            repl = '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        elif isinstance(value, bytes):
            repl = '"' + base64.b64encode(value).decode("ascii") + '"'
        else:
            continue
        start, end = m.span()
        result[start:end] = repl
    return "".join(result)


class IndexTypeEnum(Enum):
    """Index type for filter generation matching."""
    NUMERIC = "NUMERIC"
    STRING = "STRING"
    GEO2D_SPHERE = "GEO2D_SPHERE"
    BLOB = "BLOB"

    def to_aerospike(self) -> IndexType:
        """Convert to aerospike_async IndexType."""
        if self == IndexTypeEnum.NUMERIC:
            return IndexType.NUMERIC
        elif self == IndexTypeEnum.STRING:
            return IndexType.STRING
        elif self == IndexTypeEnum.GEO2D_SPHERE:
            return IndexType.GEO2D_SPHERE
        elif self == IndexTypeEnum.BLOB:
            return getattr(IndexType, "BLOB", IndexType.STRING)
        raise ValueError(f"Unknown index type: {self}")


@dataclass
class Index:
    """Represents an Aerospike secondary index.

    Attributes:
        namespace: Namespace of the indexed bin.
        bin: Name of the indexed bin.
        index_type: Type of the index (NUMERIC, STRING, GEO2D_SPHERE).
        name: Optional name of the index.
        bin_values_ratio: Cardinality ratio (entries_per_bval). Higher = more selective.
            Used to choose optimal index when multiple are available.
        collection_index_type: Collection index type for list/map indexes.
        ctx: Array of CTX representing context of the index.
    """
    bin: str
    index_type: IndexTypeEnum
    namespace: Optional[str] = None
    name: Optional[str] = None
    bin_values_ratio: Optional[float] = None
    collection_index_type: Optional[CollectionIndexType] = None
    ctx: Optional[List[CTX]] = None


@dataclass
class IndexContext:
    """Holds namespace and indexes for filter generation.

    Attributes:
        namespace: Namespace to match with index namespaces.
        indexes: Collection of Index objects for filter generation.
    """
    namespace: str
    indexes: List[Index] = field(default_factory=list)

    @classmethod
    def of(cls, namespace: str, indexes: List[Index]) -> "IndexContext":
        """Create IndexContext with namespace and indexes."""
        return cls(namespace=namespace, indexes=indexes)


@dataclass
class ParseResult:
    """Result of parsing DSL with filter generation.

    Contains both a secondary index Filter (if applicable) and a filter Exp
    (for remaining expression parts that can't use secondary indexes).

    Attributes:
        filter: Secondary index Filter. None if no applicable index found.
        exp: Filter expression for remaining parts. None if fully covered by filter.
    """
    filter: Optional[Filter]
    exp: Optional[FilterExpression]


class OperationType(Enum):
    """Expression operation types."""
    AND = "AND"
    OR = "OR"
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GE = "GE"
    LT = "LT"
    LE = "LE"
    NOT = "NOT"


@dataclass
class ExpressionNode:
    """A node in the expression tree.

    Tracks filter eligibility for secondary index selection.
    """
    op: OperationType
    left: Optional["ExpressionNode"] = None
    right: Optional["ExpressionNode"] = None
    # For leaf nodes (comparisons)
    bin_name: Optional[str] = None
    value: Any = None
    value_type: Optional[IndexTypeEnum] = None
    bin_explicit_type: Optional[str] = None  # from .get(type: XXX)
    ctx: Optional[List[CTX]] = None  # from path e.g. .[5]
    # Filter eligibility tracking
    has_secondary_index_filter: bool = False
    is_excl_from_secondary_index_filter: bool = False
    # Original DSL string for this part (for Exp generation)
    dsl_fragment: Optional[str] = None
    # Arithmetic comparison: (bin arith_op arith_constant) rel value
    arith_op: Optional[str] = None  # '+', '-', '*', '/'
    arith_constant: Optional[int] = None
    bin_on_left: Optional[bool] = None  # True if $.bin op const, False if const op $.bin


class FilterGenerator:
    """Generates optimal Filter from DSL expression based on available indexes.

    The generator analyzes the expression tree to find parts that can use
    secondary indexes, choosing the most selective index based on cardinality.
    Parts that can't use secondary indexes remain as filter expressions.
    """

    def __init__(self, index_context: Optional[IndexContext] = None):
        """Initialize with optional index context."""
        self.index_context = index_context
        self._indexes_by_bin: Dict[str, List[Index]] = {}
        if index_context:
            self._build_index_map()

    def _validate_comparison_types(self, node: Optional[ExpressionNode]) -> None:
        """Raise DslParseException if BOOL is compared to numeric (e.g. INT)."""
        if node is None:
            return
        if node.bin_name is not None and node.bin_explicit_type == "BOOL":
            is_numeric = node.value_type == IndexTypeEnum.NUMERIC or (
                isinstance(node.value, (int, float)) and not isinstance(node.value, bool)
            )
            if is_numeric:
                raise DslParseException("Cannot compare BOOL to INT")
        self._validate_comparison_types(node.left)
        self._validate_comparison_types(node.right)

    def _build_index_map(self) -> None:
        """Build map of bin name to matching indexes."""
        if not self.index_context:
            return

        for index in self.index_context.indexes:
            # Index namespace must match context namespace
            # - Index with no namespace doesn't match context with namespace
            # - Index with different namespace doesn't match
            if index.namespace != self.index_context.namespace:
                continue
            if index.bin not in self._indexes_by_bin:
                self._indexes_by_bin[index.bin] = []
            self._indexes_by_bin[index.bin].append(index)

    def generate(self, dsl_string: str, placeholder_values=None) -> ParseResult:
        """Generate Filter and Exp from DSL string.
        
        Args:
            dsl_string: The DSL expression string.
            placeholder_values: Optional placeholder values.
            
        Returns:
            ParseResult with Filter and/or Exp.
        """
        from aerospike_fluent.dsl.parser import parse_dsl
        from aerospike_fluent.dsl.filter_visitor import build_filter_tree_from_parse_tree
        from antlr4 import InputStream, CommonTokenStream
        from aerospike_fluent.dsl.antlr4.generated.ConditionLexer import ConditionLexer
        from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser

        try:
            from aerospike_fluent.dsl.parser import _DSLParseErrorListener
            input_stream = InputStream(dsl_string)
            lexer = ConditionLexer(input_stream)
            token_stream = CommonTokenStream(lexer)
            parser = ConditionParser(token_stream)
            parser.removeErrorListeners()
            parser.addErrorListener(_DSLParseErrorListener())
            parse_tree = parser.parse()
        except DslParseException:
            raise
        except Exception as e:
            raise DslParseException("Could not parse given DSL expression input") from e

        tree = build_filter_tree_from_parse_tree(parse_tree, dsl_string, placeholder_values)

        def _safe_exp():
            try:
                return parse_dsl(dsl_string, placeholder_values)
            except DslParseException:
                return None

        if tree is None:
            try:
                exp = parse_dsl(dsl_string, placeholder_values)
                return ParseResult(filter=None, exp=exp)
            except DslParseException as e:
                msg = str(e)
                if "List constants in expressions are not yet fully supported" in msg or "Map constants in expressions are not yet fully supported" in msg:
                    return ParseResult(filter=None, exp=None)
                raise

        self._validate_comparison_types(tree)

        # No indexes - return full expression as Exp
        if not self._indexes_by_bin:
            return ParseResult(filter=None, exp=_safe_exp())

        # Mark nodes excluded from filter (under OR)
        self._mark_excluded_nodes(tree)
        
        # Collect filterable expressions by cardinality
        exprs_by_cardinality: Dict[float, List[ExpressionNode]] = {}
        self._collect_filterable_expressions(tree, exprs_by_cardinality)
        
        if not exprs_by_cardinality:
            return ParseResult(filter=None, exp=_safe_exp())
        
        # Choose best expression (highest cardinality, then alphabetical)
        best_cardinality = max(exprs_by_cardinality.keys())
        candidates = exprs_by_cardinality[best_cardinality]
        
        # Sort alphabetically by bin name for consistent selection
        candidates.sort(key=lambda n: n.bin_name or "")
        chosen = candidates[0]

        # Create Filter
        filter_obj = self._create_filter(chosen)
        if filter_obj is None:
            return ParseResult(filter=None, exp=_safe_exp())

        chosen.has_secondary_index_filter = True
        exp = self._generate_exp(tree, placeholder_values)
        return ParseResult(filter=filter_obj, exp=exp)
    
    def _mark_excluded_nodes(self, node: Optional[ExpressionNode]) -> None:
        """Mark nodes under OR as excluded from filter building."""
        if node is None:
            return
        
        if node.op == OperationType.OR:
            # Both children of OR are excluded
            self._mark_subtree_excluded(node.left)
            self._mark_subtree_excluded(node.right)
        else:
            # Propagate exclusion from parent
            if node.is_excl_from_secondary_index_filter:
                if node.left:
                    node.left.is_excl_from_secondary_index_filter = True
                if node.right:
                    node.right.is_excl_from_secondary_index_filter = True
            
            self._mark_excluded_nodes(node.left)
            self._mark_excluded_nodes(node.right)
    
    def _mark_subtree_excluded(self, node: Optional[ExpressionNode]) -> None:
        """Mark entire subtree as excluded from filter building."""
        if node is None:
            return
        node.is_excl_from_secondary_index_filter = True
        self._mark_subtree_excluded(node.left)
        self._mark_subtree_excluded(node.right)
    
    def _collect_filterable_expressions(
        self,
        node: Optional[ExpressionNode],
        exprs_by_cardinality: Dict[float, List[ExpressionNode]]
    ) -> None:
        """Collect expressions that can use secondary index, grouped by cardinality."""
        if node is None:
            return
        
        # Skip excluded nodes
        if node.is_excl_from_secondary_index_filter:
            return
        
        # Check if this is a filterable comparison
        if node.bin_name and node.op in (OperationType.EQ, OperationType.GT, OperationType.GE,
                                          OperationType.LT, OperationType.LE):
            # Check if we have a matching index (including ctx when present)
            cardinality = self._get_index_cardinality(
                node.bin_name, node.value_type, node.op, node.ctx
            )
            if cardinality is not None:
                if cardinality not in exprs_by_cardinality:
                    exprs_by_cardinality[cardinality] = []
                exprs_by_cardinality[cardinality].append(node)
        
        # Recurse for AND nodes
        if node.op == OperationType.AND:
            self._collect_filterable_expressions(node.left, exprs_by_cardinality)
            self._collect_filterable_expressions(node.right, exprs_by_cardinality)
    
    def _get_index_cardinality(
        self,
        bin_name: str,
        value_type: Optional[IndexTypeEnum],
        op: OperationType,
        ctx: Optional[List[CTX]] = None,
    ) -> Optional[float]:
        """Get cardinality of matching index, or None if no match."""
        if bin_name not in self._indexes_by_bin:
            return None

        # String comparisons (>, <, etc.) not supported by secondary index
        if value_type == IndexTypeEnum.STRING and op in (OperationType.GT, OperationType.GE,
                                                          OperationType.LT, OperationType.LE):
            return None

        for index in self._indexes_by_bin[bin_name]:
            if not self._ctx_matches(index.ctx, ctx):
                continue
            if index.index_type == value_type:
                return index.bin_values_ratio if index.bin_values_ratio is not None else -1
            # BLOB index: quoted string is base64-encoded blob literal (EQ only)
            if index.index_type == IndexTypeEnum.BLOB and value_type == IndexTypeEnum.STRING and op == OperationType.EQ:
                return index.bin_values_ratio if index.bin_values_ratio is not None else -1

        return None

    def _ctx_matches(
        self, index_ctx: Optional[List[CTX]], node_ctx: Optional[List[CTX]]
    ) -> bool:
        """True if index context matches expression node context."""
        if node_ctx is None and index_ctx is None:
            return True
        if node_ctx is None or index_ctx is None:
            return False
        if len(node_ctx) != len(index_ctx):
            return False
        return all(
            getattr(a, "ctx", a) == getattr(b, "ctx", b) for a, b in zip(index_ctx, node_ctx)
        )
    
    def _create_filter(self, node: ExpressionNode) -> Optional[Filter]:
        """Create Filter from expression node."""
        if node.bin_name is None or node.value is None:
            return None

        if node.arith_op is not None and node.arith_constant is not None and node.bin_on_left is not None:
            from aerospike_fluent.dsl.arithmetic_filter import filter_from_arithmetic_node
            if not isinstance(node.value, int):
                return None
            return filter_from_arithmetic_node(
                node.bin_name,
                node.arith_op,
                node.arith_constant,
                node.bin_on_left,
                node.op,
                int(node.value),
                node.ctx,
            )

        value = node.value
        if node.op == OperationType.EQ and isinstance(value, str):
            indexes_for_bin = self._indexes_by_bin.get(node.bin_name, [])
            if any(idx.index_type == IndexTypeEnum.BLOB for idx in indexes_for_bin):
                value = base64.b64decode(value)

        if node.op == OperationType.EQ:
            if node.ctx:
                try:
                    return Filter.equal(node.bin_name, value, *node.ctx)
                except TypeError:
                    return Filter.equal(node.bin_name, value)
            return Filter.equal(node.bin_name, value)
        elif node.op == OperationType.GT:
            return Filter.range(node.bin_name, int(node.value) + 1, 2**63 - 1)
        elif node.op == OperationType.GE:
            return Filter.range(node.bin_name, int(node.value), 2**63 - 1)
        elif node.op == OperationType.LT:
            return Filter.range(node.bin_name, -(2**63), int(node.value) - 1)
        elif node.op == OperationType.LE:
            return Filter.range(node.bin_name, -(2**63), int(node.value))

        return None
    
    def _generate_exp(self, tree: ExpressionNode, placeholder_values) -> Optional[FilterExpression]:
        """Generate complementary Exp, skipping part used for Filter."""
        from aerospike_fluent.dsl.parser import parse_dsl
        
        # Collect remaining DSL fragments
        remaining_parts = self._collect_remaining_parts(tree)
        
        if not remaining_parts:
            return None
        
        if len(remaining_parts) == 1:
            return parse_dsl(remaining_parts[0], placeholder_values)
        
        # Join with AND
        remaining_dsl = " and ".join(remaining_parts)
        return parse_dsl(remaining_dsl, placeholder_values)
    
    def _collect_remaining_parts(self, node: Optional[ExpressionNode]) -> List[str]:
        """Collect DSL fragments not used for Filter."""
        if node is None:
            return []
        
        # Skip nodes used for filter
        if node.has_secondary_index_filter:
            return []
        
        # Leaf comparison node
        if node.dsl_fragment:
            return [node.dsl_fragment]
        
        # OR node - include both children
        if node.op == OperationType.OR:
            left_parts = self._collect_remaining_parts(node.left)
            right_parts = self._collect_remaining_parts(node.right)
            if left_parts and right_parts:
                left_dsl = " and ".join(left_parts) if len(left_parts) > 1 else left_parts[0]
                right_dsl = " and ".join(right_parts) if len(right_parts) > 1 else right_parts[0]
                return [f"({left_dsl} or {right_dsl})"]
            return left_parts or right_parts
        
        # AND node - collect children
        if node.op == OperationType.AND:
            parts = []
            parts.extend(self._collect_remaining_parts(node.left))
            parts.extend(self._collect_remaining_parts(node.right))
            return parts
        
        return []


def _exp_type_matches_index_type(value_type: Optional[IndexTypeEnum], index: Index) -> bool:
    """Check if expression value type matches index type."""
    if value_type is None:
        return False
    return value_type == index.index_type
