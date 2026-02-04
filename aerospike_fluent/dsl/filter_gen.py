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


class IndexTypeEnum(Enum):
    """Index type for filter generation matching."""
    NUMERIC = "NUMERIC"
    STRING = "STRING"
    GEO2D_SPHERE = "GEO2D_SPHERE"

    def to_aerospike(self) -> IndexType:
        """Convert to aerospike_async IndexType."""
        if self == IndexTypeEnum.NUMERIC:
            return IndexType.NUMERIC
        elif self == IndexTypeEnum.STRING:
            return IndexType.STRING
        elif self == IndexTypeEnum.GEO2D_SPHERE:
            return IndexType.GEO2D_SPHERE
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
    
    Tracks filter eligibility similar to JFC's ExpressionContainer.
    """
    op: OperationType
    left: Optional["ExpressionNode"] = None
    right: Optional["ExpressionNode"] = None
    # For leaf nodes (comparisons)
    bin_name: Optional[str] = None
    value: Any = None
    value_type: Optional[IndexTypeEnum] = None
    # Filter eligibility tracking
    has_secondary_index_filter: bool = False
    is_excl_from_secondary_index_filter: bool = False
    # Original DSL string for this part (for Exp generation)
    dsl_fragment: Optional[str] = None


class ExpressionTreeBuilder:
    """Builds expression tree from DSL string with proper operator precedence."""
    
    def __init__(self, dsl_string: str):
        self.dsl = dsl_string
        self.pos = 0
    
    def parse(self) -> Optional[ExpressionNode]:
        """Parse DSL string into expression tree."""
        self.pos = 0
        self.skip_whitespace()
        if self.pos >= len(self.dsl):
            return None
        return self._parse_or()
    
    def skip_whitespace(self):
        while self.pos < len(self.dsl) and self.dsl[self.pos] in ' \t\n':
            self.pos += 1
    
    def _parse_or(self) -> Optional[ExpressionNode]:
        """Parse OR expressions (lowest precedence)."""
        left = self._parse_and()
        if left is None:
            return None
        
        parts = [left]
        while True:
            self.skip_whitespace()
            if self._match_keyword("or"):
                right = self._parse_and()
                if right:
                    parts.append(right)
            else:
                break
        
        if len(parts) == 1:
            return parts[0]
        
        # Build right-associative OR tree
        result = parts[-1]
        for i in range(len(parts) - 2, -1, -1):
            result = ExpressionNode(op=OperationType.OR, left=parts[i], right=result)
        return result
    
    def _parse_and(self) -> Optional[ExpressionNode]:
        """Parse AND expressions (higher precedence than OR)."""
        left = self._parse_unary()
        if left is None:
            return None
        
        parts = [left]
        while True:
            self.skip_whitespace()
            if self._match_keyword("and"):
                right = self._parse_unary()
                if right:
                    parts.append(right)
            else:
                break
        
        if len(parts) == 1:
            return parts[0]
        
        # Build right-associative AND tree
        result = parts[-1]
        for i in range(len(parts) - 2, -1, -1):
            result = ExpressionNode(op=OperationType.AND, left=parts[i], right=result)
        return result
    
    def _parse_unary(self) -> Optional[ExpressionNode]:
        """Parse unary NOT and parenthesized expressions."""
        self.skip_whitespace()
        
        # Handle NOT
        if self._match_keyword("not"):
            operand = self._parse_unary()
            return ExpressionNode(op=OperationType.NOT, left=operand)
        
        # Handle parentheses
        if self.pos < len(self.dsl) and self.dsl[self.pos] == '(':
            self.pos += 1
            result = self._parse_or()
            self.skip_whitespace()
            if self.pos < len(self.dsl) and self.dsl[self.pos] == ')':
                self.pos += 1
            return result
        
        return self._parse_comparison()
    
    def _parse_comparison(self) -> Optional[ExpressionNode]:
        """Parse comparison expressions ($.bin op value) or (value op $.bin)."""
        self.skip_whitespace()
        start_pos = self.pos

        # Try $.bin op value first
        bin_name = self._parse_bin_ref()
        if bin_name is not None:
            self.skip_whitespace()
            op = self._parse_operator()
            if op is not None:
                self.skip_whitespace()
                value, value_type = self._parse_value()
                end_pos = self.pos
                dsl_fragment = self.dsl[start_pos:end_pos].strip()
                return ExpressionNode(
                    op=op,
                    bin_name=bin_name,
                    value=value,
                    value_type=value_type,
                    dsl_fragment=dsl_fragment
                )

        # Try value op $.bin (normalize to $.bin op value by flipping operator)
        if self._can_start_value():
            saved = self.pos
            value, value_type = self._parse_value()
            self.skip_whitespace()
            op = self._parse_operator()
            if op is not None:
                self.skip_whitespace()
                bin_name = self._parse_bin_ref()
                if bin_name is not None:
                    flipped = self._flip_op(op)
                    end_pos = self.pos
                    dsl_fragment = self.dsl[start_pos:end_pos].strip()
                    return ExpressionNode(
                        op=flipped,
                        bin_name=bin_name,
                        value=value,
                        value_type=value_type,
                        dsl_fragment=dsl_fragment
                    )
            self.pos = saved

        return None
    
    def _parse_bin_ref(self) -> Optional[str]:
        """Parse $.binName or $.binName.get(type: TYPE) reference; returns bin name only."""
        self.skip_whitespace()
        if not self.dsl[self.pos:].startswith('$.'):
            return None
        self.pos += 2

        # Read bin name
        start = self.pos
        while self.pos < len(self.dsl) and (self.dsl[self.pos].isalnum() or self.dsl[self.pos] == '_'):
            self.pos += 1

        bin_name = self.dsl[start:self.pos]
        if not bin_name:
            return None

        # Optional .get(type: INT) or .get(type: STRING) etc. so operator is next
        self.skip_whitespace()
        if self.pos + 10 <= len(self.dsl) and self.dsl[self.pos : self.pos + 10] == '.get(type:':
            self.pos += 10
            self.skip_whitespace()
            while self.pos < len(self.dsl) and self.dsl[self.pos].isalpha():
                self.pos += 1
            self.skip_whitespace()
            if self.pos < len(self.dsl) and self.dsl[self.pos] == ')':
                self.pos += 1

        return bin_name
    
    def _can_start_value(self) -> bool:
        """Return True if current position can start a value (string or number)."""
        self.skip_whitespace()
        if self.pos >= len(self.dsl):
            return False
        c = self.dsl[self.pos]
        if c in ('"', "'"):
            return True
        if c.isdigit():
            return True
        if c == '-' and self.pos + 1 < len(self.dsl) and (
            self.dsl[self.pos + 1].isdigit() or self.dsl[self.pos + 1] == '.'
        ):
            return True
        return False

    def _flip_op(self, op: OperationType) -> OperationType:
        """Flip operator for value op $.bin -> $.bin op value (e.g. 5 < $.x -> $.x > 5)."""
        if op == OperationType.LT:
            return OperationType.GT
        if op == OperationType.LE:
            return OperationType.GE
        if op == OperationType.GT:
            return OperationType.LT
        if op == OperationType.GE:
            return OperationType.LE
        return op  # EQ, NE unchanged

    def _parse_operator(self) -> Optional[OperationType]:
        """Parse comparison operator."""
        ops = [
            ("==", OperationType.EQ),
            ("!=", OperationType.NE),
            (">=", OperationType.GE),
            ("<=", OperationType.LE),
            (">", OperationType.GT),
            ("<", OperationType.LT),
        ]
        for op_str, op_type in ops:
            if self.dsl[self.pos:].startswith(op_str):
                self.pos += len(op_str)
                return op_type
        return None
    
    def _parse_value(self) -> Tuple[Any, Optional[IndexTypeEnum]]:
        """Parse value (int, float, string)."""
        self.skip_whitespace()
        
        # String value
        if self.dsl[self.pos] in ('"', "'"):
            quote = self.dsl[self.pos]
            self.pos += 1
            start = self.pos
            while self.pos < len(self.dsl) and self.dsl[self.pos] != quote:
                self.pos += 1
            value = self.dsl[start:self.pos]
            self.pos += 1  # Skip closing quote
            return value, IndexTypeEnum.STRING
        
        # Numeric value
        start = self.pos
        has_dot = False
        if self.dsl[self.pos] == '-':
            self.pos += 1
        while self.pos < len(self.dsl) and (self.dsl[self.pos].isdigit() or self.dsl[self.pos] == '.'):
            if self.dsl[self.pos] == '.':
                has_dot = True
            self.pos += 1
        
        value_str = self.dsl[start:self.pos]
        if has_dot:
            return float(value_str), IndexTypeEnum.NUMERIC
        return int(value_str), IndexTypeEnum.NUMERIC
    
    def _match_keyword(self, keyword: str) -> bool:
        """Try to match a keyword (case insensitive, word boundary)."""
        self.skip_whitespace()
        remaining = self.dsl[self.pos:].lower()
        if remaining.startswith(keyword.lower()):
            # Check word boundary
            next_pos = self.pos + len(keyword)
            if next_pos >= len(self.dsl) or not self.dsl[next_pos].isalnum():
                self.pos = next_pos
                return True
        return False


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
        from aerospike_fluent.dsl.arithmetic_filter import try_arithmetic_filter

        arith = try_arithmetic_filter(dsl_string.strip())
        if arith is not None and self._indexes_by_bin and arith.range_min <= arith.range_max:
            indexes_for_bin = self._indexes_by_bin.get(arith.bin_name, [])
            if any(idx.index_type == IndexTypeEnum.NUMERIC for idx in indexes_for_bin):
                return ParseResult(
                    filter=Filter.range(arith.bin_name, arith.range_min, arith.range_max),
                    exp=None,
                )

        # Build expression tree
        builder = ExpressionTreeBuilder(dsl_string)
        tree = builder.parse()
        
        if tree is None:
            return ParseResult(filter=None, exp=parse_dsl(dsl_string, placeholder_values))
        
        # No indexes - return full expression as Exp
        if not self._indexes_by_bin:
            return ParseResult(filter=None, exp=parse_dsl(dsl_string, placeholder_values))
        
        # Mark nodes excluded from filter (under OR)
        self._mark_excluded_nodes(tree)
        
        # Collect filterable expressions by cardinality
        exprs_by_cardinality: Dict[float, List[ExpressionNode]] = {}
        self._collect_filterable_expressions(tree, exprs_by_cardinality)
        
        if not exprs_by_cardinality:
            return ParseResult(filter=None, exp=parse_dsl(dsl_string, placeholder_values))
        
        # Choose best expression (highest cardinality, then alphabetical)
        best_cardinality = max(exprs_by_cardinality.keys())
        candidates = exprs_by_cardinality[best_cardinality]
        
        # Sort alphabetically by bin name for consistent selection
        candidates.sort(key=lambda n: n.bin_name or "")
        chosen = candidates[0]
        chosen.has_secondary_index_filter = True
        
        # Create Filter
        filter_obj = self._create_filter(chosen)
        
        # Generate complementary Exp
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
            # Check if we have a matching index
            cardinality = self._get_index_cardinality(node.bin_name, node.value_type, node.op)
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
        op: OperationType
    ) -> Optional[float]:
        """Get cardinality of matching index, or None if no match."""
        if bin_name not in self._indexes_by_bin:
            return None
        
        # String comparisons (>, <, etc.) not supported by secondary index
        if value_type == IndexTypeEnum.STRING and op in (OperationType.GT, OperationType.GE, 
                                                          OperationType.LT, OperationType.LE):
            return None
        
        for index in self._indexes_by_bin[bin_name]:
            if index.index_type == value_type:
                return index.bin_values_ratio if index.bin_values_ratio is not None else -1
        
        return None
    
    def _create_filter(self, node: ExpressionNode) -> Optional[Filter]:
        """Create Filter from expression node."""
        if node.bin_name is None or node.value is None:
            return None
        
        if node.op == OperationType.EQ:
            return Filter.equal(node.bin_name, node.value)
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
