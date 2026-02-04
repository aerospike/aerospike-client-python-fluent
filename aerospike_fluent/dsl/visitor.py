"""DSL Visitor - Converts ANTLR parse tree to FilterExpression objects.

This module contains the visitor that walks the ANTLR parse tree and converts
it into Aerospike FilterExpression objects.

Type Inference:
    Bin types are inferred from the comparison operand. For example,
    `$.A == 1` will use int_bin("A") because 1 is an integer. Explicit
    casts like `$.A.asInt()` override inference.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, List, Optional, Sequence, Union

from aerospike_async import (
    CTX,
    ExpType,
    FilterExpression,
    ListReturnType,
    MapReturnType,
)

from aerospike_fluent.dsl.antlr4.generated.ConditionParser import ConditionParser
from aerospike_fluent.dsl.antlr4.generated.ConditionVisitor import ConditionVisitor
from aerospike_fluent.dsl.exceptions import DslParseException


class InferredType(Enum):
    """Type hints for expression inference."""
    INT = auto()
    FLOAT = auto()
    STRING = auto()
    BOOL = auto()
    BLOB = auto()
    LIST = auto()
    MAP = auto()
    UNKNOWN = auto()


@dataclass
class DeferredBin:
    """Represents a bin reference that hasn't been typed yet.
    
    The bin type will be inferred from context (comparison operand) or
    defaulted to INT.
    """
    name: str
    explicit_type: Optional[InferredType] = None
    
    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert to FilterExpression with the given type.

        If explicit_type is set (via .asInt(), .asFloat(), etc.), use that.
        Otherwise use the inferred type.
        """
        type_to_use = self.explicit_type if self.explicit_type else inferred_type

        if type_to_use == InferredType.INT:
            return FilterExpression.int_bin(self.name)
        elif type_to_use == InferredType.FLOAT:
            return FilterExpression.float_bin(self.name)
        elif type_to_use == InferredType.BOOL:
            return FilterExpression.bool_bin(self.name)
        elif type_to_use == InferredType.STRING:
            return FilterExpression.string_bin(self.name)
        elif type_to_use == InferredType.BLOB:
            return FilterExpression.blob_bin(self.name)
        elif type_to_use == InferredType.LIST:
            return FilterExpression.list_bin(self.name)
        elif type_to_use == InferredType.MAP:
            return FilterExpression.map_bin(self.name)
        else:
            return FilterExpression.int_bin(self.name)


@dataclass
class TypedExpr:
    """Wrapper for FilterExpression with type hint for inference.

    Since FilterExpression is a PyO3 object that doesn't allow attribute setting,
    we wrap it with type information for inference during parsing.
    """
    expr: FilterExpression
    type_hint: InferredType


class ArithOp(Enum):
    """Arithmetic operation types."""
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()
    MOD = auto()


@dataclass
class DeferredArithmetic:
    """Represents an arithmetic expression with deferred type resolution.

    The type is determined from context (comparison operand) at the point
    where the expression is used, allowing float inference to propagate.
    """
    op: ArithOp
    operands: List[Any]  # Can contain DeferredBin, DeferredArithmetic, or FilterExpression

    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert to FilterExpression with the given type."""
        resolved_operands = []
        for operand in self.operands:
            if isinstance(operand, DeferredBin):
                resolved_operands.append(operand.to_expression(inferred_type))
            elif isinstance(operand, DeferredArithmetic):
                resolved_operands.append(operand.to_expression(inferred_type))
            elif isinstance(operand, TypedExpr):
                resolved_operands.append(operand.expr)
            else:
                resolved_operands.append(operand)

        if self.op == ArithOp.ADD:
            return FilterExpression.num_add(resolved_operands)
        elif self.op == ArithOp.SUB:
            return FilterExpression.num_sub(resolved_operands)
        elif self.op == ArithOp.MUL:
            return FilterExpression.num_mul(resolved_operands)
        elif self.op == ArithOp.DIV:
            return FilterExpression.num_div(resolved_operands)
        elif self.op == ArithOp.MOD:
            return FilterExpression.num_mod(resolved_operands[0], resolved_operands[1])
        else:
            raise DslParseException(f"Unknown arithmetic operation: {self.op}")


# =============================================================================
# CDT Path Parts - Represent list/map access in paths like $.bin[0] or $.bin.key
# =============================================================================

class CDTPart(ABC):
    """Abstract base class for CDT path parts."""
    
    @abstractmethod
    def get_context(self) -> CTX:
        """Get the CTX for this path part (used for nested operations)."""
        pass
    
    @abstractmethod
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        """Construct the final FilterExpression for this CDT access.
        
        Args:
            bin_name: The name of the bin containing the CDT
            value_type: The expected type of the value being accessed
            return_type: The return type for the CDT operation
            ctx: Context array for nested operations (preceding parts)
            bin_expr: Optional pre-built bin expression (for mixed CDT paths)
        """
        pass


@dataclass
class ListIndexPart(CDTPart):
    """List access by index: $.myList[0] or $.myList[-1]"""
    index: int
    
    def get_context(self) -> CTX:
        return CTX.list_index(self.index)
    
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.list_bin(bin_name)
        return FilterExpression.list_get_by_index(
            return_type,
            value_type,
            FilterExpression.int_val(self.index),
            bin_expr,
            list(ctx),
        )


@dataclass
class ListRankPart(CDTPart):
    """List access by rank: $.myList[#0] (smallest) or $.myList[#-1] (largest)"""
    rank: int

    def get_context(self) -> CTX:
        return CTX.list_rank(self.rank)

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.list_bin(bin_name)
        return FilterExpression.list_get_by_rank(
            return_type,
            value_type,
            FilterExpression.int_val(self.rank),
            bin_expr,
            list(ctx),
        )


@dataclass
class ListValuePart(CDTPart):
    """List access by value: $.myList.[=100]"""
    value: Any
    inverted: bool = False

    def get_context(self) -> CTX:
        # Value-based access doesn't have a simple context
        raise NotImplementedError("ListValuePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        # Determine value expression based on type
        if isinstance(self.value, int):
            val_expr = FilterExpression.int_val(self.value)
        elif isinstance(self.value, float):
            val_expr = FilterExpression.float_val(self.value)
        elif isinstance(self.value, str):
            val_expr = FilterExpression.string_val(self.value)
        elif isinstance(self.value, bool):
            val_expr = FilterExpression.bool_val(self.value)
        else:
            val_expr = FilterExpression.int_val(int(self.value))
        return FilterExpression.list_get_by_value(
            return_type,
            val_expr,
            FilterExpression.list_bin(bin_name),
            list(ctx),
        )


@dataclass
class ListIndexRangePart(CDTPart):
    """List access by index range: $.myList.[1:3] or $.myList.[1:]"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListIndexRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        if self.count is not None:
            return FilterExpression.list_get_by_index_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                FilterExpression.list_bin(bin_name),
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_index_range(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.list_bin(bin_name),
                list(ctx),
            )


@dataclass
class ListValueRangePart(CDTPart):
    """List access by value range: $.myList.[=10:20] or $.myList.[=10:]"""
    value_begin: Optional[Any] = None
    value_end: Optional[Any] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListValueRangePart cannot be used as context")

    def _make_val_expr(self, value: Any) -> Optional[FilterExpression]:
        if value is None:
            return None
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.int_val(int(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        return FilterExpression.list_get_by_value_range(
            return_type,
            self._make_val_expr(self.value_begin),
            self._make_val_expr(self.value_end),
            bin_expr if bin_expr else FilterExpression.list_bin(bin_name),
            list(ctx),
        )


@dataclass
class ListValueListPart(CDTPart):
    """List access by value list: $.myList.[=a,b,c]"""
    values: List[Any] = field(default_factory=list)
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListValueListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        # Create a list value expression
        return FilterExpression.list_get_by_value_list(
            return_type,
            FilterExpression.list_val(self.values),
            bin_expr if bin_expr else FilterExpression.list_bin(bin_name),
            list(ctx),
        )


@dataclass
class ListRankRangePart(CDTPart):
    """List access by rank range: $.myList.[#0:3] or $.myList.[#-3:]"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListRankRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.list_bin(bin_name)
        if self.count is not None:
            return FilterExpression.list_get_by_rank_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_rank_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class MapKeyPart(CDTPart):
    """Map access by key: $.myMap.key or $.myMap["key"]"""
    key: str
    
    def get_context(self) -> CTX:
        return CTX.map_key(self.key)
    
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.map_bin(bin_name)
        return FilterExpression.map_get_by_key(
            return_type,
            value_type,
            FilterExpression.string_val(self.key),
            bin_expr,
            list(ctx),
        )


@dataclass
class MapIndexPart(CDTPart):
    """Map access by index: $.myMap{0}"""
    index: int
    
    def get_context(self) -> CTX:
        return CTX.map_index(self.index)
    
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        return FilterExpression.map_get_by_index(
            return_type,
            value_type,
            FilterExpression.int_val(self.index),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapRankPart(CDTPart):
    """Map access by rank: $.myMap{#0}"""
    rank: int

    def get_context(self) -> CTX:
        return CTX.map_rank(self.rank)

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        return FilterExpression.map_get_by_rank(
            return_type,
            value_type,
            FilterExpression.int_val(self.rank),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapValuePart(CDTPart):
    """Map access by value: $.myMap.{=100}"""
    value: Any
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValuePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        if isinstance(self.value, int):
            val_expr = FilterExpression.int_val(self.value)
        elif isinstance(self.value, float):
            val_expr = FilterExpression.float_val(self.value)
        elif isinstance(self.value, str):
            val_expr = FilterExpression.string_val(self.value)
        elif isinstance(self.value, bool):
            val_expr = FilterExpression.bool_val(self.value)
        else:
            val_expr = FilterExpression.int_val(int(self.value))
        return FilterExpression.map_get_by_value(
            return_type,
            val_expr,
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapKeyRangePart(CDTPart):
    """Map access by key range: $.myMap.{a-c}"""
    key_begin: Optional[str] = None
    key_end: Optional[str] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapKeyRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        begin_expr = FilterExpression.string_val(self.key_begin) if self.key_begin else None
        end_expr = FilterExpression.string_val(self.key_end) if self.key_end else None
        return FilterExpression.map_get_by_key_range(
            return_type,
            begin_expr,
            end_expr,
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapKeyListPart(CDTPart):
    """Map access by key list: $.myMap.{a,b,c}"""
    keys: List[str] = field(default_factory=list)
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapKeyListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_key_list(
            return_type,
            FilterExpression.list_val(self.keys),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapIndexRangePart(CDTPart):
    """Map access by index range: $.myMap.{1:3}"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapIndexRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_index_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_index_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class MapValueRangePart(CDTPart):
    """Map access by value range: $.myMap.{=10:20}"""
    value_begin: Optional[Any] = None
    value_end: Optional[Any] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValueRangePart cannot be used as context")

    def _make_val_expr(self, value: Any) -> Optional[FilterExpression]:
        if value is None:
            return None
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.int_val(int(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_value_range(
            return_type,
            self._make_val_expr(self.value_begin),
            self._make_val_expr(self.value_end),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapValueListPart(CDTPart):
    """Map access by value list: $.myMap.{=a,b,c}"""
    values: List[Any] = field(default_factory=list)
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValueListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_value_list(
            return_type,
            FilterExpression.list_val(self.values),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapRankRangePart(CDTPart):
    """Map access by rank range: $.myMap.{#0:3}"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapRankRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_rank_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_rank_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class ListRankRangeRelativePart(CDTPart):
    """List access by value-relative rank range: $.list.[#-3:-1~b] (rank -3 to -1 relative to value b)"""
    rank: int
    value: Any
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListRankRangeRelativePart cannot be used as context")

    def _make_val_expr(self, value: Any) -> FilterExpression:
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.string_val(str(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.list_bin(bin_name)
        if self.count is not None:
            return FilterExpression.list_get_by_value_relative_rank_range_count(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_value_relative_rank_range(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                base_bin,
                list(ctx),
            )


@dataclass
class MapRankRangeRelativePart(CDTPart):
    """Map access by value-relative rank range: $.map.{#-1:1~10} (rank -1 to 1 relative to value 10)"""
    rank: int
    value: Any
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapRankRangeRelativePart cannot be used as context")

    def _make_val_expr(self, value: Any) -> FilterExpression:
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.string_val(str(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_value_relative_rank_range_count(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_value_relative_rank_range(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                base_bin,
                list(ctx),
            )


@dataclass
class MapIndexRangeRelativePart(CDTPart):
    """Map access by key-relative index range: $.map.{0:1~a} (index 0 to 1 relative to key a)"""
    index: int
    key: str
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapIndexRangeRelativePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_key_relative_index_range_count(
                return_type,
                FilterExpression.string_val(self.key),
                FilterExpression.int_val(self.index),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_key_relative_index_range(
                return_type,
                FilterExpression.string_val(self.key),
                FilterExpression.int_val(self.index),
                base_bin,
                list(ctx),
            )


@dataclass
class CDTPath:
    """Represents a complete CDT path: bin name + list of CDT parts.
    
    Examples:
        $.myList[0] -> CDTPath("myList", [ListIndexPart(0)])
        $.myMap.key -> CDTPath("myMap", [MapKeyPart("key")])
        $.nested[0].key -> CDTPath("nested", [ListIndexPart(0), MapKeyPart("key")])
    """
    bin_name: str
    parts: List[CDTPart] = field(default_factory=list)
    value_type: ExpType = ExpType.INT  # Default, can be overridden by .get(type:...)
    explicit_type: Optional[InferredType] = None  # For .asInt(), .asFloat() casts
    has_path_function: bool = False  # True if .get(), .asInt(), etc. was applied

    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert the CDT path to a FilterExpression.
        
        If no CDT parts, returns a simple bin expression.
        If CDT parts exist, returns a CDT get expression.
        """
        if not self.parts:
            # No CDT parts - just a bin reference
            type_to_use = self.explicit_type if self.explicit_type else inferred_type
            if type_to_use == InferredType.INT:
                return FilterExpression.int_bin(self.bin_name)
            elif type_to_use == InferredType.FLOAT:
                return FilterExpression.float_bin(self.bin_name)
            elif type_to_use == InferredType.BOOL:
                return FilterExpression.bool_bin(self.bin_name)
            elif type_to_use == InferredType.STRING:
                return FilterExpression.string_bin(self.bin_name)
            else:
                return FilterExpression.int_bin(self.bin_name)
        
        # Has CDT parts - build the expression
        # Build context from all parts except the last one
        ctx: List[CTX] = []
        for part in self.parts[:-1]:
            ctx.append(part.get_context())
        
        # Determine value type from inferred type
        type_to_use = self.explicit_type if self.explicit_type else inferred_type
        if type_to_use == InferredType.INT:
            exp_type = ExpType.INT
        elif type_to_use == InferredType.FLOAT:
            exp_type = ExpType.FLOAT
        elif type_to_use == InferredType.STRING:
            exp_type = ExpType.STRING
        elif type_to_use == InferredType.BOOL:
            exp_type = ExpType.BOOL
        else:
            exp_type = self.value_type  # Use configured value_type
        
        # Determine bin type from FIRST part (not last)
        # If first part is a map access, base bin is map_bin; if list, base bin is list_bin
        first_part = self.parts[0]
        is_map_base = isinstance(first_part, (MapKeyPart, MapIndexPart, MapRankPart,
                                               MapValuePart, MapKeyRangePart, MapKeyListPart,
                                               MapIndexRangePart, MapValueRangePart, MapValueListPart,
                                               MapRankRangePart, MapRankRangeRelativePart,
                                               MapIndexRangeRelativePart))
        
        # Create the appropriate bin expression
        bin_expr = FilterExpression.map_bin(self.bin_name) if is_map_base else FilterExpression.list_bin(self.bin_name)
        
        # Use the last part to construct the expression
        last_part = self.parts[-1]
        if isinstance(last_part, (ListIndexPart, ListRankPart, ListValuePart,
                                   ListIndexRangePart, ListValueRangePart, ListValueListPart,
                                   ListRankRangePart, ListRankRangeRelativePart)):
            return last_part.construct_expr(
                self.bin_name, exp_type, ListReturnType.VALUE, ctx,
                bin_expr=bin_expr
            )
        else:
            return last_part.construct_expr(
                self.bin_name, exp_type, MapReturnType.VALUE, ctx,
                bin_expr=bin_expr
            )


# Type alias for visitor return types (can be deferred, typed, CDT path, or raw expression)
ExprOrDeferred = Union[FilterExpression, DeferredBin, TypedExpr, CDTPath, None]


def _unquote(text: str) -> str:
    """Remove quotes from a quoted string."""
    if (text.startswith("'") and text.endswith("'")) or (text.startswith('"') and text.endswith('"')):
        return text[1:-1]
    return text


def _get_type_hint(expr: ExprOrDeferred) -> InferredType:
    """Get the type hint from an expression for type inference.
    
    Returns the explicit_type if expr is a DeferredBin/CDTPath with one set.
    Returns the type if expr is a TypedExpr with a known type.
    Returns UNKNOWN for raw FilterExpression or None.
    """
    if isinstance(expr, (DeferredBin, CDTPath)):
        # Check for explicit type set via .asInt(), .asFloat(), .get(type: X)
        if expr.explicit_type is not None:
            return expr.explicit_type
        return InferredType.UNKNOWN
    if isinstance(expr, TypedExpr):
        return expr.type_hint
    # Raw FilterExpression - no type info available
    return InferredType.UNKNOWN


def _unwrap_expr(expr: ExprOrDeferred) -> Optional[FilterExpression]:
    """Unwrap a TypedExpr to get the underlying FilterExpression."""
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    return None


def _resolve_deferred(expr: ExprOrDeferred, inferred_type: InferredType) -> FilterExpression:
    """Resolve a DeferredBin, CDTPath, or DeferredArithmetic to a FilterExpression."""
    if isinstance(expr, DeferredBin):
        return expr.to_expression(inferred_type)
    if isinstance(expr, CDTPath):
        return expr.to_expression(inferred_type)
    if isinstance(expr, DeferredArithmetic):
        return expr.to_expression(inferred_type)
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    raise DslParseException("Cannot resolve None expression")


def _resolve_for_comparison(left: ExprOrDeferred, right: ExprOrDeferred) -> tuple[FilterExpression, FilterExpression]:
    """Resolve deferred bins/CDT paths based on comparison operand types.

    If one side is a DeferredBin/CDTPath/DeferredArithmetic and the other has
    a known type, use that type for resolution.
    """
    left_hint = _get_type_hint(left)
    right_hint = _get_type_hint(right)

    # Resolve left side
    if isinstance(left, (DeferredBin, CDTPath, DeferredArithmetic)):
        # Infer from right side, default to INT
        inferred = right_hint if right_hint != InferredType.UNKNOWN else InferredType.INT
        resolved_left = _resolve_deferred(left, inferred)
    elif left is None:
        raise DslParseException("Left operand cannot be None")
    else:
        resolved_left = _unwrap_expr(left)
        if resolved_left is None:
            raise DslParseException("Failed to unwrap left operand")

    # Resolve right side
    if isinstance(right, (DeferredBin, CDTPath, DeferredArithmetic)):
        # Infer from left side, default to INT
        inferred = left_hint if left_hint != InferredType.UNKNOWN else InferredType.INT
        resolved_right = _resolve_deferred(right, inferred)
    elif right is None:
        raise DslParseException("Right operand cannot be None")
    else:
        resolved_right = _unwrap_expr(right)
        if resolved_right is None:
            raise DslParseException("Failed to unwrap right operand")

    return resolved_left, resolved_right


def _resolve_for_arithmetic(expr: ExprOrDeferred, has_float: bool = False) -> FilterExpression:
    """Resolve deferred bin/CDT path for arithmetic operations.

    Arithmetic operations default to INT unless there's a float in the expression.
    """
    if isinstance(expr, (DeferredBin, CDTPath, DeferredArithmetic)):
        inferred = InferredType.FLOAT if has_float else InferredType.INT
        return _resolve_deferred(expr, inferred)
    elif expr is None:
        raise DslParseException("Operand cannot be None in arithmetic expression")

    unwrapped = _unwrap_expr(expr)
    if unwrapped is None:
        raise DslParseException("Failed to unwrap operand in arithmetic expression")
    return unwrapped


def _contains_deferred(expr: ExprOrDeferred) -> bool:
    """Check if expression contains deferred types that need later resolution."""
    if isinstance(expr, (DeferredBin, CDTPath)):
        return True
    if isinstance(expr, DeferredArithmetic):
        return True
    return False


def _require_numeric_operands(op: ArithOp, left: ExprOrDeferred, right: ExprOrDeferred) -> None:
    """Raise DslParseException if either operand has a known non-numeric type."""
    non_numeric = (InferredType.STRING, InferredType.BOOL, InferredType.LIST, InferredType.MAP, InferredType.BLOB)
    left_hint = _get_type_hint(left)
    right_hint = _get_type_hint(right)
    if left_hint in non_numeric or right_hint in non_numeric:
        raise DslParseException(
            "Arithmetic requires numeric operands (INT or FLOAT); got non-numeric type"
        )


def _build_arithmetic(op: ArithOp, left: ExprOrDeferred, right: ExprOrDeferred) -> ExprOrDeferred:
    """Build arithmetic expression, deferring if operands need type inference.

    If either operand contains deferred types (bins without known type),
    returns DeferredArithmetic so the type can be inferred from comparison context.

    If both operands are resolved and one is FLOAT, resolves as FLOAT.
    Otherwise resolves as INT.
    """
    # Check if we need to defer
    if _contains_deferred(left) or _contains_deferred(right):
        return DeferredArithmetic(op, [left, right])

    # Both resolved - require numeric types (reject string, bool, list, map, blob)
    _require_numeric_operands(op, left, right)

    has_float = (_get_type_hint(left) == InferredType.FLOAT or
                 _get_type_hint(right) == InferredType.FLOAT)
    resolved_left = _resolve_for_arithmetic(left, has_float)
    resolved_right = _resolve_for_arithmetic(right, has_float)

    if op == ArithOp.ADD:
        return FilterExpression.num_add([resolved_left, resolved_right])
    elif op == ArithOp.SUB:
        return FilterExpression.num_sub([resolved_left, resolved_right])
    elif op == ArithOp.MUL:
        return FilterExpression.num_mul([resolved_left, resolved_right])
    elif op == ArithOp.DIV:
        return FilterExpression.num_div([resolved_left, resolved_right])
    elif op == ArithOp.MOD:
        return FilterExpression.num_mod(resolved_left, resolved_right)
    else:
        raise DslParseException(f"Unknown arithmetic operation: {op}")


def _finalize_result(result: ExprOrDeferred, default_type: InferredType = InferredType.INT) -> Optional[FilterExpression]:
    """Finalize a visitor result into a FilterExpression.

    Handles DeferredBin, CDTPath, DeferredArithmetic (all default to default_type),
    TypedExpr (unwraps), and raw FilterExpression (pass-through).

    Args:
        result: The visitor result to finalize.
        default_type: Type to use if not explicitly set (default INT).
    """
    if result is None:
        return None
    if isinstance(result, DeferredBin):
        # Use explicit_type if set (via .get(type:X) or .asX()), else use default
        inferred = result.explicit_type if result.explicit_type else default_type
        return result.to_expression(inferred)
    if isinstance(result, CDTPath):
        # Use explicit_type if set, else use default
        inferred = result.explicit_type if result.explicit_type else default_type
        return result.to_expression(inferred)
    if isinstance(result, DeferredArithmetic):
        return result.to_expression(default_type)
    if isinstance(result, TypedExpr):
        return result.expr
    return result


class ExpressionConditionVisitor(ConditionVisitor):
    """Visitor that converts ANTLR parse tree nodes to FilterExpression objects."""
    
    def __init__(self, placeholder_values: Optional[Any] = None, ctx_only: bool = False):
        """Initialize the visitor.

        Args:
            placeholder_values: Optional PlaceholderValues for resolving ?0, ?1, etc.
            ctx_only: If True, don't finalize CDTPath to FilterExpression (for parse_ctx).
        """
        super().__init__()
        self._placeholder_values = placeholder_values
        self._ctx_only = ctx_only

    def visitParse(self, ctx: ConditionParser.ParseContext) -> ExprOrDeferred:
        """Visit the root parse node."""
        result = self.visit(ctx.expression())
        if self._ctx_only:
            return result
        return _finalize_result(result)

    def visitExpression(self, ctx: ConditionParser.ExpressionContext) -> ExprOrDeferred:
        """Visit expression node."""
        return self.visit(ctx.logicalOrExpression())

    def visitOrExpression(self, ctx: ConditionParser.OrExpressionContext) -> ExprOrDeferred:
        """Visit OR expression: expr1 or expr2 or expr3 ...

        Bare bins in logical expressions are inferred as bool_bin.
        """
        if len(ctx.logicalAndExpression()) == 1:
            return self.visit(ctx.logicalAndExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.logicalAndExpression():
            expr = _finalize_result(self.visit(expr_ctx), InferredType.BOOL)
            if expr is None:
                raise DslParseException("Failed to parse expression in OR clause")
            expressions.append(expr)

        if not expressions:
            raise DslParseException("OR expression requires at least one expression")
        return FilterExpression.or_(expressions)

    def visitAndExpression(self, ctx: ConditionParser.AndExpressionContext) -> ExprOrDeferred:
        """Visit AND expression: expr1 and expr2 and expr3 ...

        Bare bins in logical expressions are inferred as bool_bin.
        """
        if len(ctx.basicExpression()) == 1:
            return self.visit(ctx.basicExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.basicExpression():
            expr = _finalize_result(self.visit(expr_ctx), InferredType.BOOL)
            if expr is None:
                raise DslParseException("Failed to parse expression in AND clause")
            expressions.append(expr)

        if not expressions:
            raise DslParseException("AND expression requires at least one expression")
        return FilterExpression.and_(expressions)

    def visitNotExpression(self, ctx: ConditionParser.NotExpressionContext) -> ExprOrDeferred:
        """Visit NOT expression: not (expr)

        Bare bins in logical expressions are inferred as bool_bin.
        """
        expr = _finalize_result(self.visit(ctx.expression()), InferredType.BOOL)
        if expr is None:
            raise DslParseException("Failed to parse expression in NOT clause")
        return FilterExpression.not_(expr)

    def visitEqualityExpression(self, ctx: ConditionParser.EqualityExpressionContext) -> Optional[FilterExpression]:
        """Visit equality expression: left == right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.eq(resolved_left, resolved_right)

    def visitInequalityExpression(self, ctx: ConditionParser.InequalityExpressionContext) -> Optional[FilterExpression]:
        """Visit inequality expression: left != right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.ne(resolved_left, resolved_right)

    def visitGreaterThanExpression(self, ctx: ConditionParser.GreaterThanExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than expression: left > right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.gt(resolved_left, resolved_right)

    def visitGreaterThanOrEqualExpression(self, ctx: ConditionParser.GreaterThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than or equal expression: left >= right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.ge(resolved_left, resolved_right)

    def visitLessThanExpression(self, ctx: ConditionParser.LessThanExpressionContext) -> Optional[FilterExpression]:
        """Visit less than expression: left < right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.lt(resolved_left, resolved_right)

    def visitLessThanOrEqualExpression(self, ctx: ConditionParser.LessThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit less than or equal expression: left <= right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.additiveExpression(0))
        right = self.visit(ctx.additiveExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.le(resolved_left, resolved_right)

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

    def visitBinPart(self, ctx: ConditionParser.BinPartContext) -> ExprOrDeferred:
        """Visit bin part: bin name identifier.
        
        Returns a DeferredBin that will be resolved to the correct bin type
        based on context (comparison operand type) or explicit cast.
        Type defaults to INT.
        """
        bin_name = ctx.NAME_IDENTIFIER().getText()
        return DeferredBin(bin_name)

    def visitPath(self, ctx: ConditionParser.PathContext) -> ExprOrDeferred:
        """Visit path: $.binName or $.binName.pathFunction() or $.binName[0].get(...)"""
        base_path = self.visit(ctx.basePath())
        if base_path is None:
            raise DslParseException("Failed to parse base path")
        
        # Handle path functions like asInt(), asFloat(), exists(), count(), get()
        if ctx.pathFunction() is not None:
            path_func_ctx = ctx.pathFunction()
            
            # Check for cast functions (asInt, asFloat, etc.)
            if hasattr(path_func_ctx, 'pathFunctionCast') and path_func_ctx.pathFunctionCast() is not None:
                cast_ctx = path_func_ctx.pathFunctionCast()
                cast_text = cast_ctx.getText().lower()

                # Handle explicit type casts on DeferredBin or CDTPath
                if isinstance(base_path, (DeferredBin, CDTPath)):
                    if isinstance(base_path, CDTPath):
                        base_path.has_path_function = True
                    if cast_text == "asint()":
                        base_path.explicit_type = InferredType.INT
                        return base_path
                    elif cast_text == "asfloat()":
                        base_path.explicit_type = InferredType.FLOAT
                        return base_path
                    elif cast_text == "asstring()":
                        base_path.explicit_type = InferredType.STRING
                        return base_path
                    elif cast_text == "asbool()":
                        base_path.explicit_type = InferredType.BOOL
                        return base_path
                else:
                    # Already resolved, apply conversion
                    resolved = _finalize_result(base_path)
                    if resolved is None:
                        raise DslParseException("Failed to resolve path for cast")
                    if cast_text == "asint()":
                        return FilterExpression.to_int(resolved)
                    elif cast_text == "asfloat()":
                        return FilterExpression.to_float(resolved)
            
            # Check for get(type:..., return:...)
            if hasattr(path_func_ctx, 'pathFunctionGet') and path_func_ctx.pathFunctionGet() is not None:
                get_ctx = path_func_ctx.pathFunctionGet()
                # Parse get() parameters - extract type for simple bins
                if isinstance(base_path, DeferredBin):
                    self._apply_get_params_to_deferred(base_path, get_ctx)
                elif isinstance(base_path, CDTPath):
                    self._apply_get_params(base_path, get_ctx)
                    base_path.has_path_function = True
            
            # Check for exists()
            if hasattr(path_func_ctx, 'pathFunctionExists') and path_func_ctx.pathFunctionExists() is not None:
                # exists() returns boolean indicating if the path exists
                if isinstance(base_path, CDTPath) and base_path.parts:
                    # For CDT paths, we need bin_exists for the bin
                    return FilterExpression.bin_exists(base_path.bin_name)
                elif isinstance(base_path, (DeferredBin, CDTPath)):
                    bin_name = base_path.bin_name if isinstance(base_path, CDTPath) else base_path.name
                    return FilterExpression.bin_exists(bin_name)
            
            # Check for count()
            if hasattr(path_func_ctx, 'pathFunctionCount') and path_func_ctx.pathFunctionCount() is not None:
                # count() returns the size/count of the list/map or result set
                if isinstance(base_path, CDTPath) and base_path.parts:
                    last_part = base_path.parts[-1]

                    # For list value/range parts, use COUNT return type
                    if isinstance(last_part, (ListValuePart, ListIndexRangePart,
                                              ListValueRangePart, ListValueListPart, ListRankRangePart,
                                              ListRankRangeRelativePart)):
                        ctx_list: List[CTX] = []
                        for p in base_path.parts[:-1]:
                            try:
                                ctx_list.append(p.get_context())
                            except NotImplementedError:
                                pass
                        return last_part.construct_expr(
                            base_path.bin_name,
                            ExpType.INT,
                            ListReturnType.COUNT,
                            ctx_list,
                        )

                    # For map value/range parts, use COUNT return type
                    if isinstance(last_part, (MapValuePart, MapKeyRangePart, MapKeyListPart,
                                              MapIndexRangePart, MapValueRangePart,
                                              MapValueListPart, MapRankRangePart,
                                              MapRankRangeRelativePart, MapIndexRangeRelativePart)):
                        ctx_list = []
                        for p in base_path.parts[:-1]:
                            try:
                                ctx_list.append(p.get_context())
                            except NotImplementedError:
                                pass
                        return last_part.construct_expr(
                            base_path.bin_name,
                            ExpType.INT,
                            MapReturnType.COUNT,
                            ctx_list,
                        )

                    # For simple index/rank parts, get element as LIST and then size it
                    # Build context from all parts except the last
                    ctx_list: List[CTX] = []
                    for p in base_path.parts[:-1]:
                        try:
                            ctx_list.append(p.get_context())
                        except NotImplementedError:
                            pass

                    # Determine bin type from first part
                    first_part = base_path.parts[0]
                    is_map_base = isinstance(first_part, (MapKeyPart, MapIndexPart, MapRankPart,
                                                           MapValuePart, MapKeyRangePart, MapKeyListPart,
                                                           MapIndexRangePart, MapValueRangePart, MapValueListPart,
                                                           MapRankRangePart, MapRankRangeRelativePart,
                                                           MapIndexRangeRelativePart))
                    bin_expr = FilterExpression.map_bin(base_path.bin_name) if is_map_base else FilterExpression.list_bin(base_path.bin_name)

                    if isinstance(last_part, ListIndexPart):
                        # Get the element as LIST type, then get its size
                        inner_expr = FilterExpression.list_get_by_index(
                            ListReturnType.VALUE,
                            ExpType.LIST,
                            FilterExpression.int_val(last_part.index),
                            bin_expr,
                            ctx_list,
                        )
                        return FilterExpression.list_size(inner_expr, [])
                    elif isinstance(last_part, ListRankPart):
                        inner_expr = FilterExpression.list_get_by_rank(
                            ListReturnType.VALUE,
                            ExpType.LIST,
                            FilterExpression.int_val(last_part.rank),
                            bin_expr,
                            ctx_list,
                        )
                        return FilterExpression.list_size(inner_expr, [])
                    elif isinstance(last_part, MapKeyPart):
                        inner_expr = FilterExpression.map_get_by_key(
                            MapReturnType.VALUE,
                            ExpType.LIST,
                            FilterExpression.string_val(last_part.key),
                            bin_expr,
                            ctx_list,
                        )
                        return FilterExpression.list_size(inner_expr, [])
                    elif isinstance(last_part, MapIndexPart):
                        inner_expr = FilterExpression.map_get_by_index(
                            MapReturnType.VALUE,
                            ExpType.LIST,
                            FilterExpression.int_val(last_part.index),
                            bin_expr,
                            ctx_list,
                        )
                        return FilterExpression.list_size(inner_expr, [])
                    elif isinstance(last_part, MapRankPart):
                        inner_expr = FilterExpression.map_get_by_rank(
                            MapReturnType.VALUE,
                            ExpType.LIST,
                            FilterExpression.int_val(last_part.rank),
                            bin_expr,
                            ctx_list,
                        )
                        return FilterExpression.list_size(inner_expr, [])
                elif isinstance(base_path, CDTPath):
                    # No parts - just bin.count()
                    return FilterExpression.list_size(
                        FilterExpression.list_bin(base_path.bin_name),
                        [],
                    )
                elif isinstance(base_path, DeferredBin):
                    # Assume list for bare bin.count()
                    return FilterExpression.list_size(
                        FilterExpression.list_bin(base_path.name),
                        [],
                    )
        
        return base_path
    
    def _apply_get_params(self, cdt_path: CDTPath, get_ctx) -> None:
        """Apply get() function parameters to a CDTPath."""
        if hasattr(get_ctx, 'pathFunctionParams') and get_ctx.pathFunctionParams() is not None:
            params_ctx = get_ctx.pathFunctionParams()
            if hasattr(params_ctx, 'pathFunctionParam'):
                for param_ctx in params_ctx.pathFunctionParam():
                    if hasattr(param_ctx, 'pathFunctionParamName') and hasattr(param_ctx, 'pathFunctionParamValue'):
                        name_ctx = param_ctx.pathFunctionParamName()
                        value_ctx = param_ctx.pathFunctionParamValue()
                        if name_ctx and value_ctx:
                            param_name = name_ctx.getText().lower()
                            param_value = value_ctx.getText().upper()
                            
                            if param_name == "type":
                                # Set value type
                                type_map = {
                                    "INT": ExpType.INT,
                                    "STRING": ExpType.STRING,
                                    "FLOAT": ExpType.FLOAT,
                                    "BOOL": ExpType.BOOL,
                                    "LIST": ExpType.LIST,
                                    "MAP": ExpType.MAP,
                                    "BLOB": ExpType.BLOB,
                                    "GEO": ExpType.GEO,
                                    "HLL": ExpType.HLL,
                                }
                                if param_value in type_map:
                                    cdt_path.value_type = type_map[param_value]
                            # Note: return type is handled by the CDT part's construct_expr

    def _apply_get_params_to_deferred(self, deferred: DeferredBin, get_ctx) -> None:
        """Apply get() function type parameter to a DeferredBin.

        For simple bins like $.name.get(type: STRING), this sets the explicit_type.
        """
        if hasattr(get_ctx, 'pathFunctionParams') and get_ctx.pathFunctionParams() is not None:
            params_ctx = get_ctx.pathFunctionParams()
            if hasattr(params_ctx, 'pathFunctionParam'):
                for param_ctx in params_ctx.pathFunctionParam():
                    if hasattr(param_ctx, 'pathFunctionParamName') and hasattr(param_ctx, 'pathFunctionParamValue'):
                        name_ctx = param_ctx.pathFunctionParamName()
                        value_ctx = param_ctx.pathFunctionParamValue()
                        if name_ctx and value_ctx:
                            param_name = name_ctx.getText().lower()
                            param_value = value_ctx.getText().upper()

                            if param_name == "type":
                                # Set explicit type for bin resolution
                                type_map = {
                                    "INT": InferredType.INT,
                                    "STRING": InferredType.STRING,
                                    "FLOAT": InferredType.FLOAT,
                                    "BOOL": InferredType.BOOL,
                                    "BLOB": InferredType.BLOB,
                                    "LIST": InferredType.LIST,
                                    "MAP": InferredType.MAP,
                                }
                                if param_value in type_map:
                                    deferred.explicit_type = type_map[param_value]

    def visitBasePath(self, ctx: ConditionParser.BasePathContext) -> ExprOrDeferred:
        """Visit base path: binPart with optional CDT parts (list/map access)."""
        # Find the bin part (should be first child)
        bin_name: Optional[str] = None
        cdt_parts: List[CDTPart] = []
        
        for child in ctx.children:
            # Check for bin part (NAME_IDENTIFIER)
            if hasattr(child, 'NAME_IDENTIFIER'):
                bin_name = child.NAME_IDENTIFIER().getText()
            # Check for list part
            elif hasattr(child, 'getRuleIndex'):
                rule_index = child.getRuleIndex()
                # listPart
                if rule_index == ConditionParser.RULE_listPart:
                    part = self._parse_list_part(child)
                    if part:
                        cdt_parts.append(part)
                # mapPart
                elif rule_index == ConditionParser.RULE_mapPart:
                    part = self._parse_map_part(child)
                    if part:
                        cdt_parts.append(part)
        
        if bin_name is None:
            raise DslParseException("Base path must start with a bin name")
        
        # If no CDT parts, return a DeferredBin (for backward compatibility)
        if not cdt_parts:
            return DeferredBin(bin_name)
        
        # Return CDTPath with bin name and parts
        return CDTPath(bin_name=bin_name, parts=cdt_parts)
    
    def _parse_list_part(self, ctx) -> Optional[CDTPart]:
        """Parse a list part from the parse tree."""
        # Check for listIndex: [INT]
        if hasattr(ctx, 'listIndex') and ctx.listIndex() is not None:
            idx_ctx = ctx.listIndex()
            if hasattr(idx_ctx, 'INT') and idx_ctx.INT() is not None:
                index = int(idx_ctx.INT().getText())
                return ListIndexPart(index)

        # Check for listRank: [#INT]
        if hasattr(ctx, 'listRank') and ctx.listRank() is not None:
            rank_ctx = ctx.listRank()
            if hasattr(rank_ctx, 'INT') and rank_ctx.INT() is not None:
                rank = int(rank_ctx.INT().getText())
                return ListRankPart(rank)

        # Check for listValue: [=value]
        if hasattr(ctx, 'listValue') and ctx.listValue() is not None:
            val_ctx = ctx.listValue()
            value = self._parse_value_identifier(val_ctx.valueIdentifier())
            return ListValuePart(value=value)

        # Check for listIndexRange: [start:end] or [!start:end]
        if hasattr(ctx, 'listIndexRange') and ctx.listIndexRange() is not None:
            range_ctx = ctx.listIndexRange()
            inverted = False
            if hasattr(range_ctx, 'standardListIndexRange') and range_ctx.standardListIndexRange() is not None:
                idx_range = range_ctx.standardListIndexRange().indexRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListIndexRange') and range_ctx.invertedListIndexRange() is not None:
                idx_range = range_ctx.invertedListIndexRange().indexRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_index_range(idx_range)
            return ListIndexRangePart(start=start, count=count, inverted=inverted)

        # Check for listValueList: [=a,b,c] or [!=a,b,c]
        if hasattr(ctx, 'listValueList') and ctx.listValueList() is not None:
            list_ctx = ctx.listValueList()
            inverted = False
            if hasattr(list_ctx, 'standardListValueList') and list_ctx.standardListValueList() is not None:
                val_list = list_ctx.standardListValueList().valueListIdentifier()
            elif hasattr(list_ctx, 'invertedListValueList') and list_ctx.invertedListValueList() is not None:
                val_list = list_ctx.invertedListValueList().valueListIdentifier()
                inverted = True
            else:
                return None
            values = self._parse_value_list(val_list)
            return ListValueListPart(values=values, inverted=inverted)

        # Check for listValueRange: [=start:end] or [!=start:end]
        if hasattr(ctx, 'listValueRange') and ctx.listValueRange() is not None:
            range_ctx = ctx.listValueRange()
            inverted = False
            if hasattr(range_ctx, 'standardListValueRange') and range_ctx.standardListValueRange() is not None:
                val_range = range_ctx.standardListValueRange().valueRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListValueRange') and range_ctx.invertedListValueRange() is not None:
                val_range = range_ctx.invertedListValueRange().valueRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_value_range(val_range)
            return ListValueRangePart(value_begin=begin, value_end=end, inverted=inverted)

        # Check for listRankRange: [#start:count] or [!#start:count]
        if hasattr(ctx, 'listRankRange') and ctx.listRankRange() is not None:
            range_ctx = ctx.listRankRange()
            inverted = False
            if hasattr(range_ctx, 'standardListRankRange') and range_ctx.standardListRankRange() is not None:
                rank_range = range_ctx.standardListRankRange().rankRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListRankRange') and range_ctx.invertedListRankRange() is not None:
                rank_range = range_ctx.invertedListRankRange().rankRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_rank_range(rank_range)
            return ListRankRangePart(start=start, count=count, inverted=inverted)

        # Check for listRankRangeRelative: [#start:end~value] or [!#start:end~value]
        if hasattr(ctx, 'listRankRangeRelative') and ctx.listRankRangeRelative() is not None:
            range_ctx = ctx.listRankRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardListRankRangeRelative') and range_ctx.standardListRankRangeRelative() is not None:
                rel_range = range_ctx.standardListRankRangeRelative().rankRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedListRankRangeRelative') and range_ctx.invertedListRankRangeRelative() is not None:
                rel_range = range_ctx.invertedListRankRangeRelative().rankRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            rank, count, value = self._parse_rank_range_relative(rel_range)
            return ListRankRangeRelativePart(rank=rank, value=value, count=count, inverted=inverted)

        # Check for LIST_TYPE_DESIGNATOR: []
        if hasattr(ctx, 'LIST_TYPE_DESIGNATOR') and ctx.LIST_TYPE_DESIGNATOR() is not None:
            return None

        return None

    def _parse_value_identifier(self, ctx) -> Any:
        """Parse a valueIdentifier into a Python value."""
        if ctx is None:
            return None
        text = ctx.getText()
        # Check for negative int
        if hasattr(ctx, 'INT') and ctx.INT() is not None:
            if text.startswith('-'):
                return -int(ctx.INT().getText())
            return int(ctx.INT().getText())
        # Check for quoted string
        if hasattr(ctx, 'QUOTED_STRING') and ctx.QUOTED_STRING() is not None:
            return _unquote(ctx.QUOTED_STRING().getText())
        # Check for name identifier (treat as string)
        if hasattr(ctx, 'NAME_IDENTIFIER') and ctx.NAME_IDENTIFIER() is not None:
            return ctx.NAME_IDENTIFIER().getText()
        # Try to parse as int
        try:
            return int(text)
        except ValueError:
            return text

    def _parse_index_range(self, ctx) -> tuple[int, Optional[int]]:
        """Parse indexRangeIdentifier into (start, count)."""
        if ctx is None:
            return (0, None)
        # Access StartContext and EndContext via getTypedRuleContext
        # (start is shadowed by ANTLR's built-in start token attribute)
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        end_ctx = ctx.end() if hasattr(ctx, 'end') and callable(ctx.end) else None

        start = int(start_ctx.getText()) if start_ctx else 0
        if end_ctx:
            end = int(end_ctx.getText())
            count = end - start
        else:
            count = None
        return (start, count)

    def _parse_rank_range(self, ctx) -> tuple[int, Optional[int]]:
        """Parse rankRangeIdentifier into (start, count)."""
        if ctx is None:
            return (0, None)
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        end_ctx = ctx.end() if hasattr(ctx, 'end') and callable(ctx.end) else None

        start = int(start_ctx.getText()) if start_ctx else 0
        if end_ctx:
            end = int(end_ctx.getText())
            count = end - start
        else:
            count = None
        return (start, count)

    def _parse_value_range(self, ctx) -> tuple[Optional[Any], Optional[Any]]:
        """Parse valueRangeIdentifier into (begin, end)."""
        if ctx is None:
            return (None, None)
        val_ids = ctx.valueIdentifier()
        if len(val_ids) >= 2:
            begin = self._parse_value_identifier(val_ids[0])
            end = self._parse_value_identifier(val_ids[1])
        elif len(val_ids) == 1:
            begin = self._parse_value_identifier(val_ids[0])
            end = None
        else:
            begin = None
            end = None
        return (begin, end)

    def _parse_value_list(self, ctx) -> List[Any]:
        """Parse valueListIdentifier into a list of values."""
        if ctx is None:
            return []
        values = []
        for val_id in ctx.valueIdentifier():
            values.append(self._parse_value_identifier(val_id))
        return values

    def _parse_rank_range_relative(self, ctx) -> tuple[int, Optional[int], Any]:
        """Parse rankRangeRelativeIdentifier into (rank, count, value).
        
        Format: start ':' relativeRankEnd
        where relativeRankEnd is: end relativeValue | relativeValue
        and relativeValue is: '~' valueIdentifier
        """
        if ctx is None:
            return (0, None, None)
        
        # Get start
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        rank = int(start_ctx.getText()) if start_ctx else 0
        
        # Get relativeRankEnd
        rel_end = ctx.relativeRankEnd() if hasattr(ctx, 'relativeRankEnd') else None
        if rel_end is None:
            return (rank, None, None)
        
        # Check for end (count)
        end_ctx = rel_end.end() if hasattr(rel_end, 'end') and callable(rel_end.end) else None
        if end_ctx:
            end = int(end_ctx.getText())
            count = end - rank
        else:
            count = None
        
        # Get relativeValue (~value)
        rel_val = rel_end.relativeValue() if hasattr(rel_end, 'relativeValue') else None
        if rel_val:
            val_id = rel_val.valueIdentifier() if hasattr(rel_val, 'valueIdentifier') else None
            value = self._parse_value_identifier(val_id)
        else:
            value = None
        
        return (rank, count, value)

    def _parse_index_range_relative(self, ctx) -> tuple[int, Optional[int], str]:
        """Parse indexRangeRelativeIdentifier into (index, count, key).
        
        Format: start ':' relativeKeyEnd
        where relativeKeyEnd is: end '~' mapKey | '~' mapKey
        """
        if ctx is None:
            return (0, None, "")
        
        # Get start
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        index = int(start_ctx.getText()) if start_ctx else 0
        
        # Get relativeKeyEnd
        rel_end = ctx.relativeKeyEnd() if hasattr(ctx, 'relativeKeyEnd') else None
        if rel_end is None:
            return (index, None, "")
        
        # Check for end (count)
        end_ctx = rel_end.end() if hasattr(rel_end, 'end') and callable(rel_end.end) else None
        if end_ctx:
            end = int(end_ctx.getText())
            count = end - index
        else:
            count = None
        
        # Get key
        key_ctx = rel_end.mapKey() if hasattr(rel_end, 'mapKey') else None
        if key_ctx:
            key = self._parse_map_key(key_ctx)
        else:
            key = ""
        
        return (index, count, key)
    
    def _parse_map_part(self, ctx) -> Optional[CDTPart]:
        """Parse a map part from the parse tree."""
        # Check for mapKey: NAME_IDENTIFIER or QUOTED_STRING
        if hasattr(ctx, 'mapKey') and ctx.mapKey() is not None:
            key_ctx = ctx.mapKey()
            if hasattr(key_ctx, 'NAME_IDENTIFIER') and key_ctx.NAME_IDENTIFIER() is not None:
                key = key_ctx.NAME_IDENTIFIER().getText()
                return MapKeyPart(key)
            elif hasattr(key_ctx, 'QUOTED_STRING') and key_ctx.QUOTED_STRING() is not None:
                key = _unquote(key_ctx.QUOTED_STRING().getText())
                return MapKeyPart(key)

        # Check for mapIndex: {INT}
        if hasattr(ctx, 'mapIndex') and ctx.mapIndex() is not None:
            idx_ctx = ctx.mapIndex()
            if hasattr(idx_ctx, 'INT') and idx_ctx.INT() is not None:
                index = int(idx_ctx.INT().getText())
                return MapIndexPart(index)

        # Check for mapRank: {#INT}
        if hasattr(ctx, 'mapRank') and ctx.mapRank() is not None:
            rank_ctx = ctx.mapRank()
            if hasattr(rank_ctx, 'INT') and rank_ctx.INT() is not None:
                rank = int(rank_ctx.INT().getText())
                return MapRankPart(rank)

        # Check for mapValue: {=value}
        if hasattr(ctx, 'mapValue') and ctx.mapValue() is not None:
            val_ctx = ctx.mapValue()
            value = self._parse_value_identifier(val_ctx.valueIdentifier())
            return MapValuePart(value=value)

        # Check for mapKeyRange: {key-key} or {!key-key}
        if hasattr(ctx, 'mapKeyRange') and ctx.mapKeyRange() is not None:
            range_ctx = ctx.mapKeyRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapKeyRange') and range_ctx.standardMapKeyRange() is not None:
                key_range = range_ctx.standardMapKeyRange().keyRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapKeyRange') and range_ctx.invertedMapKeyRange() is not None:
                key_range = range_ctx.invertedMapKeyRange().keyRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_key_range(key_range)
            return MapKeyRangePart(key_begin=begin, key_end=end, inverted=inverted)

        # Check for mapKeyList: {a,b,c} or {!a,b,c}
        if hasattr(ctx, 'mapKeyList') and ctx.mapKeyList() is not None:
            list_ctx = ctx.mapKeyList()
            inverted = False
            if hasattr(list_ctx, 'standardMapKeyList') and list_ctx.standardMapKeyList() is not None:
                key_list = list_ctx.standardMapKeyList().keyListIdentifier()
            elif hasattr(list_ctx, 'invertedMapKeyList') and list_ctx.invertedMapKeyList() is not None:
                key_list = list_ctx.invertedMapKeyList().keyListIdentifier()
                inverted = True
            else:
                return None
            keys = self._parse_key_list(key_list)
            return MapKeyListPart(keys=keys, inverted=inverted)

        # Check for mapIndexRange: {start:end} or {!start:end}
        if hasattr(ctx, 'mapIndexRange') and ctx.mapIndexRange() is not None:
            range_ctx = ctx.mapIndexRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapIndexRange') and range_ctx.standardMapIndexRange() is not None:
                idx_range = range_ctx.standardMapIndexRange().indexRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapIndexRange') and range_ctx.invertedMapIndexRange() is not None:
                idx_range = range_ctx.invertedMapIndexRange().indexRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_index_range(idx_range)
            return MapIndexRangePart(start=start, count=count, inverted=inverted)

        # Check for mapValueList: {=a,b,c} or {!=a,b,c}
        if hasattr(ctx, 'mapValueList') and ctx.mapValueList() is not None:
            list_ctx = ctx.mapValueList()
            inverted = False
            if hasattr(list_ctx, 'standardMapValueList') and list_ctx.standardMapValueList() is not None:
                val_list = list_ctx.standardMapValueList().valueListIdentifier()
            elif hasattr(list_ctx, 'invertedMapValueList') and list_ctx.invertedMapValueList() is not None:
                val_list = list_ctx.invertedMapValueList().valueListIdentifier()
                inverted = True
            else:
                return None
            values = self._parse_value_list(val_list)
            return MapValueListPart(values=values, inverted=inverted)

        # Check for mapValueRange: {=start:end} or {!=start:end}
        if hasattr(ctx, 'mapValueRange') and ctx.mapValueRange() is not None:
            range_ctx = ctx.mapValueRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapValueRange') and range_ctx.standardMapValueRange() is not None:
                val_range = range_ctx.standardMapValueRange().valueRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapValueRange') and range_ctx.invertedMapValueRange() is not None:
                val_range = range_ctx.invertedMapValueRange().valueRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_value_range(val_range)
            return MapValueRangePart(value_begin=begin, value_end=end, inverted=inverted)

        # Check for mapRankRange: {#start:count} or {!#start:count}
        if hasattr(ctx, 'mapRankRange') and ctx.mapRankRange() is not None:
            range_ctx = ctx.mapRankRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapRankRange') and range_ctx.standardMapRankRange() is not None:
                rank_range = range_ctx.standardMapRankRange().rankRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapRankRange') and range_ctx.invertedMapRankRange() is not None:
                rank_range = range_ctx.invertedMapRankRange().rankRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_rank_range(rank_range)
            return MapRankRangePart(start=start, count=count, inverted=inverted)

        # Check for mapRankRangeRelative: {#start:end~value} or {!#start:end~value}
        if hasattr(ctx, 'mapRankRangeRelative') and ctx.mapRankRangeRelative() is not None:
            range_ctx = ctx.mapRankRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardMapRankRangeRelative') and range_ctx.standardMapRankRangeRelative() is not None:
                rel_range = range_ctx.standardMapRankRangeRelative().rankRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedMapRankRangeRelative') and range_ctx.invertedMapRankRangeRelative() is not None:
                rel_range = range_ctx.invertedMapRankRangeRelative().rankRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            rank, count, value = self._parse_rank_range_relative(rel_range)
            return MapRankRangeRelativePart(rank=rank, value=value, count=count, inverted=inverted)

        # Check for mapIndexRangeRelative: {start:end~key} or {!start:end~key}
        if hasattr(ctx, 'mapIndexRangeRelative') and ctx.mapIndexRangeRelative() is not None:
            range_ctx = ctx.mapIndexRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardMapIndexRangeRelative') and range_ctx.standardMapIndexRangeRelative() is not None:
                rel_range = range_ctx.standardMapIndexRangeRelative().indexRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedMapIndexRangeRelative') and range_ctx.invertedMapIndexRangeRelative() is not None:
                rel_range = range_ctx.invertedMapIndexRangeRelative().indexRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            index, count, key = self._parse_index_range_relative(rel_range)
            return MapIndexRangeRelativePart(index=index, key=key, count=count, inverted=inverted)

        # Check for MAP_TYPE_DESIGNATOR: {}
        if hasattr(ctx, 'MAP_TYPE_DESIGNATOR') and ctx.MAP_TYPE_DESIGNATOR() is not None:
            return None

        return None

    def _parse_key_range(self, ctx) -> tuple[Optional[str], Optional[str]]:
        """Parse keyRangeIdentifier into (begin, end)."""
        if ctx is None:
            return (None, None)
        # keyRangeIdentifier has mapKey children
        key_ids = ctx.mapKey()
        if len(key_ids) >= 2:
            begin = self._parse_map_key(key_ids[0])
            end = self._parse_map_key(key_ids[1])
        elif len(key_ids) == 1:
            begin = self._parse_map_key(key_ids[0])
            end = None
        else:
            begin = None
            end = None
        return (begin, end)

    def _parse_key_list(self, ctx) -> List[str]:
        """Parse keyListIdentifier into a list of keys."""
        if ctx is None:
            return []
        keys = []
        # keyListIdentifier has mapKey children
        for key_ctx in ctx.mapKey():
            keys.append(self._parse_map_key(key_ctx))
        return keys

    def _parse_map_key(self, ctx) -> str:
        """Parse a mapKey into a string."""
        if ctx is None:
            return ""
        if hasattr(ctx, 'NAME_IDENTIFIER') and ctx.NAME_IDENTIFIER() is not None:
            return ctx.NAME_IDENTIFIER().getText()
        elif hasattr(ctx, 'QUOTED_STRING') and ctx.QUOTED_STRING() is not None:
            return _unquote(ctx.QUOTED_STRING().getText())
        return ctx.getText()

    def visitStringOperand(self, ctx: ConditionParser.StringOperandContext) -> ExprOrDeferred:
        """Visit string operand: 'value' or "value" """
        text = ctx.getText()
        unquoted = _unquote(text)
        expr = FilterExpression.string_val(unquoted)
        return TypedExpr(expr, InferredType.STRING)

    def visitIntOperand(self, ctx: ConditionParser.IntOperandContext) -> ExprOrDeferred:
        """Visit integer operand: 123"""
        text = ctx.INT().getText()
        value = int(text)
        expr = FilterExpression.int_val(value)
        return TypedExpr(expr, InferredType.INT)

    def visitFloatOperand(self, ctx: ConditionParser.FloatOperandContext) -> ExprOrDeferred:
        """Visit float operand: 123.45"""
        text = ctx.FLOAT().getText()
        value = float(text)
        expr = FilterExpression.float_val(value)
        return TypedExpr(expr, InferredType.FLOAT)

    def visitBooleanOperand(self, ctx: ConditionParser.BooleanOperandContext) -> ExprOrDeferred:
        """Visit boolean operand: true or false"""
        text = ctx.getText().lower()
        value = text == "true"
        expr = FilterExpression.bool_val(value)
        return TypedExpr(expr, InferredType.BOOL)

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
        
        # Check for variable reference: ${varName}
        if ctx.variable() is not None:
            return self.visit(ctx.variable())
        
        # Check for list constant: [1, 2, 3]
        if ctx.listConstant() is not None:
            return self.visit(ctx.listConstant())
        
        # Check for map constant: {a: 1, b: 2}
        if ctx.orderedMapConstant() is not None:
            return self.visit(ctx.orderedMapConstant())
        
        # Check for placeholder: ?1, ?2
        if ctx.placeholder() is not None:
            return self.visit(ctx.placeholder())
        
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
            # recordSize maps to device_size in aerospike-async
            return FilterExpression.device_size()
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
        """Visit with expression: with(var1=expr1, var2=expr2) do (action)
        
        Translates to: FilterExpression.exp_let([def1, def2, ..., action])
        
        Example:
            with (x = 1, y = ${x} + 1) do (${x} + ${y})
            ->
            FilterExpression.exp_let([
                FilterExpression.def_("x", FilterExpression.int_val(1)),
                FilterExpression.def_("y", FilterExpression.num_add([FilterExpression.var("x"), FilterExpression.int_val(1)])),
                FilterExpression.num_add([FilterExpression.var("x"), FilterExpression.var("y")])
            ])
        """
        # Collect all variable definitions
        definitions: List[FilterExpression] = []
        for var_def_ctx in ctx.variableDefinition():
            var_name = var_def_ctx.stringOperand().getText()
            # Remove quotes from variable name
            if (var_name.startswith("'") and var_name.endswith("'")) or \
               (var_name.startswith('"') and var_name.endswith('"')):
                var_name = var_name[1:-1]
            
            value_expr = self.visit(var_def_ctx.expression())
            value_expr = _resolve_for_arithmetic(value_expr)
            definitions.append(FilterExpression.def_(var_name, value_expr))
        
        # Get the action expression (the 'do' part)
        action_expr = self.visit(ctx.expression())
        action_expr = _resolve_for_arithmetic(action_expr)
        
        # Build the exp_let call: [def1, def2, ..., action]
        all_exprs = definitions + [action_expr]
        return FilterExpression.exp_let(all_exprs)

    def visitWhenExpression(self, ctx: ConditionParser.WhenExpressionContext) -> Optional[FilterExpression]:
        """Visit when expression: when(cond1=>action1, cond2=>action2, default=>action)
        
        Translates to: FilterExpression.cond([bool1, action1, bool2, action2, ..., default_action])
        
        Example:
            when ($.who == 1 => "bob", $.who == 2 => "fred", default => "other")
            ->
            FilterExpression.cond([
                FilterExpression.eq(FilterExpression.int_bin("who"), FilterExpression.int_val(1)),
                FilterExpression.string_val("bob"),
                FilterExpression.eq(FilterExpression.int_bin("who"), FilterExpression.int_val(2)),
                FilterExpression.string_val("fred"),
                FilterExpression.string_val("other")
            ])
        """
        cond_exprs: List[FilterExpression] = []
        
        # Process each condition => action mapping
        for mapping_ctx in ctx.expressionMapping():
            # expression(0) is the condition, expression(1) is the action
            condition = self.visit(mapping_ctx.expression(0))
            condition = _resolve_for_arithmetic(condition)
            
            action = self.visit(mapping_ctx.expression(1))
            action = _resolve_for_arithmetic(action)
            
            cond_exprs.append(condition)
            cond_exprs.append(action)
        
        # Get the default action (the expression after 'default =>')
        default_action = self.visit(ctx.expression())
        default_action = _resolve_for_arithmetic(default_action)
        cond_exprs.append(default_action)
        
        return FilterExpression.cond(cond_exprs)

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

    def visitAddExpression(self, ctx: ConditionParser.AddExpressionContext) -> ExprOrDeferred:
        """Visit add expression: left + right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        return _build_arithmetic(ArithOp.ADD, left, right)

    def visitSubExpression(self, ctx: ConditionParser.SubExpressionContext) -> ExprOrDeferred:
        """Visit subtract expression: left - right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        return _build_arithmetic(ArithOp.SUB, left, right)

    def visitMulExpression(self, ctx: ConditionParser.MulExpressionContext) -> ExprOrDeferred:
        """Visit multiply expression: left * right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.bitwiseExpression())
        return _build_arithmetic(ArithOp.MUL, left, right)

    def visitDivExpression(self, ctx: ConditionParser.DivExpressionContext) -> ExprOrDeferred:
        """Visit divide expression: left / right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.bitwiseExpression())
        return _build_arithmetic(ArithOp.DIV, left, right)

    def visitModExpression(self, ctx: ConditionParser.ModExpressionContext) -> ExprOrDeferred:
        """Visit modulo expression: left % right

        Modulo always uses INT (no float support).
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.bitwiseExpression())
        # Modulo doesn't support float, resolve immediately as INT
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.num_mod(resolved_left, resolved_right)

    def visitIntAndExpression(self, ctx: ConditionParser.IntAndExpressionContext) -> Optional[FilterExpression]:
        """Visit integer AND expression: left & right
        
        Grammar: bitwiseExpression '&' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_and([resolved_left, resolved_right])

    def visitIntOrExpression(self, ctx: ConditionParser.IntOrExpressionContext) -> Optional[FilterExpression]:
        """Visit integer OR expression: left | right
        
        Grammar: bitwiseExpression '|' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_or([resolved_left, resolved_right])

    def visitIntXorExpression(self, ctx: ConditionParser.IntXorExpressionContext) -> Optional[FilterExpression]:
        """Visit integer XOR expression: left ^ right
        
        Grammar: bitwiseExpression '^' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_xor([resolved_left, resolved_right])

    def visitIntNotExpression(self, ctx: ConditionParser.IntNotExpressionContext) -> Optional[FilterExpression]:
        """Visit integer NOT expression: ~expr
        
        Grammar: '~' shiftExpression
        """
        expr = self.visit(ctx.shiftExpression())
        resolved = _resolve_for_arithmetic(expr, has_float=False)
        return FilterExpression.int_not(resolved)

    def visitIntLShiftExpression(self, ctx: ConditionParser.IntLShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit left shift expression: left << right
        
        Grammar: shiftExpression '<<' operand
        """
        value = self.visit(ctx.shiftExpression())
        shift = self.visit(ctx.operand())
        resolved_value = _resolve_for_arithmetic(value, has_float=False)
        resolved_shift = _resolve_for_arithmetic(shift, has_float=False)
        return FilterExpression.int_lshift(resolved_value, resolved_shift)

    def visitIntRShiftExpression(self, ctx: ConditionParser.IntRShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit right shift expression: left >> right
        
        Grammar: shiftExpression '>>' operand
        """
        value = self.visit(ctx.shiftExpression())
        shift = self.visit(ctx.operand())
        resolved_value = _resolve_for_arithmetic(value, has_float=False)
        resolved_shift = _resolve_for_arithmetic(shift, has_float=False)
        return FilterExpression.int_rshift(resolved_value, resolved_shift)

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

    def visitPlaceholder(self, ctx: ConditionParser.PlaceholderContext) -> ExprOrDeferred:
        """Visit placeholder: ?0, ?1, etc.
        
        Resolves the placeholder to a value expression using the provided PlaceholderValues.
        The type of expression created depends on the Python type of the value:
            - int -> int_val
            - float -> float_val
            - str -> string_val
            - bool -> bool_val
            - bytes -> blob_val
            - list -> list_val
            - dict -> map_val
        """
        if self._placeholder_values is None:
            raise DslParseException("Placeholder used but no PlaceholderValues provided")
        
        # Extract the index from ?0, ?1, etc.
        text = ctx.getText()  # e.g., "?0"
        try:
            index = int(text[1:])  # Remove the '?' prefix
        except ValueError:
            raise DslParseException(f"Invalid placeholder format: {text}")
        
        # Get the value from PlaceholderValues
        value = self._placeholder_values.get(index)
        
        # Convert Python value to FilterExpression based on type
        if isinstance(value, bool):
            # Check bool before int since bool is subclass of int
            return TypedExpr(FilterExpression.bool_val(value), InferredType.BOOL)
        elif isinstance(value, int):
            return TypedExpr(FilterExpression.int_val(value), InferredType.INT)
        elif isinstance(value, float):
            return TypedExpr(FilterExpression.float_val(value), InferredType.FLOAT)
        elif isinstance(value, str):
            return TypedExpr(FilterExpression.string_val(value), InferredType.STRING)
        elif isinstance(value, bytes):
            return TypedExpr(FilterExpression.blob_val(list(value)), InferredType.UNKNOWN)
        elif isinstance(value, (list, tuple)):
            return TypedExpr(FilterExpression.list_val(list(value)), InferredType.UNKNOWN)
        elif isinstance(value, dict):
            return TypedExpr(FilterExpression.map_val(value), InferredType.UNKNOWN)
        else:
            raise DslParseException(f"Unsupported placeholder value type: {type(value).__name__}")

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
