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

"""Unit tests for expression bin operations.

Covers:
- BinBuilder._build_write_flags bitmask construction
- BinBuilder._resolve_expression DSL -> FilterExpression conversion
- BinBuilder expression methods (select_from, insert_from, update_from, upsert_from)
- QueryBinBuilder.select_from
- OP_NOT_APPLICABLE guard on dataset queries with expression ops
- SyncBinBuilder delegation
"""

import pytest
from unittest.mock import MagicMock, patch

from aerospike_async import ExpOperation, FilterExpression, Key
from aerospike_async.exceptions import ResultCode

from aerospike_fluent.aio.operations.key_value import (
    BinBuilder,
    KeyValueOperation,
    _EXP_READ_DEFAULT,
    _EXP_READ_EVAL_NO_FAIL,
    _EXP_WRITE_ALLOW_DELETE,
    _EXP_WRITE_CREATE_ONLY,
    _EXP_WRITE_DEFAULT,
    _EXP_WRITE_EVAL_NO_FAIL,
    _EXP_WRITE_POLICY_NO_FAIL,
    _EXP_WRITE_UPDATE_ONLY,
)
from aerospike_fluent.aio.operations.query import QueryBinBuilder, QueryBuilder
from aerospike_fluent.exceptions import AerospikeError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_kv_operation() -> KeyValueOperation:
    """Create a KeyValueOperation with a mock client for unit testing."""
    return KeyValueOperation(
        client=MagicMock(), namespace="test", set_name="unit", key="k1",
    )


def _make_bin_builder(bin_name: str = "mybin") -> BinBuilder:
    """Create a BinBuilder ready for a .select_from / .*_from call."""
    op = _mock_kv_operation()
    return BinBuilder(op, bin_name)


class _OpCollector:
    """Minimal parent that satisfies the add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


# ===================================================================
# _build_write_flags
# ===================================================================

class TestBuildWriteFlags:

    def test_default_no_options(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_DEFAULT, False, False, False)
        assert flags == _EXP_WRITE_DEFAULT

    def test_create_only_base(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_CREATE_ONLY, False, False, False)
        assert flags == _EXP_WRITE_CREATE_ONLY

    def test_update_only_base(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_UPDATE_ONLY, False, False, False)
        assert flags == _EXP_WRITE_UPDATE_ONLY

    def test_ignore_op_failure(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_DEFAULT, True, False, False)
        assert flags & _EXP_WRITE_POLICY_NO_FAIL

    def test_ignore_eval_failure(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_DEFAULT, False, True, False)
        assert flags & _EXP_WRITE_EVAL_NO_FAIL

    def test_delete_if_null(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_DEFAULT, False, False, True)
        assert flags & _EXP_WRITE_ALLOW_DELETE

    def test_all_options_combined(self):
        flags = BinBuilder._build_write_flags(
            _EXP_WRITE_CREATE_ONLY, True, True, True,
        )
        assert flags & _EXP_WRITE_CREATE_ONLY
        assert flags & _EXP_WRITE_POLICY_NO_FAIL
        assert flags & _EXP_WRITE_EVAL_NO_FAIL
        assert flags & _EXP_WRITE_ALLOW_DELETE

    def test_flags_are_ints(self):
        flags = BinBuilder._build_write_flags(_EXP_WRITE_UPDATE_ONLY, True, True, True)
        assert isinstance(flags, int)


# ===================================================================
# _resolve_expression
# ===================================================================

class TestResolveExpression:

    def test_string_converted_via_parse_dsl(self):
        result = BinBuilder._resolve_expression("$.age + 1")
        assert isinstance(result, FilterExpression)

    def test_filter_expression_passthrough(self):
        from aerospike_fluent.dsl.parser import parse_dsl
        expr = parse_dsl("$.x == 1")
        result = BinBuilder._resolve_expression(expr)
        assert result is expr


# ===================================================================
# BinBuilder expression methods
# ===================================================================

class TestBinBuilderSelectFrom:

    def test_select_from_string(self):
        bb = _make_bin_builder("ev")
        result = bb.select_from("$.A + 4")
        assert result is bb
        assert len(bb._exp_ops) == 1
        assert bb._current_bin is None

    def test_select_from_filter_expression(self):
        from aerospike_fluent.dsl.parser import parse_dsl
        expr = parse_dsl("$.A + 4")
        bb = _make_bin_builder("ev")
        bb.select_from(expr)
        assert len(bb._exp_ops) == 1

    def test_select_from_ignore_eval_failure(self):
        bb = _make_bin_builder("ev")
        bb.select_from("$.A + 4", ignore_eval_failure=True)
        assert len(bb._exp_ops) == 1

    def test_select_from_requires_bin(self):
        op = _mock_kv_operation()
        bb = BinBuilder(op)
        with pytest.raises(ValueError, match="Must call .bin"):
            bb.select_from("$.A + 4")


class TestBinBuilderInsertFrom:

    def test_insert_from_string(self):
        bb = _make_bin_builder("c")
        result = bb.insert_from("$.A + 4")
        assert result is bb
        assert len(bb._exp_ops) == 1

    def test_insert_from_with_flags(self):
        bb = _make_bin_builder("c")
        bb.insert_from("$.A + 4", ignore_op_failure=True, delete_if_null=True)
        assert len(bb._exp_ops) == 1

    def test_insert_from_requires_bin(self):
        op = _mock_kv_operation()
        bb = BinBuilder(op)
        with pytest.raises(ValueError, match="Must call .bin"):
            bb.insert_from("$.A + 4")


class TestBinBuilderUpdateFrom:

    def test_update_from_string(self):
        bb = _make_bin_builder("c")
        result = bb.update_from("$.A + 4")
        assert result is bb
        assert len(bb._exp_ops) == 1

    def test_update_from_with_flags(self):
        bb = _make_bin_builder("c")
        bb.update_from("$.A + 4", ignore_op_failure=True, ignore_eval_failure=True)
        assert len(bb._exp_ops) == 1

    def test_update_from_requires_bin(self):
        op = _mock_kv_operation()
        bb = BinBuilder(op)
        with pytest.raises(ValueError, match="Must call .bin"):
            bb.update_from("$.A + 4")


class TestBinBuilderUpsertFrom:

    def test_upsert_from_string(self):
        bb = _make_bin_builder("c")
        result = bb.upsert_from("$.A + 4")
        assert result is bb
        assert len(bb._exp_ops) == 1

    def test_upsert_from_with_all_flags(self):
        bb = _make_bin_builder("c")
        bb.upsert_from(
            "$.A + 4",
            ignore_op_failure=True,
            ignore_eval_failure=True,
            delete_if_null=True,
        )
        assert len(bb._exp_ops) == 1

    def test_upsert_from_requires_bin(self):
        op = _mock_kv_operation()
        bb = BinBuilder(op)
        with pytest.raises(ValueError, match="Must call .bin"):
            bb.upsert_from("$.A + 4")


class TestBinBuilderChaining:

    def test_mixed_set_to_and_expression(self):
        bb = _make_bin_builder("name")
        bb.set_to("Alice").bin("computed").upsert_from("$.age * 2")
        assert bb._bins == {"name": "Alice"}
        assert len(bb._exp_ops) == 1

    def test_multiple_expression_ops(self):
        bb = _make_bin_builder("c")
        bb.upsert_from("$.A + 4").bin("ev").select_from("$.A + 4")
        assert len(bb._exp_ops) == 2

    def test_write_then_read_expression(self):
        bb = _make_bin_builder("c")
        bb.upsert_from("$.A + 4").bin("c").get()
        assert len(bb._exp_ops) == 1
        assert hasattr(bb, "_gets")


# ===================================================================
# QueryBinBuilder.select_from
# ===================================================================

class TestQueryBinBuilderSelectFrom:

    def test_select_from_string(self):
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        result = qbb.select_from("$.A + 4")
        assert result is collector
        assert len(collector.operations) == 1

    def test_select_from_filter_expression(self):
        from aerospike_fluent.dsl.parser import parse_dsl
        expr = parse_dsl("$.A + 4")
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        qbb.select_from(expr)
        assert len(collector.operations) == 1

    def test_select_from_ignore_eval_failure(self):
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        qbb.select_from("$.A + 4", ignore_eval_failure=True)
        assert len(collector.operations) == 1

    def test_multiple_select_from(self):
        collector = _OpCollector()
        QueryBinBuilder(collector, "r1").select_from("$.A == 0 and $.D == 2")
        QueryBinBuilder(collector, "r2").select_from("$.A == 0 or $.D == 2")
        assert len(collector.operations) == 2


# ===================================================================
# Dataset query guard
# ===================================================================

class TestDatasetQueryGuard:

    @pytest.mark.asyncio
    async def test_select_from_on_dataset_query_raises(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="s")
        qb.bin("ev").select_from("$.A + 4")
        with pytest.raises(AerospikeError) as exc_info:
            await qb.execute()
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


# ===================================================================
# SyncBinBuilder delegation
# ===================================================================

class TestSyncBinBuilderExpression:

    def test_select_from_delegates(self):
        from aerospike_fluent.sync.operations.key_value import SyncBinBuilder, SyncKeyValueOperation
        sync_op = MagicMock(spec=SyncKeyValueOperation)
        async_bb = MagicMock(spec=BinBuilder)
        sync_op._get_async_operation.return_value = MagicMock()

        sbb = SyncBinBuilder(sync_op, "ev")
        sbb._async_builder = async_bb
        result = sbb.select_from("$.A + 4")
        async_bb.select_from.assert_called_once_with("$.A + 4", ignore_eval_failure=False)
        assert result is sbb

    def test_insert_from_delegates(self):
        from aerospike_fluent.sync.operations.key_value import SyncBinBuilder, SyncKeyValueOperation
        sync_op = MagicMock(spec=SyncKeyValueOperation)
        async_bb = MagicMock(spec=BinBuilder)
        sync_op._get_async_operation.return_value = MagicMock()

        sbb = SyncBinBuilder(sync_op, "c")
        sbb._async_builder = async_bb
        sbb.insert_from("$.A + 4", ignore_op_failure=True)
        async_bb.insert_from.assert_called_once_with(
            "$.A + 4", ignore_op_failure=True, ignore_eval_failure=False, delete_if_null=False,
        )

    def test_update_from_delegates(self):
        from aerospike_fluent.sync.operations.key_value import SyncBinBuilder, SyncKeyValueOperation
        sync_op = MagicMock(spec=SyncKeyValueOperation)
        async_bb = MagicMock(spec=BinBuilder)
        sync_op._get_async_operation.return_value = MagicMock()

        sbb = SyncBinBuilder(sync_op, "c")
        sbb._async_builder = async_bb
        sbb.update_from("$.A + 4", delete_if_null=True)
        async_bb.update_from.assert_called_once_with(
            "$.A + 4", ignore_op_failure=False, ignore_eval_failure=False, delete_if_null=True,
        )

    def test_upsert_from_delegates(self):
        from aerospike_fluent.sync.operations.key_value import SyncBinBuilder, SyncKeyValueOperation
        sync_op = MagicMock(spec=SyncKeyValueOperation)
        async_bb = MagicMock(spec=BinBuilder)
        sync_op._get_async_operation.return_value = MagicMock()

        sbb = SyncBinBuilder(sync_op, "c")
        sbb._async_builder = async_bb
        sbb.upsert_from("$.A + 4", ignore_op_failure=True, ignore_eval_failure=True, delete_if_null=True)
        async_bb.upsert_from.assert_called_once_with(
            "$.A + 4", ignore_op_failure=True, ignore_eval_failure=True, delete_if_null=True,
        )


# ===================================================================
# BatchBinBuilder expression methods
# ===================================================================

class TestBatchBinBuilderExpression:

    def _make_batch_builder(self, bin_name: str = "ev"):
        from aerospike_fluent.aio.operations.batch import (
            BatchBinBuilder, BatchKeyOperationBuilder, BatchOperationBuilder, BatchOpType,
        )
        batch = BatchOperationBuilder(client=MagicMock())
        key_op = BatchKeyOperationBuilder(batch, Key("test", "s", "k1"), BatchOpType.UPDATE)
        return BatchBinBuilder(key_op, bin_name), key_op

    def test_select_from_string(self):
        bb, key_op = self._make_batch_builder("ev")
        result = bb.select_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_insert_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.insert_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_update_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.update_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_upsert_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.upsert_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_upsert_from_with_flags(self):
        bb, key_op = self._make_batch_builder("c")
        bb.upsert_from("$.A + 4", ignore_op_failure=True, ignore_eval_failure=True, delete_if_null=True)
        assert len(key_op._operations) == 1

    def test_chaining_set_to_and_expression(self):
        from aerospike_fluent.aio.operations.batch import (
            BatchBinBuilder, BatchKeyOperationBuilder, BatchOperationBuilder, BatchOpType,
        )
        batch = BatchOperationBuilder(client=MagicMock())
        key_op = BatchKeyOperationBuilder(batch, Key("test", "s", "k1"), BatchOpType.UPSERT)
        key_op.bin("name").set_to("Alice").bin("computed").upsert_from("$.age * 2")
        assert len(key_op._operations) == 2
