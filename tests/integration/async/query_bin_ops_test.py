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

"""Integration tests for query bin-level read operations and stacking.

Coverage:
  - Simple bin reads (get, multiple bins)
  - CDT map reads (key, index range, rank)
  - CDT list reads (index, rank)
  - Batch key queries with bin ops
  - Dataset query with bin ops -> OP_NOT_APPLICABLE
  - Query stacking
"""

import pytest
import pytest_asyncio

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode
from aerospike_fluent import FluentClient
from aerospike_fluent.exceptions import AerospikeError


KEY_PREFIX = "qbops_"
NS = "test"
SET = "query_bin_ops"


@pytest_asyncio.fixture
async def client(aerospike_host, client_policy):
    """Setup fluent client, seed test data, yield the client."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        for i in range(1, 4):
            settings = {"theme": "dark", "volume": i * 10, "notifications": True}
            scores = [i * 10, i * 20, i * 30]
            async with client.key_value_service(NS, SET) as kv:
                await kv.put(
                    f"{KEY_PREFIX}{i}",
                    {
                        "name": f"user{i}",
                        "age": 20 + i,
                        "score": i * 100,
                        "settings": settings,
                        "scores": scores,
                    },
                )

        yield client


def _key(i: int) -> Key:
    return Key(NS, SET, f"{KEY_PREFIX}{i}")


# ===================================================================
# Simple bin reads
# ===================================================================

class TestSimpleBinReads:

    @pytest.mark.asyncio
    async def test_get_single_bin(self, client):
        rs = await client.query(key=_key(1)).bin("name").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["name"] == "user1"

    @pytest.mark.asyncio
    async def test_get_multiple_bins(self, client):
        rs = await (
            client.query(key=_key(1))
                .bin("name").get()
                .bin("age").get()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age"] == 21

    @pytest.mark.asyncio
    async def test_map_size(self, client):
        rs = await client.query(key=_key(1)).bin("settings").map_size().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == 3

    @pytest.mark.asyncio
    async def test_list_size(self, client):
        rs = await client.query(key=_key(1)).bin("scores").list_size().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 3


# ===================================================================
# CDT map reads
# ===================================================================

class TestCdtMapReads:

    @pytest.mark.asyncio
    async def test_map_key_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_key("theme").get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == "dark"

    @pytest.mark.asyncio
    async def test_map_key_count(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_key("theme").count()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == 1

    @pytest.mark.asyncio
    async def test_map_index_range_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_index_range(0, 2).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    @pytest.mark.asyncio
    async def test_map_rank_get_values(self, client):
        rs = await (
            client.query(key=_key(2)).bin("settings").on_map_rank(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] is not None


# ===================================================================
# CDT list reads
# ===================================================================

class TestCdtListReads:

    @pytest.mark.asyncio
    async def test_list_index_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").on_list_index(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 10

    @pytest.mark.asyncio
    async def test_list_index_count(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").on_list_index(0).count()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 1

    @pytest.mark.asyncio
    async def test_list_rank_get_values(self, client):
        """Rank 0 = lowest value; for key 2 scores=[20,40,60], lowest=20."""
        rs = await (
            client.query(key=_key(2)).bin("scores").on_list_rank(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 20


# ===================================================================
# Batch key queries
# ===================================================================

class TestBatchKeyQueries:

    @pytest.mark.asyncio
    async def test_batch_bin_get(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2)]).bin("name").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        names = {r.record.bins["name"] for r in results if r.is_ok}
        assert names == {"user1", "user2"}

    @pytest.mark.asyncio
    async def test_batch_cdt_map_read(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2), _key(3)])
                .bin("settings").on_map_key("theme").get_values()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["settings"] == "dark"


# ===================================================================
# Dataset query with bin ops -> OP_NOT_APPLICABLE
# ===================================================================

class TestDatasetQueryGuard:

    @pytest.mark.asyncio
    async def test_dataset_query_with_bin_ops_raises(self, client):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                client.query(NS, SET).bin("settings").on_map_key("theme").get_values()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE

    @pytest.mark.asyncio
    async def test_dataset_query_with_get_raises(self, client):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                client.query(NS, SET).bin("name").get()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


# ===================================================================
# Query stacking
# ===================================================================

class TestQueryStacking:

    @pytest.mark.asyncio
    async def test_stack_two_point_queries(self, client):
        rs = await (
            client
                .query(key=_key(1)).bin("name").get()
                .query(_key(2)).bin("age").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        bins_by_key = {}
        for r in results:
            assert r.is_ok
            digest = r.key.digest
            bins_by_key[digest] = r.record.bins
        digests = list(bins_by_key.keys())
        assert len(digests) == 2

    @pytest.mark.asyncio
    async def test_stack_batch_queries(self, client):
        rs = await (
            client
                .query(keys=[_key(1), _key(2)]).bin("name").get()
                .query([_key(3)]).bin("age").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_dataset_query_cannot_stack(self, client):
        with pytest.raises(ValueError, match="cannot be stacked"):
            client.query(NS, SET).query(_key(1))

    @pytest.mark.asyncio
    async def test_stacked_read_complex(self, client):
        """Stacked query mixing specific bins, all bins, no bins,
        select_from, missing bin, and missing key."""
        rs = await (
            client
                .query(key=_key(1)).bins(["name"])
                .query(_key(2))
                .query(_key(3)).with_no_bins()
                .query(_key(1)).bin("score").select_from("$.score * 8")
                .query(_key(2)).bins(["binnotfound"])
                .query(Key(NS, SET, f"{KEY_PREFIX}999")).bins(["name"])
                .execute()
        )
        results = await rs.collect()
        # Missing key (key999) is excluded from stream by default
        assert len(results) == 5

        # key1 — specific bin
        r = results[0]
        assert r.is_ok
        assert r.record.bins["name"] == "user1"
        assert "age" not in r.record.bins

        # key2 — all bins
        r = results[1]
        assert r.is_ok
        assert "name" in r.record.bins
        assert "age" in r.record.bins

        # key3 — no bins (header only)
        r = results[2]
        assert r.is_ok
        assert r.record.bins == {}

        # key1 — select_from expression
        r = results[3]
        assert r.is_ok
        assert r.record.bins["score"] == 800

        # key2 — missing bin returns empty
        r = results[4]
        assert r.is_ok
        assert "binnotfound" not in r.record.bins


# ===================================================================
# Inverted reads
# ===================================================================

class TestInvertedReads:

    @pytest.mark.asyncio
    async def test_map_key_range_get_all_other_values(self, client):
        """Get all map values EXCEPT those in the range."""
        rs = await (
            client.query(key=_key(1))
                .bin("settings").on_map_key_range("theme", "volume").get_all_other_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        # Key range ["theme", "volume") is exclusive on upper bound, so
        # "theme" is in the range but "volume" is NOT.  Inverted returns
        # values for "notifications" and "volume".
        assert len(vals) == 2

    @pytest.mark.asyncio
    async def test_list_value_get_all_other_values(self, client):
        """Get all list elements EXCEPT those matching the value."""
        rs = await (
            client.query(key=_key(1))
                .bin("scores").on_list_value(10).get_all_other_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["scores"]
        assert isinstance(vals, list)
        assert 10 not in vals
        assert 20 in vals
        assert 30 in vals
