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

"""Sync integration tests for query bin-level read operations.

Coverage:
  - Simple bin reads (get, multiple bins)
  - Expression reads (select_from)
  - CDT map reads (key, index range, rank)
  - CDT list reads (index, rank)
  - Batch key queries with bin ops
  - Inverted reads
  - Nested CDT read navigation
"""

import pytest

from aerospike_async import Key
from aerospike_fluent import DataSet, SyncFluentClient


KEY_PREFIX = "qbops_"
NS = "test"
SET = "query_bin_ops_sync"


@pytest.fixture
def client(aerospike_host, client_policy):
    with SyncFluentClient(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of(NS, SET)

        for i in range(1, 4):
            settings = {"theme": "dark", "volume": i * 10, "notifications": True}
            scores = [i * 10, i * 20, i * 30]
            nested = {
                "level1": {"a": i * 100, "b": i * 200},
                "level2": {"x": i, "y": i + 1},
            }
            session.upsert(ds.id(f"{KEY_PREFIX}{i}")).put({
                "name": f"user{i}",
                "age": 20 + i,
                "score": i * 100,
                "settings": settings,
                "scores": scores,
                "nested": nested,
            }).execute()

        yield client


def _key(i: int) -> Key:
    return Key(NS, SET, f"{KEY_PREFIX}{i}")


# ===================================================================
# Simple bin reads
# ===================================================================

class TestSimpleBinReads:

    def test_get_single_bin(self, client):
        session = client.create_session()
        result = session.query(_key(1)).bin("name").get().execute().first_or_raise()
        assert result.record.bins["name"] == "user1"

    def test_get_multiple_bins(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("name").get()
                .bin("age").get()
                .execute()
                .first_or_raise()
        )
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age"] == 21

    def test_map_size(self, client):
        session = client.create_session()
        result = session.query(_key(1)).bin("settings").map_size().execute().first_or_raise()
        assert result.record.bins["settings"] == 3

    def test_list_size(self, client):
        session = client.create_session()
        result = session.query(_key(1)).bin("scores").list_size().execute().first_or_raise()
        assert result.record.bins["scores"] == 3


# ===================================================================
# CDT map reads
# ===================================================================

class TestCdtMapReads:

    def test_map_key_get_values(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_key("theme").get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["settings"] == "dark"

    def test_map_key_count(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_key("theme").count()
                .execute().first_or_raise()
        )
        assert result.record.bins["settings"] == 1

    def test_map_index_range_get_values(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_index_range(0, 2).get_values()
                .execute().first_or_raise()
        )
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    def test_map_rank_get_values(self, client):
        session = client.create_session()
        result = (
            session.query(_key(2)).bin("settings").on_map_rank(0).get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["settings"] is not None


# ===================================================================
# CDT list reads
# ===================================================================

class TestCdtListReads:

    def test_list_index_get_values(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1)).bin("scores").on_list_index(0).get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 10

    def test_list_index_count(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1)).bin("scores").on_list_index(0).count()
                .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 1

    def test_list_rank_get_values(self, client):
        """Rank 0 = lowest value; for key 2 scores=[20,40,60], lowest=20."""
        session = client.create_session()
        result = (
            session.query(_key(2)).bin("scores").on_list_rank(0).get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 20


# ===================================================================
# Batch key queries
# ===================================================================

class TestBatchKeyQueries:

    def test_batch_bin_get(self, client):
        session = client.create_session()
        results = (
            session.query([_key(1), _key(2)]).bin("name").get()
                .execute().collect()
        )
        assert len(results) == 2
        names = {r.record.bins["name"] for r in results if r.is_ok}
        assert names == {"user1", "user2"}

    def test_batch_cdt_map_read(self, client):
        session = client.create_session()
        results = (
            session.query([_key(1), _key(2), _key(3)])
                .bin("settings").on_map_key("theme").get_values()
                .execute().collect()
        )
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["settings"] == "dark"


# ===================================================================
# Inverted reads
# ===================================================================

class TestInvertedReads:

    def test_map_key_range_get_all_other_values(self, client):
        """Get all map values EXCEPT those in the range."""
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("settings").on_map_key_range("theme", "volume").get_all_other_values()
                .execute().first_or_raise()
        )
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    def test_list_value_get_all_other_values(self, client):
        """Get all list elements EXCEPT those matching the value."""
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("scores").on_list_value(10).get_all_other_values()
                .execute().first_or_raise()
        )
        vals = result.record.bins["scores"]
        assert isinstance(vals, list)
        assert 10 not in vals
        assert 20 in vals
        assert 30 in vals


# ===================================================================
# Expression reads (select_from)
# ===================================================================

class TestExpressionReads:

    def test_select_from_simple(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("age_plus_20").select_from("$.age + 20")
                .execute().first_or_raise()
        )
        assert result.record.bins["age_plus_20"] == 41

    def test_select_from_multiple(self, client):
        session = client.create_session()
        result = (
            session.query(_key(2))
                .bin("double_age").select_from("$.age * 2")
                .bin("triple_score").select_from("$.score * 3")
                .execute().first_or_raise()
        )
        assert result.record.bins["double_age"] == 44   # (20+2)*2
        assert result.record.bins["triple_score"] == 600  # 200*3

    def test_select_from_with_get(self, client):
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("name").get()
                .bin("age_in_10").select_from("$.age + 10")
                .execute().first_or_raise()
        )
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age_in_10"] == 31


# ===================================================================
# Nested CDT read navigation
# ===================================================================

class TestNestedCdtReads:

    def test_nested_map_key_get_values(self, client):
        """Read a value 2 levels deep: nested.level1.a"""
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 100

    def test_nested_map_key_count(self, client):
        """Count at a nested path should be 1 for a scalar."""
        session = client.create_session()
        result = (
            session.query(_key(1))
                .bin("nested").on_map_key("level1").on_map_key("b").count()
                .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 1

    def test_nested_map_key_different_branches(self, client):
        """Read from two different nested branches in separate queries."""
        session = client.create_session()
        r1 = (
            session.query(_key(2))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .execute().first_or_raise()
        )
        assert r1.record.bins["nested"] == 200

        r2 = (
            session.query(_key(2))
                .bin("nested").on_map_key("level2").on_map_key("x").get_values()
                .execute().first_or_raise()
        )
        assert r2.record.bins["nested"] == 2

    def test_nested_map_key_with_flat_bin(self, client):
        """Combine a nested CDT read with a flat bin read."""
        session = client.create_session()
        result = (
            session.query(_key(3))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .bin("name").get()
                .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 300
        assert result.record.bins["name"] == "user3"

    def test_nested_map_key_get_values_key3(self, client):
        """Read nested value for a different key to verify data independence."""
        session = client.create_session()
        result = (
            session.query(_key(3))
                .bin("nested").on_map_key("level2").on_map_key("y").get_values()
                .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 4
