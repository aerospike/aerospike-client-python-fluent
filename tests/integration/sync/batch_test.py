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

"""Synchronous :meth:`SyncSession.batch` integration tests (mirrors async batch paths)."""

import time

import pytest

from aerospike_sdk import DataSet, SyncClient


@pytest.fixture
def client(aerospike_host, client_policy, enterprise):
    with SyncClient(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest.fixture
def users():
    return DataSet.of("test", "sync_batch_test")


class TestSyncBatchOperations:

    def test_batch_insert_multiple_keys(self, client: SyncClient, users: DataSet):
        session = client.create_session()
        key1 = users.id("sb_user_1")
        key2 = users.id("sb_user_2")

        for k in (key1, key2):
            try:
                session.delete(k).execute()
            except Exception:
                pass

        stream = (
            session.batch()
            .insert(key1).bin("name").set_to("Ada")
            .insert(key2).bin("name").set_to("Bob")
            .execute()
        )
        results = stream.collect()
        assert len(results) == 2

        r1 = session.query(key1).execute().first_or_raise()
        assert r1.record.bins["name"] == "Ada"
        r2 = session.query(key2).execute().first_or_raise()
        assert r2.record.bins["name"] == "Bob"

        session.delete(key1).execute()
        session.delete(key2).execute()

    def test_batch_mixed_update_delete_insert(self, client: SyncClient, users: DataSet):
        session = client.create_session()
        key1 = users.id("sb_mix_1")
        key2 = users.id("sb_mix_2")
        key3 = users.id("sb_mix_3")

        session.upsert(key1).put({"counter": 10}).execute()
        session.upsert(key2).put({"name": "gone"}).execute()
        try:
            session.delete(key3).execute()
        except Exception:
            pass

        stream = (
            session.batch()
            .update(key1).bin("counter").add(5)
            .delete(key2)
            .insert(key3).bin("status").set_to("new")
            .execute()
        )
        assert len(stream.collect()) == 3

        assert session.query(key1).execute().first_or_raise().record.bins["counter"] == 15
        ex = session.exists(key2).respond_all_keys().execute().first()
        assert ex is not None and ex.as_bool() is False
        assert session.query(key3).execute().first_or_raise().record.bins["status"] == "new"

        session.delete(key1).execute()
        session.delete(key3).execute()

    def test_batch_empty_raises(self, client: SyncClient):
        session = client.create_session()
        with pytest.raises(ValueError, match="No operations to execute"):
            session.batch().execute()


class TestSyncBatchExpressionOps:

    def test_batch_upsert_from(self, client: SyncClient, users: DataSet, enterprise):
        session = client.create_session()
        keys = [users.id(f"sbx_{i}") for i in range(2)]

        for i, key in enumerate(keys):
            session.upsert(key).put({"A": (i + 1) * 10}).execute()

        stream = (
            session.batch()
            .upsert(keys[0]).bin("C").upsert_from("$.A + 1")
            .upsert(keys[1]).bin("C").upsert_from("$.A + 1")
            .execute()
        )
        assert len(stream.collect()) == 2
        time.sleep(0.25 if not enterprise else 0.01)

        for i, key in enumerate(keys):
            rec = session.query(key).bin("C").get().execute().first_or_raise()
            assert rec.record.bins["C"] == (i + 1) * 10 + 1

        for key in keys:
            session.delete(key).execute()
