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

"""Tests for FluentClient."""

import pytest
from aerospike_fluent import FluentClient


@pytest.mark.asyncio
async def test_client_connection(aerospike_host, client_policy):
    """Test that we can connect to Aerospike using the fluent client."""
    async with FluentClient(seeds=aerospike_host, policy=client_policy) as client:
        assert client.is_connected
        # Test that we can create a session and perform operations
        session = client.create_session()
        assert session is not None

@pytest.mark.asyncio
async def test_client_context_manager(aerospike_host, client_policy):
    """Test that the context manager properly manages connection lifecycle."""
    client = FluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

    async with client:
        assert client.is_connected

    # After exiting context, connection should be closed
    assert not client.is_connected

@pytest.mark.asyncio
async def test_client_manual_connect_close(aerospike_host, client_policy):
    """Test manual connect and close methods."""
    client = FluentClient(seeds=aerospike_host, policy=client_policy)
    assert not client.is_connected

    await client.connect()
    assert client.is_connected

    await client.close()
    assert not client.is_connected
