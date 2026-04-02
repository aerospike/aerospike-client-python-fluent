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

"""Integration tests for TLS connections through the fluent API."""

import os
import pytest

from aerospike_fluent import (
    Behavior,
    ClusterDefinition,
    DataSet,
    FluentClient,
    Host,
    SyncFluentClient,
)


def _tls_host_env():
    """TLS host from environment."""
    return os.environ.get("AEROSPIKE_TLS_HOST") or os.environ.get("AEROSPIKE_HOST_TLS")


def _parse_host_port(host_str: str, default_port: int = 4333) -> tuple[str, int]:
    parts = host_str.split(":")
    return parts[0], int(parts[1]) if len(parts) > 1 else default_port


# ---------------------------------------------------------------------------
# Shared env helpers
# ---------------------------------------------------------------------------

TLS_CA = os.environ.get("AEROSPIKE_TLS_CA_FILE")
TLS_NAME = os.environ.get("AEROSPIKE_TLS_NAME")
TLS_CERT = os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE")
TLS_KEY = os.environ.get("AEROSPIKE_TLS_CLIENT_KEY_FILE")
TLS_USER = os.environ.get("AEROSPIKE_USER", "admin")
TLS_PASS = os.environ.get("AEROSPIKE_PASSWORD", "admin")

USERS = DataSet.of("test", "tls_smoke")

def _ca_exists():
    return TLS_CA is not None and os.path.isfile(TLS_CA)

def _client_cert_exists():
    return TLS_CERT is not None and os.path.isfile(TLS_CERT)

skip_no_tls = pytest.mark.skipif(
    not _tls_host_env() or not _ca_exists(),
    reason="TLS host not set or CA file missing — TLS server not available",
)

skip_no_pki = pytest.mark.skipif(
    not _tls_host_env() or not _ca_exists() or not _client_cert_exists(),
    reason="TLS host/CA/client-cert not set or files missing — PKI not available",
)


# ============================================================================
# ClusterDefinition — TLS via fluent builder
# ============================================================================


@skip_no_tls
class TestTlsClusterDefinition:
    """TLS connections through ClusterDefinition's fluent builder."""

    @pytest.fixture
    def host_port(self):
        return _parse_host_port(_tls_host_env())

    async def test_basic_tls_connection(self, host_port):
        """ClusterDefinition + with_tls_config_of() connects over TLS."""
        if not TLS_CA:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        hostname, port = host_port
        cd = (
            ClusterDefinition(hostname, port)
            .with_tls_config_of()
                .tls_name(TLS_NAME or "")
                .ca_file(TLS_CA)
            .done()
            .with_native_credentials(TLS_USER, TLS_PASS)
            .using_services_alternate()
        )
        cluster = await cd.connect()
        try:
            assert cluster.is_connected()
            session = cluster.create_session(Behavior.DEFAULT)
            assert session is not None
        finally:
            await cluster.close()

    async def test_tls_with_host_tls_name(self, host_port):
        """Host objects carry per-host tls_name."""
        if not TLS_CA or not TLS_NAME:
            pytest.skip("AEROSPIKE_TLS_CA_FILE or AEROSPIKE_TLS_NAME not set")

        hostname, port = host_port
        cd = (
            ClusterDefinition(hosts=[Host(hostname, port, tls_name=TLS_NAME)])
            .with_tls_config_of()
                .ca_file(TLS_CA)
            .done()
            .with_native_credentials(TLS_USER, TLS_PASS)
            .using_services_alternate()
        )
        cluster = await cd.connect()
        try:
            assert cluster.is_connected()
        finally:
            await cluster.close()

    async def test_tls_read_write(self, host_port):
        """Basic put/get over TLS."""
        if not TLS_CA:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        hostname, port = host_port
        cd = (
            ClusterDefinition(hostname, port)
            .with_tls_config_of()
                .tls_name(TLS_NAME or "")
                .ca_file(TLS_CA)
            .done()
            .with_native_credentials(TLS_USER, TLS_PASS)
            .using_services_alternate()
        )
        cluster = await cd.connect()
        try:
            session = cluster.create_session(Behavior.DEFAULT)
            key = USERS.id("tls_rw")
            await session.upsert(key).bin("v").set_to(42).execute()
            stream = await session.query(key).execute()
            result = await stream.first_or_raise()
            assert result.record.bins["v"] == 42
            await session.delete(key).execute()
        finally:
            await cluster.close()


# ============================================================================
# ClusterDefinition — PKI (client certificate)
# ============================================================================


@skip_no_pki
class TestPkiClusterDefinition:
    """PKI (mutual TLS) connections through ClusterDefinition."""

    @pytest.fixture
    def host_port(self):
        return _parse_host_port(_tls_host_env())

    async def test_pki_connection(self, host_port):
        """Connect with client cert + key via the fluent builder."""
        if not all([TLS_CA, TLS_CERT, TLS_KEY]):
            pytest.skip("TLS certificate files not configured")

        hostname, port = host_port
        cd = (
            ClusterDefinition(hostname, port)
            .with_tls_config_of()
                .tls_name(TLS_NAME or "")
                .ca_file(TLS_CA)
                .client_cert_file(TLS_CERT)
                .client_key_file(TLS_KEY)
            .done()
            .with_native_credentials(TLS_USER, TLS_PASS)
            .using_services_alternate()
        )
        cluster = await cd.connect()
        try:
            assert cluster.is_connected()
        finally:
            await cluster.close()


# ============================================================================
# FluentClient — TLS via direct policy
# ============================================================================


@skip_no_tls
class TestTlsFluentClient:
    """TLS connections through FluentClient (seeds string)."""

    def _seeds(self) -> str:
        """Build host:tls_name:port seed string."""
        host_str = _tls_host_env()
        hostname, port = _parse_host_port(host_str)
        if TLS_NAME:
            return f"{hostname}:{TLS_NAME}:{port}"
        return host_str

    def _policy(self):
        from aerospike_async import ClientPolicy, TlsConfig, AuthMode

        policy = ClientPolicy()
        policy.use_services_alternate = True
        if TLS_CA:
            if TLS_CERT and TLS_KEY:
                policy.tls_config = TlsConfig.with_client_auth(TLS_CA, TLS_CERT, TLS_KEY)
            else:
                policy.tls_config = TlsConfig(TLS_CA)
        policy.set_auth_mode(AuthMode.INTERNAL, user=TLS_USER, password=TLS_PASS)
        return policy

    async def test_async_fluent_client_tls(self):
        """FluentClient with explicit TLS policy."""
        if not TLS_CA:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        async with FluentClient(self._seeds(), self._policy()) as client:
            assert client.is_connected
            session = client.create_session(Behavior.DEFAULT)
            key = USERS.id("fc_tls")
            await session.upsert(key).bin("v").set_to(1).execute()
            stream = await session.query(key).execute()
            result = await stream.first_or_raise()
            assert result.record.bins["v"] == 1
            await session.delete(key).execute()

    def test_sync_fluent_client_tls(self):
        """SyncFluentClient with explicit TLS policy."""
        if not TLS_CA:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        with SyncFluentClient(self._seeds(), self._policy()) as client:
            assert client.is_connected
            session = client.create_session(Behavior.DEFAULT)
            key = USERS.id("sfc_tls")
            session.upsert(key).bin("v").set_to(2).execute()
            stream = session.query(key).execute()
            result = stream.first_or_raise()
            assert result.record.bins["v"] == 2
            session.delete(key).execute()
