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

"""Cluster - Represents a connection to an Aerospike cluster."""

from __future__ import annotations

import types
import typing
from typing import Optional

from aerospike_async import ClientPolicy

from aerospike_sdk.aio.client import Client
from aerospike_sdk.exceptions import ConnectionError
from aerospike_sdk.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_sdk.aio.session import Session
    from aerospike_sdk.aio.transactional_session import TransactionalSession


class Cluster:
    """Live connection to a cluster, obtained from :meth:`ClusterDefinition.connect`.

    Owns a connected :class:`~aerospike_sdk.aio.client.Client` and exposes
    :meth:`create_session` / :meth:`create_transactional_session`. Prefer
    ``async with await ClusterDefinition(...).connect() as cluster`` so
    :meth:`close` runs on exit.

    Example::

            async with await ClusterDefinition("localhost", 3100).connect() as cluster:
                session = cluster.create_session(Behavior.DEFAULT)

    See Also:
        :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`
    """
    
    def __init__(self, sdk_client: Client) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            sdk_client: The underlying Client instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._sdk_client = sdk_client
    
    @classmethod
    async def _create(cls, policy: ClientPolicy, seeds: str) -> Cluster:
        """
        Internal method to create a new Cluster instance.
        
        Args:
            policy: The ClientPolicy configuration
            seeds: The seeds string (e.g., "localhost:3000")
        
        Returns:
            A new Cluster instance
        
        Raises:
            ConnectionError: If post-connect validation fails
        """
        sdk_client = Client(seeds=seeds, policy=policy)
        await sdk_client.connect()

        if not await sdk_client.underlying_client.is_connected():
            await sdk_client.close()
            raise ConnectionError(
                f"Connected to seeds '{seeds}' but cluster reports not connected"
            )

        return cls(sdk_client)
    
    async def __aenter__(self) -> Cluster:
        """Async context manager entry."""
        return self
    
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()
    
    @property
    def _client(self) -> Client:
        """Get the underlying Client."""
        return self._sdk_client
    
    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """Open a :class:`~aerospike_sdk.aio.session.Session` with optional behavior.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: Defaults to :attr:`~aerospike_sdk.policy.behavior.Behavior.DEFAULT`.

        Returns:
            Session bound to this cluster's SDK client.

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.create_session`
        """
        if behavior is None:
            behavior = Behavior.DEFAULT
        return self._sdk_client.create_session(behavior)
    
    def create_transactional_session(
        self,
        behavior: Optional[Behavior] = None,
    ) -> TransactionalSession:
        """Return a transactional session facade (behavior reserved for API parity).

        Args:
            behavior: Accepted for symmetry with :meth:`create_session`; the
                underlying client may not apply it yet.

        Returns:
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`.

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.transaction_session`
        """
        # Note: Client.transaction_session() doesn't take behavior parameter yet
        # but we include it in the signature for API consistency
        return self._sdk_client.transaction_session()
    
    def is_connected(self) -> bool:
        """Mirror :attr:`~aerospike_sdk.aio.client.Client.is_connected`."""
        return self._sdk_client.is_connected
    
    async def close(self) -> None:
        """Close the SDK client and release cluster resources.

        Invoked automatically when used as an async context manager.
        """
        await self._sdk_client.close()

