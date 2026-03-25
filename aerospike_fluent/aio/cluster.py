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

import typing
from typing import Optional

from aerospike_async import ClientPolicy, new_client

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.exceptions import ConnectionError
from aerospike_fluent.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.session import Session
    from aerospike_fluent.aio.transactional_session import TransactionalSession


class Cluster:
    """Live connection to a cluster, obtained from :meth:`ClusterDefinition.connect`.

    Owns a connected :class:`~aerospike_fluent.aio.client.FluentClient` and exposes
    :meth:`create_session` / :meth:`create_transactional_session`. Prefer
    ``async with await ClusterDefinition(...).connect() as cluster`` so
    :meth:`close` runs on exit.

    Example:
        ```python
        async with await ClusterDefinition("localhost", 3100).connect() as cluster:
            session = cluster.create_session(Behavior.DEFAULT)
        ```

    See Also:
        :class:`~aerospike_fluent.aio.cluster_definition.ClusterDefinition`
    """
    
    def __init__(self, fluent_client: FluentClient) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            fluent_client: The underlying FluentClient instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._fluent_client = fluent_client
    
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
        fluent_client = FluentClient(seeds=seeds, policy=policy)
        await fluent_client.connect()

        if not await fluent_client.underlying_client.is_connected():
            await fluent_client.close()
            raise ConnectionError(
                f"Connected to seeds '{seeds}' but cluster reports not connected"
            )

        return cls(fluent_client)
    
    async def __aenter__(self) -> Cluster:
        """Async context manager entry."""
        return self
    
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[typing.TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()
    
    @property
    def _client(self) -> FluentClient:
        """Get the underlying FluentClient."""
        return self._fluent_client
    
    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """Open a :class:`~aerospike_fluent.aio.session.Session` with optional behavior.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: Defaults to :attr:`~aerospike_fluent.policy.behavior.Behavior.DEFAULT`.

        Returns:
            Session bound to this cluster's fluent client.

        See Also:
            :meth:`~aerospike_fluent.aio.client.FluentClient.create_session`
        """
        if behavior is None:
            behavior = Behavior.DEFAULT
        return self._fluent_client.create_session(behavior)
    
    def create_transactional_session(
        self,
        behavior: Optional[Behavior] = None,
    ) -> TransactionalSession:
        """Return a transactional session facade (behavior reserved for API parity).

        Args:
            behavior: Accepted for symmetry with :meth:`create_session`; the
                underlying client may not apply it yet.

        Returns:
            :class:`~aerospike_fluent.aio.transactional_session.TransactionalSession`.

        See Also:
            :meth:`~aerospike_fluent.aio.client.FluentClient.transaction_session`
        """
        # Note: FluentClient.transaction_session() doesn't take behavior parameter yet
        # but we include it in the signature for API consistency
        return self._fluent_client.transaction_session()
    
    def is_connected(self) -> bool:
        """Mirror :attr:`~aerospike_fluent.aio.client.FluentClient.is_connected`."""
        return self._fluent_client.is_connected
    
    async def close(self) -> None:
        """Close the fluent client and release cluster resources.

        Invoked automatically when used as an async context manager.
        """
        await self._fluent_client.close()

