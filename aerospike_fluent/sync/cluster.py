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

"""Cluster - Represents a connection to an Aerospike cluster (sync version)."""

from __future__ import annotations

import typing
from typing import NoReturn, Optional

from aerospike_async import ClientPolicy

from aerospike_fluent.exceptions import ConnectionError
from aerospike_fluent.policy.behavior import Behavior
from aerospike_fluent.sync.client import SyncFluentClient

if typing.TYPE_CHECKING:
    from aerospike_fluent.sync.session import SyncSession


class Cluster:
    """Synchronous cluster handle from ``sync.cluster_definition.ClusterDefinition.connect``.

    Mirrors :class:`~aerospike_fluent.aio.cluster.Cluster` but uses
    :class:`~aerospike_fluent.sync.client.SyncFluentClient` and
    :class:`~aerospike_fluent.sync.session.SyncSession`.

    Example:
        ```python
        with ClusterDefinition("localhost", 3100).connect() as cluster:
            session = cluster.create_session(Behavior.DEFAULT)
        ```

    See Also:
        :class:`~aerospike_fluent.aio.cluster.Cluster`
    """
    
    def __init__(self, fluent_client: SyncFluentClient) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            fluent_client: The underlying SyncFluentClient instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._fluent_client = fluent_client
    
    @classmethod
    def _create(cls, policy: ClientPolicy, seeds: str) -> Cluster:
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
        fluent_client = SyncFluentClient(seeds=seeds, policy=policy)
        fluent_client.connect()

        async def _check():
            return await fluent_client._async_client.underlying_client.is_connected()

        if not fluent_client._loop_manager.run_async(_check()):
            fluent_client.close()
            raise ConnectionError(
                f"Connected to seeds '{seeds}' but cluster reports not connected"
            )

        return cls(fluent_client)
    
    def __enter__(self) -> Cluster:
        """Context manager entry."""
        return self
    
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[typing.TracebackType],
    ) -> None:
        """Context manager exit."""
        self.close()
    
    @property
    def _client(self) -> SyncFluentClient:
        """Get the underlying SyncFluentClient."""
        return self._fluent_client
    
    def create_session(self, behavior: Optional[Behavior] = None) -> SyncSession:
        """Return a :class:`~aerospike_fluent.sync.session.SyncSession` for this cluster.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).
        Args:
            behavior: Defaults to :attr:`~aerospike_fluent.policy.behavior.Behavior.DEFAULT`.

        Returns:
            A new sync session bound to this cluster's client.

        See Also:
            :meth:`~aerospike_fluent.aio.cluster.Cluster.create_session`
        """
        if behavior is None:
            behavior = Behavior.DEFAULT
        return self._fluent_client.create_session(behavior)
    
    def create_transactional_session(
        self,
        behavior: Optional[Behavior] = None,
    ) -> NoReturn:
        """
        Creates a new transactional session with the specified behavior.
        
        Args:
            behavior: The behavior configuration for the session. If None, uses Behavior.DEFAULT.
                     Note: Currently TransactionalSession doesn't use behavior, but parameter
                     is included for API consistency.
        
        Returns:
            A new transactional session instance
        
        Note:
            SyncTransactionalSession may not be implemented yet. This is a placeholder
            for API consistency with the async version.
        """
        # TODO: Implement SyncTransactionalSession
        raise NotImplementedError("SyncTransactionalSession is not yet implemented")
    
    def is_connected(self) -> bool:
        """
        Checks if the cluster connection is currently active.
        
        Returns:
            True if the connection is active, False otherwise
        """
        return self._fluent_client.is_connected
    
    def close(self) -> None:
        """
        Closes the cluster connection and releases all associated resources.
        
        This method closes the underlying client connection. It should be called
        when the cluster is no longer needed to ensure proper resource cleanup.
        
        This method is automatically called when using context manager:
            ```python
            with ClusterDefinition("localhost", 3100).connect() as cluster:
                # Use the cluster...
            # cluster.close() is automatically called here
            ```
        """
        self._fluent_client.close()

