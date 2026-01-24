"""Cluster - Represents a connection to an Aerospike cluster."""

from __future__ import annotations

import typing
from typing import Optional

from aerospike_async import ClientPolicy, new_client

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.policy.behavior import Behavior

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.session import Session
    from aerospike_fluent.aio.services.transactional_session import TransactionalSession


class Cluster:
    """
    Represents a connection to an Aerospike cluster.
    
    This class manages the lifecycle of a connection to an Aerospike cluster,
    including the underlying client. It implements async context manager protocol
    to ensure proper resource cleanup.
    
    Example usage:
        ```python
        async with await ClusterDefinition("localhost", 3100).connect() as cluster:
            session = cluster.create_session(Behavior.DEFAULT)
            # Use the session for database operations...
        ```
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
        """
        fluent_client = FluentClient(seeds=seeds, policy=policy)
        await fluent_client.connect()
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
        """
        Creates a new session with the specified behavior.
        
        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).
        
        Args:
            behavior: The behavior configuration for the session. If None, uses Behavior.DEFAULT.
        
        Returns:
            A new Session instance
        """
        if behavior is None:
            behavior = Behavior.DEFAULT
        return self._fluent_client.create_session(behavior)
    
    def create_transactional_session(
        self,
        behavior: Optional[Behavior] = None,
    ) -> TransactionalSession:
        """
        Creates a new transactional session with the specified behavior.
        
        Args:
            behavior: The behavior configuration for the session. If None, uses Behavior.DEFAULT.
                     Note: Currently TransactionalSession doesn't use behavior, but parameter
                     is included for API consistency.
        
        Returns:
            A new TransactionalSession instance
        """
        # Note: FluentClient.transaction_session() doesn't take behavior parameter yet
        # but we include it in the signature for API consistency
        return self._fluent_client.transaction_session()
    
    def is_connected(self) -> bool:
        """
        Checks if the cluster connection is currently active.
        
        Returns:
            True if the connection is active, False otherwise
        """
        return self._fluent_client.is_connected
    
    async def close(self) -> None:
        """
        Closes the cluster connection and releases all associated resources.
        
        This method closes the underlying client connection. It should be called
        when the cluster is no longer needed to ensure proper resource cleanup.
        
        This method is automatically called when using async context manager:
            ```python
            async with await ClusterDefinition("localhost", 3100).connect() as cluster:
                # Use the cluster...
            # cluster.close() is automatically called here
            ```
        """
        await self._fluent_client.close()

