"""SyncInfoCommands - Synchronous wrapper for InfoCommands."""

from __future__ import annotations

from typing import Dict, List, Optional, Set

from aerospike_fluent.aio.info import InfoCommands as AsyncInfoCommands
from aerospike_fluent.sync.client import _EventLoopManager


class SyncInfoCommands:
    """
    Synchronous wrapper for InfoCommands.

    Provides the same API as the async InfoCommands but executes operations
    synchronously by running them on an event loop.
    """

    def __init__(self, async_info: AsyncInfoCommands, loop_manager: _EventLoopManager) -> None:
        """
        Initialize a SyncInfoCommands.

        Args:
            async_info: The async InfoCommands to wrap.
            loop_manager: The event loop manager for executing async operations.
        """
        self._async_info = async_info
        self._loop_manager = loop_manager

    def build(self) -> Set[str]:
        """Get the build information from all nodes in the cluster (synchronous)."""
        async def _build():
            return await self._async_info.build()

        return self._loop_manager.run_async(_build())

    def namespaces(self) -> Set[str]:
        """Get the list of namespaces from all nodes in the cluster (synchronous)."""
        async def _namespaces():
            return await self._async_info.namespaces()

        return self._loop_manager.run_async(_namespaces())

    def namespace_details(self, namespace: str) -> Optional[Dict[str, str]]:
        """Get detailed information about a specific namespace (synchronous)."""
        async def _namespace_details():
            return await self._async_info.namespace_details(namespace)

        return self._loop_manager.run_async(_namespace_details())

    def sets(self, namespace: str) -> List[str]:
        """Get the list of sets in a specific namespace (synchronous)."""
        async def _sets():
            return await self._async_info.sets(namespace)

        return self._loop_manager.run_async(_sets())

    def secondary_indexes(self, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """Get information about all secondary indexes (synchronous)."""
        async def _secondary_indexes():
            return await self._async_info.secondary_indexes(namespace)

        return self._loop_manager.run_async(_secondary_indexes())

    def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[Dict[str, str]]:
        """Get detailed information about a specific secondary index (synchronous)."""
        async def _secondary_index_details():
            return await self._async_info.secondary_index_details(namespace, index_name)

        return self._loop_manager.run_async(_secondary_index_details())

    def is_cluster_stable(self) -> bool:
        """Check if all nodes agree on the current cluster state (synchronous)."""
        async def _is_cluster_stable():
            return await self._async_info.is_cluster_stable()

        return self._loop_manager.run_async(_is_cluster_stable())

    def get_cluster_size(self) -> int:
        """Get the number of nodes in the cluster (synchronous)."""
        async def _get_cluster_size():
            return await self._async_info.get_cluster_size()

        return self._loop_manager.run_async(_get_cluster_size())

    def info(self, command: str) -> Dict[str, str]:
        """Execute a raw info command against the cluster (synchronous)."""
        async def _info():
            return await self._async_info.info(command)

        return self._loop_manager.run_async(_info())

    def info_on_all_nodes(self, command: str) -> Dict[str, Dict[str, str]]:
        """Execute a raw info command against all nodes in the cluster (synchronous)."""
        async def _info_on_all_nodes():
            return await self._async_info.info_on_all_nodes(command)

        return self._loop_manager.run_async(_info_on_all_nodes())

