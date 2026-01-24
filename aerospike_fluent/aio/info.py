"""InfoCommands - High-level interface for Aerospike info commands."""

from __future__ import annotations

import typing
from typing import TYPE_CHECKING, Dict, List, Optional, Set

if TYPE_CHECKING:
    from aerospike_fluent.aio.session import Session


class InfoCommands:
    """
    Provides high-level methods to execute common Aerospike info commands.

    This class encapsulates the most commonly used Aerospike info commands and provides
    a convenient API for retrieving cluster information.

    Example:
        ```python
        info = session.info()

        # Get all namespaces
        namespaces = await info.namespaces()

        # Get namespace details
        ns_detail = await info.namespace_details("test")

        # Get all secondary indexes
        indexes = await info.secondary_indexes()
        ```
    """

    def __init__(self, session: "Session") -> None:
        """
        Initialize InfoCommands.

        Args:
            session: The Session to use for info commands.
        """
        self._session = session

    async def build(self) -> Set[str]:
        """
        Get the build information from all nodes in the cluster.

        Returns:
            A set of build strings from all nodes.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes("build")

        # Extract build strings from all node responses
        build_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"build": "8.1.0.1"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    build_set.add(value.strip())

        return build_set

    async def namespaces(self) -> Set[str]:
        """
        Get the list of namespaces from all nodes in the cluster.

        Returns:
            A set of namespace names from all nodes.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes("namespaces")

        # Extract namespace names from all node responses
        namespace_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"namespaces": "ns1,ns2,ns3"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    # Split comma-separated namespace list
                    namespace_set.update([ns.strip() for ns in value.split(",") if ns.strip()])

        return namespace_set

    async def namespace_details(self, namespace: str) -> Optional[Dict[str, str]]:
        """
        Get detailed information about a specific namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            A dictionary containing namespace details, or None if not found.
        """
        try:
            response = await self._session._client._client.info(f"namespace/{namespace}")
            if not response:
                return None
            # Check if response indicates namespace doesn't exist
            # Response format for non-existent: {'namespace/name': 'type=unknown'}
            expected_key = f"namespace/{namespace}"
            if expected_key in response and str(response[expected_key]).strip() == "type=unknown":
                return None
            return response
        except Exception:
            # If namespace doesn't exist or error occurs, return None
            return None

    async def sets(self, namespace: str) -> List[str]:
        """
        Get the list of sets in a specific namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            A list of set names in the namespace.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes(f"sets/{namespace}")

        # Extract set names from all node responses
        set_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"sets": "set1,set2,set3"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    # Split comma-separated set list
                    set_set.update([s.strip() for s in value.split(",") if s.strip()])

        return sorted(list(set_set))

    async def secondary_indexes(self, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get information about all secondary indexes.

        Args:
            namespace: Optional namespace filter. If provided, only returns
                      indexes for that namespace.

        Returns:
            A list of dictionaries containing secondary index information.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes("sindex-list")

        # Parse sindex-list response format: "ns:set:bin:name:type:state:sync_state:update_state"
        # Multiple indexes are separated by semicolons
        index_map: Dict[str, Dict[str, str]] = {}

        for node_response in all_responses.values():
            # Response format is typically {"sindex-list": "ns:set:bin:name:type:state;..."}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    # Split by semicolon to get individual index entries
                    for entry in value.split(";"):
                        entry = entry.strip()
                        if not entry:
                            continue

                        # Parse index entry: ns:set:bin:name:type:state:sync_state:update_state
                        parts = entry.split(":")
                        if len(parts) >= 4:
                            ns = parts[0]
                            set_name = parts[1]
                            bin_name = parts[2]
                            index_name = parts[3]

                            # Apply namespace filter if provided
                            if namespace and ns != namespace:
                                continue

                            # Use index name as key to deduplicate across nodes
                            if index_name not in index_map:
                                index_map[index_name] = {
                                    "namespace": ns,
                                    "set": set_name,
                                    "bin": bin_name,
                                    "name": index_name,
                                }
                                if len(parts) >= 5:
                                    index_map[index_name]["type"] = parts[4]
                                if len(parts) >= 6:
                                    index_map[index_name]["state"] = parts[5]

        return list(index_map.values())

    async def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[Dict[str, str]]:
        """
        Get detailed information about a specific secondary index.

        Args:
            namespace: The namespace containing the index.
            index_name: The name of the index.

        Returns:
            A dictionary containing index details, or None if not found.
        """
        try:
            response = await self._session._client._client.info(f"sindex/{namespace}/{index_name}")
            if not response:
                return None
            # Check if response indicates index doesn't exist
            # Response format for non-existent: {'sindex/ns/name': 'ERROR:201:no index'}
            expected_key = f"sindex/{namespace}/{index_name}"
            if expected_key in response and "ERROR:201:no index" in str(response[expected_key]):
                return None
            return response
        except Exception:
            # If index doesn't exist or error occurs, return None
            return None

    async def is_cluster_stable(self) -> bool:
        """
        Check if all nodes agree on the current cluster state.

        Returns:
            True if the cluster is stable, False otherwise.
        """
        # Get cluster state from all nodes
        all_responses = await self._session._client._client.info_on_all_nodes("cluster-stable")

        if not all_responses:
            return False

        # Check if all nodes report "true" for cluster-stable
        for node_response in all_responses.values():
            for value in node_response.values():
                if isinstance(value, str):
                    # cluster-stable returns "true" or "false"
                    if value.lower() != "true":
                        return False

        return True

    async def get_cluster_size(self) -> int:
        """
        Get the number of nodes in the cluster.

        Returns:
            The number of nodes in the cluster.
        """
        node_names = await self._session._client._client.node_names()
        return len(node_names)

    async def info(self, command: str) -> Dict[str, str]:
        """
        Execute a raw info command against the cluster.

        Args:
            command: The info command to execute (e.g., "statistics", "build").

        Returns:
            A dictionary containing the info command response as key-value pairs.
        """
        return await self._session._client._client.info(command)

    async def info_on_all_nodes(self, command: str) -> Dict[str, Dict[str, str]]:
        """
        Execute a raw info command against all nodes in the cluster.

        Args:
            command: The info command to execute (e.g., "statistics", "build").

        Returns:
            A dictionary mapping node names to their response dictionaries.
        """
        return await self._session._client._client.info_on_all_nodes(command)

