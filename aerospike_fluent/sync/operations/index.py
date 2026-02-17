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

"""SyncIndexBuilder - Synchronous wrapper for index operations."""

from __future__ import annotations

from typing import Optional

from aerospike_async import CollectionIndexType, IndexType

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.operations.index import IndexBuilder
from aerospike_fluent.sync.client import _EventLoopManager


class SyncIndexBuilder:
    """
    Synchronous wrapper for IndexBuilder.

    Provides the same fluent interface as IndexBuilder but with
    synchronous methods that hide async/await.
    """

    def __init__(
        self,
        async_client: FluentClient,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
    ) -> None:
        """
        Initialize a SyncIndexBuilder.

        Args:
            async_client: The async FluentClient instance.
            namespace: The namespace name.
            set_name: The set name.
            loop_manager: The event loop manager for running async operations.
        """
        self._async_client = async_client
        self._namespace = namespace
        self._set_name = set_name
        self._loop_manager = loop_manager
        self._bin_name: Optional[str] = None
        self._index_name: Optional[str] = None
        self._index_type: Optional[IndexType] = None
        self._collection_index_type: Optional[CollectionIndexType] = None

    def _get_async_builder(self) -> IndexBuilder:
        """Get the underlying async index builder."""
        builder = IndexBuilder(
            client=self._async_client._async_client,
            namespace=self._namespace,
            set_name=self._set_name,
        )
        if self._bin_name:
            builder.on_bin(self._bin_name)
        if self._index_name:
            builder.named(self._index_name)
        if self._index_type:
            if self._index_type == IndexType.NUMERIC:
                builder.numeric()
            elif self._index_type == IndexType.STRING:
                builder.string()
        if self._collection_index_type:
            builder.collection(self._collection_index_type)
        return builder

    def on_bin(self, bin_name: str) -> SyncIndexBuilder:
        """Specify the bin to index."""
        self._bin_name = bin_name
        return self

    def named(self, index_name: str) -> SyncIndexBuilder:
        """Specify the index name."""
        self._index_name = index_name
        return self

    def numeric(self) -> SyncIndexBuilder:
        """Set the index type to numeric."""
        self._index_type = IndexType.NUMERIC
        return self

    def string(self) -> SyncIndexBuilder:
        """Set the index type to string."""
        self._index_type = IndexType.STRING
        return self

    def collection(
        self, collection_index_type: CollectionIndexType
    ) -> SyncIndexBuilder:
        """Set the collection index type."""
        self._collection_index_type = collection_index_type
        return self

    def create(self) -> None:
        """Create the index synchronously."""
        builder = self._get_async_builder()
        self._loop_manager.run_async(builder.create())

    def drop(self) -> None:
        """Drop the index synchronously."""
        builder = self._get_async_builder()
        self._loop_manager.run_async(builder.drop())

