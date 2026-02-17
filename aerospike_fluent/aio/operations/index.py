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

"""IndexBuilder - Builder for index operations."""

from __future__ import annotations

from typing import Optional

from aerospike_async import (
    Client,
    CollectionIndexType,
    IndexType,
)

from aerospike_fluent.exceptions import convert_pac_exception


class IndexBuilder:
    """
    Builder for index operations.

    This class provides a fluent interface for creating and dropping indexes.
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
    ) -> None:
        """
        Initialize an IndexBuilder.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._bin_name: Optional[str] = None
        self._index_name: Optional[str] = None
        self._index_type: Optional[IndexType] = None
        self._collection_index_type: Optional[CollectionIndexType] = None

    def on_bin(self, bin_name: str) -> IndexBuilder:
        """
        Specify the bin name for the index.

        Args:
            bin_name: The name of the bin to index.

        Returns:
            self for method chaining.
        """
        self._bin_name = bin_name
        return self

    def named(self, index_name: str) -> IndexBuilder:
        """
        Specify the index name.

        Args:
            index_name: The name of the index.

        Returns:
            self for method chaining.
        """
        self._index_name = index_name
        return self

    def numeric(self) -> IndexBuilder:
        """
        Set the index type to numeric.

        Returns:
            self for method chaining.
        """
        self._index_type = IndexType.NUMERIC
        return self

    def string(self) -> IndexBuilder:
        """
        Set the index type to string.

        Returns:
            self for method chaining.
        """
        self._index_type = IndexType.STRING
        return self

    def collection(
        self, collection_index_type: CollectionIndexType
    ) -> IndexBuilder:
        """
        Set the collection index type.

        Args:
            collection_index_type: The collection index type.

        Returns:
            self for method chaining.
        """
        self._collection_index_type = collection_index_type
        return self

    async def create(self) -> None:
        """
        Create the index.

        Raises:
            ValueError: If required fields (bin_name, index_name, index_type) are not set.
        """
        if not self._bin_name:
            raise ValueError("bin_name is required. Call on_bin() first.")
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        if not self._index_type:
            raise ValueError("index_type is required. Call numeric() or string() first.")

        try:
            await self._client.create_index(
                self._namespace,
                self._set_name,
                self._bin_name,
                self._index_name,
                self._index_type,
                self._collection_index_type,
            )
        except Exception as e:
            raise convert_pac_exception(e) from e

    async def drop(self) -> None:
        """
        Drop the index.

        Raises:
            ValueError: If index_name is not set.
        """
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")

        try:
            await self._client.drop_index(
                self._namespace, self._set_name, self._index_name
            )
        except Exception as e:
            raise convert_pac_exception(e) from e


