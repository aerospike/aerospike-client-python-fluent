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

"""DataSet - A fluent API for creating Aerospike keys for a namespace and set."""

from __future__ import annotations

import typing
from typing import List, Union, overload

from aerospike_async import Key


class DataSet:
    """
    Represents a dataset in Aerospike, which is a collection of records within a namespace.
    A dataset is identified by a namespace and set name combination.

    This class provides a fluent API for creating Aerospike keys for records within the dataset.
    It supports various key types including String, Integer, and byte array keys.

    Example:
        ```python
        users = DataSet.of("test", "users")
        user_key = users.id("user123")
        user_keys = users.ids("user1", "user2", "user3")
        ```
    """

    def __init__(self, namespace: str, set_name: str) -> None:
        """
        Create a DataSet instance.

        Args:
            namespace: The Aerospike namespace
            set_name: The set name within the namespace

        Raises:
            ValueError: If namespace or set_name is empty
        """
        if not namespace:
            raise ValueError("namespace cannot be empty")
        if not set_name:
            raise ValueError("set_name cannot be empty")

        self._namespace = namespace
        self._set_name = set_name

    @staticmethod
    def of(namespace: str, set_name: str) -> DataSet:
        """
        Create a new DataSet instance for the specified namespace and set.

        Args:
            namespace: The Aerospike namespace
            set_name: The set name within the namespace

        Returns:
            A new DataSet instance

        Raises:
            ValueError: If namespace or set_name is empty
        """
        return DataSet(namespace, set_name)

    @property
    def namespace(self) -> str:
        """Get the namespace of this dataset."""
        return self._namespace

    @property
    def set_name(self) -> str:
        """Get the set name of this dataset."""
        return self._set_name

    def id(self, identifier: Union[str, int, bytes]) -> Key:
        """
        Create an Aerospike key with the given identifier.

        Args:
            identifier: The key identifier (string, int, or bytes)

        Returns:
            A new Key instance

        Example:
            ```python
            users = DataSet.of("test", "users")
            key1 = users.id("user123")  # String key
            key2 = users.id(12345)      # Integer key
            key3 = users.id(b"bytes")   # Bytes key
            ```
        """
        return Key(self._namespace, self._set_name, identifier)

    def id_from_digest(self, digest: Union[str, bytes]) -> Key:
        """
        Create an Aerospike key from a digest.

        Args:
            digest: The digest as a hex string or bytes

        Returns:
            A new Key instance created from the digest

        Example:
            ```python
            users = DataSet.of("test", "users")
            # From hex string
            key = users.id_from_digest("a1b2c3d4...")
            # From bytes
            key = users.id_from_digest(b"\\xa1\\xb2\\xc3...")
            ```
        """
        return Key.key_with_digest(self._namespace, self._set_name, digest)

    def id_for_object(self, obj: object) -> Key:
        """
        Create an Aerospike key from an object identifier.
        Supports String, Integer, Long, and byte array types.

        Args:
            obj: The object to use as the key identifier

        Returns:
            A new Key instance

        Raises:
            TypeError: If the object type is not supported

        Example:
            ```python
            users = DataSet.of("test", "users")
            key1 = users.id_for_object("user123")     # String key
            key2 = users.id_for_object(123)           # Integer key
            key3 = users.id_for_object(12345)         # Integer key
            key4 = users.id_for_object(b"bytes")      # Bytes key
            ```
        """
        if isinstance(obj, str):
            return self.id(obj)
        elif isinstance(obj, (int, float)):
            # Convert float to int if it's a whole number
            if isinstance(obj, float) and obj.is_integer():
                return self.id(int(obj))
            elif isinstance(obj, int):
                return self.id(obj)
            else:
                raise TypeError(f"Cannot use float {obj} as key identifier")
        elif isinstance(obj, bytes):
            return self.id(obj)
        elif isinstance(obj, bytearray):
            return self.id(bytes(obj))
        else:
            raise TypeError(
                f"Cannot construct a key for object of type {type(obj).__name__}. "
                "Only str, int, and bytes are supported."
            )

    @overload
    def ids(self, *identifiers: str) -> List[Key]:
        """Create multiple keys from string identifiers."""
        ...

    @overload
    def ids(self, *identifiers: int) -> List[Key]:
        """Create multiple keys from integer identifiers."""
        ...

    @overload
    def ids(self, *identifiers: bytes) -> List[Key]:
        """Create multiple keys from bytes identifiers."""
        ...

    @overload
    def ids(self, identifiers: List[Union[str, int, bytes]]) -> List[Key]:
        """Create multiple keys from a list of identifiers."""
        ...

    def ids(
        self,
        *identifiers: Union[str, int, bytes],
    ) -> List[Key]:
        """
        Create multiple Aerospike keys from identifiers.

        Can be called with:
        - Multiple positional arguments: `ids("id1", "id2", "id3")`
        - A single list argument: `ids(["id1", "id2", "id3"])`

        Args:
            *identifiers: Variable number of identifiers (str, int, or bytes)
                          OR a single list of identifiers

        Returns:
            A list of Key instances

        Example:
            ```python
            users = DataSet.of("test", "users")
            # Multiple arguments
            keys1 = users.ids("user1", "user2", "user3")
            # Single list argument
            keys2 = users.ids(["user1", "user2", "user3"])
            # Integer keys
            keys3 = users.ids(1, 2, 3, 4, 5)
            ```
        """
        # Handle case where a single list is passed
        if len(identifiers) == 1 and isinstance(identifiers[0], list):
            return [self.id_for_object(id_obj) for id_obj in identifiers[0]]

        # Handle multiple positional arguments
        return [self.id(id_obj) for id_obj in identifiers]

    def ids_from_digests(
        self, *digests: Union[str, bytes]
    ) -> List[Key]:
        """
        Create multiple Aerospike keys from digests.

        Args:
            *digests: Variable number of digests (hex strings or bytes)

        Returns:
            A list of Key instances created from the digests

        Example:
            ```python
            users = DataSet.of("test", "users")
            keys = users.ids_from_digests("digest1", "digest2", b"digest3")
            ```
        """
        return [self.id_from_digest(digest) for digest in digests]

    def __repr__(self) -> str:
        """Return a string representation of this DataSet."""
        return f"DataSet(namespace={self._namespace!r}, set_name={self._set_name!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality with another DataSet."""
        if not isinstance(other, DataSet):
            return False
        return self._namespace == other._namespace and self._set_name == other._set_name

    def __hash__(self) -> int:
        """Return hash of this DataSet."""
        return hash((self._namespace, self._set_name))

