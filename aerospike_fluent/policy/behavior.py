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

"""Behavior - Configuration for operation policies."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional


@dataclass(frozen=True)
class Behavior:
    """
    Immutable configuration object for Aerospike operation policies.

    Behaviors define how operations are executed (timeouts, retries,
    consistency levels, etc.). They are immutable and thread-safe.

    Example:
        ```python
        # Use default behavior
        session = cluster.create_session(Behavior.DEFAULT)

        # Create custom behavior
        custom = Behavior.DEFAULT.derive_with_changes(
            name="fast_reads",
            total_timeout=timedelta(seconds=5),
            max_retries=1,
        )
        session = cluster.create_session(custom)
        ```
    """

    name: str
    """Name of this behavior."""

    total_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    """Total timeout for operations."""

    socket_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    """Socket timeout for operations."""

    max_retries: int = 2
    """Maximum number of retry attempts."""

    retry_delay: timedelta = field(default_factory=lambda: timedelta(milliseconds=0))
    """Delay between retries."""

    send_key: bool = True
    """Whether to send the key with operations."""

    use_compression: bool = False
    """Whether to use compression."""

    _parent: Optional[Behavior] = field(default=None, repr=False, compare=False)
    """Parent behavior for inheritance (internal use)."""

    @classmethod
    def default(cls) -> Behavior:
        """
        Get the default behavior with sensible defaults.

        Returns:
            A Behavior instance with default settings.
        """
        return cls(
            name="DEFAULT",
            total_timeout=timedelta(seconds=30),
            socket_timeout=timedelta(seconds=5),
            max_retries=2,
            retry_delay=timedelta(milliseconds=0),
            send_key=True,
            use_compression=False,
        )

    def derive_with_changes(
        self,
        name: str,
        *,
        total_timeout: Optional[timedelta] = None,
        socket_timeout: Optional[timedelta] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[timedelta] = None,
        send_key: Optional[bool] = None,
        use_compression: Optional[bool] = None,
    ) -> Behavior:
        """
        Create a new behavior derived from this one with specified changes.

        Args:
            name: Name for the new behavior.
            total_timeout: Override total timeout.
            socket_timeout: Override socket timeout.
            max_retries: Override max retries.
            retry_delay: Override retry delay.
            send_key: Override send_key setting.
            use_compression: Override compression setting.

        Returns:
            A new Behavior instance with the specified changes.
        """
        return Behavior(
            name=name,
            total_timeout=total_timeout if total_timeout is not None else self.total_timeout,
            socket_timeout=socket_timeout if socket_timeout is not None else self.socket_timeout,
            max_retries=max_retries if max_retries is not None else self.max_retries,
            retry_delay=retry_delay if retry_delay is not None else self.retry_delay,
            send_key=send_key if send_key is not None else self.send_key,
            use_compression=use_compression if use_compression is not None else self.use_compression,
            _parent=self,
        )

    def __repr__(self) -> str:
        """String representation of the behavior."""
        return (
            f"Behavior(name={self.name!r}, "
            f"total_timeout={self.total_timeout}, "
            f"socket_timeout={self.socket_timeout}, "
            f"max_retries={self.max_retries})"
        )


# Class constant for default behavior (defined after class to avoid forward reference issues)
Behavior.DEFAULT = Behavior.default()

