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

"""SystemSettings - Cluster-wide system configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from aerospike_async import ClientPolicy


@dataclass(frozen=True)
class SystemSettings:
    """Cluster-wide settings that apply to an entire cluster instance.

    These settings cannot vary per Behavior -- they are inherently global
    to the connection pool and cluster maintenance.

    Example::

        settings = SystemSettings(
            max_connections_per_node=200,
            max_socket_idle_time=timedelta(seconds=30),
        )
        cluster = await ClusterDefinition("localhost", 3000) \\
            .with_system_settings(settings) \\
            .connect()
    """

    min_connections_per_node: Optional[int] = None
    max_connections_per_node: Optional[int] = None
    conn_pools_per_node: Optional[int] = None
    max_socket_idle_time: Optional[timedelta] = None
    tend_interval: Optional[timedelta] = None

    def apply_to(self, policy: ClientPolicy) -> ClientPolicy:
        """Apply non-None fields to *policy*, returning the same object."""
        if self.max_connections_per_node is not None:
            policy.max_conns_per_node = self.max_connections_per_node
        if self.conn_pools_per_node is not None:
            policy.conn_pools_per_node = self.conn_pools_per_node
        if self.max_socket_idle_time is not None:
            policy.idle_timeout = int(self.max_socket_idle_time.total_seconds() * 1000)
        if self.tend_interval is not None:
            policy.tend_interval = int(self.tend_interval.total_seconds() * 1000)
        return policy
