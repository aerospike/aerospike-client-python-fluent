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

"""Async client and operations for the Aerospike Fluent API."""

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.aio.cluster import Cluster
from aerospike_fluent.aio.cluster_definition import ClusterDefinition, Host
from aerospike_fluent.aio.info import InfoCommands
from aerospike_fluent.aio.operations.index import IndexBuilder
from aerospike_fluent.aio.operations.query import QueryBuilder
from aerospike_fluent.aio.transactional_session import TransactionalSession
from aerospike_fluent.aio.session import Session

__all__ = [
    "Cluster",
    "ClusterDefinition",
    "FluentClient",
    "Host",
    "InfoCommands",
    "IndexBuilder",
    "QueryBuilder",
    "Session",
    "TransactionalSession",
]

