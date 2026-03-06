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

"""Aerospike Fluent Client - A fluent API wrapper for the Aerospike async Python client."""

from aerospike_async import AuthMode

from aerospike_fluent.aio import FluentClient, Session, TransactionalSession, ClusterDefinition, Host
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.dsl.exceptions import DslParseException
from aerospike_fluent.dsl.filter_gen import Index, IndexContext, IndexTypeEnum, ParseResult
from aerospike_fluent.dsl.parser import PlaceholderValues, parse_ctx, parse_dsl, parse_dsl_with_index
from aerospike_fluent.exceptions import (
    AerospikeError,
    AuthenticationError,
    AuthorizationError,
    BackoffError,
    CommitError,
    ConnectionError,
    GenerationError,
    InvalidNamespaceError,
    InvalidNodeError,
    QueryTerminatedError,
    QuotaError,
    SecurityError,
    SerializationError,
    TimeoutError,
)
from aerospike_fluent.error_strategy import ErrorHandler, ErrorStrategy, OnError
from aerospike_fluent.exp import Exp, val
from aerospike_fluent.policy.behavior import Behavior
from aerospike_fluent.record_result import RecordResult
from aerospike_fluent.record_stream import RecordStream
from aerospike_fluent.sync import SyncFluentClient
from aerospike_fluent.sync.record_stream import SyncRecordStream

__version__ = "0.1.0"

__all__ = [
    "AerospikeError",
    "AuthenticationError",
    "AuthMode",
    "AuthorizationError",
    "BackoffError",
    "Behavior",
    "ClusterDefinition",
    "CommitError",
    "ConnectionError",
    "DataSet",
    "DslParseException",
    "ErrorHandler",
    "ErrorStrategy",
    "Exp",
    "FluentClient",
    "GenerationError",
    "Host",
    "Index",
    "IndexContext",
    "IndexTypeEnum",
    "InvalidNamespaceError",
    "InvalidNodeError",
    "OnError",
    "parse_ctx",
    "parse_dsl",
    "parse_dsl_with_index",
    "ParseResult",
    "PlaceholderValues",
    "QueryTerminatedError",
    "QuotaError",
    "RecordResult",
    "RecordStream",
    "SecurityError",
    "SerializationError",
    "Session",
    "SyncFluentClient",
    "SyncRecordStream",
    "TimeoutError",
    "TransactionalSession",
    "val",
]

