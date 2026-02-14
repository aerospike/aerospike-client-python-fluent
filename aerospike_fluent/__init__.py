"""Aerospike Fluent Client - A fluent API wrapper for the Aerospike async Python client."""

from aerospike_fluent.aio import FluentClient, KeyValueService, Session, TransactionalSession, ClusterDefinition, Host
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
from aerospike_fluent.exp import Exp, val
from aerospike_fluent.policy.behavior import Behavior
from aerospike_fluent.sync import SyncFluentClient

__version__ = "0.1.0"

__all__ = [
    "AerospikeError",
    "AuthenticationError",
    "AuthorizationError",
    "BackoffError",
    "Behavior",
    "ClusterDefinition",
    "CommitError",
    "ConnectionError",
    "DataSet",
    "DslParseException",
    "Exp",
    "FluentClient",
    "GenerationError",
    "Host",
    "Index",
    "IndexContext",
    "IndexTypeEnum",
    "InvalidNamespaceError",
    "InvalidNodeError",
    "KeyValueService",
    "parse_ctx",
    "parse_dsl",
    "parse_dsl_with_index",
    "ParseResult",
    "PlaceholderValues",
    "QueryTerminatedError",
    "QuotaError",
    "SecurityError",
    "SerializationError",
    "Session",
    "SyncFluentClient",
    "TimeoutError",
    "TransactionalSession",
    "val",
]

