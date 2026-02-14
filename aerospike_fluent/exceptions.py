"""Aerospike Fluent Client exception hierarchy.

Provides typed exceptions aligned with the Aerospike error model, enabling
targeted exception handling (e.g. ``except GenerationError`` rather than
inspecting result codes manually).

PAC (Python Async Client) exceptions are converted to PFC exceptions at
public API boundaries via :func:`convert_pac_exception`, preserving the
original cause through standard exception chaining (``raise ... from``).
"""

from __future__ import annotations

from aerospike_async.exceptions import (
    AerospikeError as PacAerospikeError,
    ConnectionError as PacConnectionError,
    InvalidNodeError as PacInvalidNodeError,
    ServerError as PacServerError,
    TimeoutError as PacTimeoutError,
)
from aerospike_async.exceptions import ResultCode


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class AerospikeError(Exception):
    """Base exception for all Aerospike fluent client errors.

    Attributes:
        result_code: The server ``ResultCode``, or ``None`` for client-side errors.
        in_doubt: ``True`` when a write may have completed despite the error.
    """

    def __init__(
        self,
        message: str = "",
        *,
        result_code: ResultCode | None = None,
        in_doubt: bool = False,
    ) -> None:
        super().__init__(message)
        self.result_code = result_code
        self.in_doubt = in_doubt


# ---------------------------------------------------------------------------
# Timeout / connectivity
# ---------------------------------------------------------------------------

class TimeoutError(AerospikeError):
    """Client or server timeout."""


class ConnectionError(AerospikeError):
    """Connection to the Aerospike cluster failed."""


class InvalidNodeError(AerospikeError):
    """The target node is invalid or unavailable."""


class InvalidNamespaceError(AerospikeError):
    """The requested namespace does not exist on the cluster."""


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

class SecurityError(AerospikeError):
    """Base class for security-related errors."""


class AuthenticationError(SecurityError):
    """Authentication failed (invalid credentials, expired password, etc.)."""


class AuthorizationError(SecurityError):
    """Insufficient permissions for the requested operation."""


# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------

class GenerationError(AerospikeError):
    """Record generation (version) check failed."""


class QuotaError(AerospikeError):
    """A server quota has been exceeded."""


class SerializationError(AerospikeError):
    """Serialization or deserialization of a value failed."""


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

class QueryTerminatedError(AerospikeError):
    """A query was aborted or terminated."""


class BackoffError(AerospikeError):
    """The server requested a backoff / rate-limit."""


class CommitError(AerospikeError):
    """A multi-record transaction (MRT) commit failed.

    Attributes:
        commit_error_type: The type of commit failure, if available.
        verify_records: Results from the verify phase, if available.
        roll_records: Results from the roll-forward/back phase, if available.
    """

    def __init__(
        self,
        message: str = "",
        *,
        commit_error_type: object | None = None,
        verify_records: list | None = None,
        roll_records: list | None = None,
        result_code: ResultCode | None = None,
        in_doubt: bool = False,
    ) -> None:
        super().__init__(message, result_code=result_code, in_doubt=in_doubt)
        self.commit_error_type = commit_error_type
        self.verify_records = verify_records
        self.roll_records = roll_records


# ---------------------------------------------------------------------------
# Factory: ResultCode -> typed exception
# ---------------------------------------------------------------------------

# Codes not yet exposed by the PAC are omitted; they will fall through to
# AerospikeError until the PAC adds them.

_RC_TO_TYPE: dict[ResultCode, type[AerospikeError]] = {
    ResultCode.GENERATION_ERROR: GenerationError,
    # Authentication
    ResultCode.NOT_AUTHENTICATED: AuthenticationError,
    ResultCode.INVALID_USER: AuthenticationError,
    # Security (catch-all for remaining security codes)
    ResultCode.ILLEGAL_STATE: SecurityError,
    ResultCode.USER_ALREADY_EXISTS: SecurityError,
    ResultCode.FORBIDDEN_PASSWORD: SecurityError,
    ResultCode.SECURITY_NOT_SUPPORTED: SecurityError,
    ResultCode.SECURITY_NOT_ENABLED: SecurityError,
    ResultCode.SECURITY_SCHEME_NOT_SUPPORTED: SecurityError,
    # Timeout
    ResultCode.TIMEOUT: TimeoutError,
    ResultCode.QUERY_TIMEOUT: TimeoutError,
    # Namespace
    ResultCode.INVALID_NAMESPACE: InvalidNamespaceError,
    # Query terminated
    ResultCode.QUERY_ABORTED: QueryTerminatedError,
}


def result_code_to_exception(
    result_code: ResultCode,
    message: str = "",
    in_doubt: bool = False,
) -> AerospikeError:
    """Map a ``ResultCode`` to the appropriate typed exception.

    Map a server result code to the appropriate typed exception.
    """
    cls = _RC_TO_TYPE.get(result_code, AerospikeError)
    return cls(message, result_code=result_code, in_doubt=in_doubt)


# ---------------------------------------------------------------------------
# Boundary converter: PAC exception -> PFC exception
# ---------------------------------------------------------------------------

def convert_pac_exception(exc: Exception) -> AerospikeError:
    """Convert a PAC exception to the appropriate PFC typed exception.

    The original exception is **not** set as ``__cause__`` here; callers
    should use ``raise convert_pac_exception(e) from e``.
    """
    if isinstance(exc, PacServerError):
        return result_code_to_exception(exc.result_code, str(exc), exc.in_doubt)

    if isinstance(exc, PacTimeoutError):
        return TimeoutError(str(exc))

    if isinstance(exc, PacConnectionError):
        return ConnectionError(str(exc))

    if isinstance(exc, PacInvalidNodeError):
        return InvalidNodeError(str(exc))

    if isinstance(exc, PacAerospikeError):
        return AerospikeError(str(exc))

    return AerospikeError(str(exc))
