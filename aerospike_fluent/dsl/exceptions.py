"""DSL exceptions."""

from __future__ import annotations


class DslParseException(Exception):
    """
    Represents a general processing exception that can occur during DSL expression parsing.

    It is typically not expected to be caught by the caller, but rather indicates a potentially
    unrecoverable issue like invalid input, failing validation or unsupported functionality.
    """

    pass


class NoApplicableFilterError(Exception):
    """No applicable secondary-index filter could be derived from a DSL expression.

    This is an internal exception used by the DSL filter visitor to signal that
    the expression cannot be converted into a secondary-index ``Filter``.  It is
    not part of the public API.
    """

    pass
