"""DSL exceptions."""

from __future__ import annotations


class DslParseException(Exception):
    """
    Represents a general processing exception that can occur during DSL expression parsing.

    It is typically not expected to be caught by the caller, but rather indicates a potentially
    unrecoverable issue like invalid input, failing validation or unsupported functionality.
    """

    pass
