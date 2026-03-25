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

"""Exceptions raised by the expression DSL."""

from __future__ import annotations


class DslParseException(Exception):
    """Raised when a filter expression string cannot be parsed or validated.

    Indicates invalid syntax, unknown operators, type mismatches, or other
    input that the DSL layer refuses before building a server-side filter.

    Example:
        Handling a user-supplied filter string::

            from aerospike_fluent import parse_dsl, DslParseException

            try:
                expr = parse_dsl(user_filter)
            except DslParseException as e:
                raise ValueError(str(e)) from e

    See Also:
        :func:`~aerospike_fluent.parse_dsl`: Primary entry point that may raise
            this exception.
    """


class NoApplicableFilterError(Exception):
    """Internal signal that no secondary-index filter can represent an expression.

    Used inside the DSL filter visitor when a valid parse tree still cannot be
    lowered to a secondary-index filter. Not part of
    the public stable API; callers working only with :func:`parse_dsl` should
    expect :class:`DslParseException` instead.
    """
