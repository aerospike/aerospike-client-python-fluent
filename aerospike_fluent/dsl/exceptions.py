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
