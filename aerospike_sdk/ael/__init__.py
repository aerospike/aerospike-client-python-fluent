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

"""Aerospike Expression Language (AEL) support for query filter expressions.

This module provides parsing of text-based AEL expressions into FilterExpression objects.
AEL lets developers write readable query conditions like:
    "$.country == 'US' and $.order_total > 500"

instead of using verbose Expression builder APIs.
"""

from __future__ import annotations

from aerospike_sdk.ael.exceptions import AelParseException
from aerospike_sdk.ael.parser import AELParser, parse_ael

__all__ = [
    "AelParseException",
    "AELParser",
    "parse_ael",
]
