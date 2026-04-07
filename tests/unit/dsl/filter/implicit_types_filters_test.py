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

"""Unit tests for implicit types filter generation."""

from aerospike_sdk import Exp, parse_ael_with_index


class TestImplicitTypesFilters:
    """Test filter generation with implicit type inference."""

    def test_implicit_int_no_filter_without_index(self):
        """$.intBin1 > 100 without index returns no Filter."""
        result = parse_ael_with_index("$.intBin1 > 100")
        assert result.filter is None
        assert result.exp == Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))

    def test_float_comparison(self):
        """$.floatBin1 >= 100.25 — no float filter support; filter is None."""
        result = parse_ael_with_index("$.floatBin1 >= 100.25")
        assert result.filter is None

    def test_boolean_comparison(self):
        """$.boolBin1 == true — no boolean filter support; filter is None."""
        result = parse_ael_with_index("$.boolBin1 == true")
        assert result.filter is None
