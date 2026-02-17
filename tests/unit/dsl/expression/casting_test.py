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

"""Unit tests for DSL casting expressions."""

import pytest

from aerospike_fluent import DslParseException, Exp, parse_dsl


class TestCasting:
    """Test casting expressions."""

    def test_float_to_int_cast(self):
        """$.intBin1 > $.floatBin1.asInt() and .get(type: INT) variant → gt(intBin1, intBin(floatBin1))."""
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_bin("floatBin1"))
        result = parse_dsl("$.intBin1 > $.floatBin1.asInt()")
        assert result == expected
        result = parse_dsl("$.intBin1.get(type: INT) > $.floatBin1.asInt()")
        assert result == expected

    def test_int_to_float_cast(self):
        """$.floatBin1 > $.intBin1.asFloat() → gt(floatBin1, floatBin(intBin1))."""
        expected = Exp.gt(Exp.float_bin("floatBin1"), Exp.float_bin("intBin1"))
        result = parse_dsl("$.floatBin1 > $.intBin1.asFloat()")
        assert result == expected

    def test_int_to_float_cast_explicit_int_left(self):
        """$.intBin1.get(type: INT) > $.intBin2.asFloat() → gt(intBin1, floatBin(intBin2))."""
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.float_bin("intBin2"))
        result = parse_dsl("$.intBin1.get(type: INT) > $.intBin2.asFloat()")
        assert result == expected

    def test_negative_invalid_types_comparison(self):
        """$.stringBin1.get(type: STRING) > $.intBin2.asFloat() raises Cannot compare STRING to FLOAT."""
        with pytest.raises(DslParseException, match="Cannot compare STRING to FLOAT"):
            parse_dsl("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")
