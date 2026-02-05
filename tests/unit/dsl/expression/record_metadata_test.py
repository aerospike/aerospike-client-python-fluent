"""Unit tests for DSL record metadata expressions. Order matches RecordMetadataTests."""

from aerospike_fluent import Exp, parse_dsl


class TestRecordMetadata:
    """Test record metadata expressions."""

    def test_device_size(self):
        """$.deviceSize() > 1048576."""
        expected = Exp.gt(Exp.device_size(), Exp.int_val(1048576))
        result = parse_dsl("$.deviceSize() > 1048576")
        assert result == expected

    def test_memory_size(self):
        """$.memorySize() > 1048576."""
        expected = Exp.gt(Exp.device_size(), Exp.int_val(1048576))
        result = parse_dsl("$.memorySize() > 1048576")
        assert result == expected

    def test_record_size(self):
        """$.recordSize() > 1048576."""
        expected = Exp.gt(Exp.device_size(), Exp.int_val(1048576))
        result = parse_dsl("$.recordSize() > 1048576")
        assert result == expected

    def test_digest_modulo(self):
        """$.digestModulo(3) == 0 and == $.digestModulo bin."""
        expected = Exp.eq(Exp.digest_modulo(3), Exp.int_val(0))
        result = parse_dsl("$.digestModulo(3) == 0")
        assert result == expected

        expected2 = Exp.eq(Exp.digest_modulo(3), Exp.int_bin("digestModulo"))
        result = parse_dsl("$.digestModulo(3) == $.digestModulo")
        assert result == expected2

    def test_is_tombstone(self):
        """$.isTombstone()."""
        expected = Exp.is_tombstone()
        result = parse_dsl("$.isTombstone()")
        assert result == expected

    def test_key_exists(self):
        """$.keyExists()."""
        expected = Exp.key_exists()
        result = parse_dsl("$.keyExists()")
        assert result == expected

    def test_last_update(self):
        """$.lastUpdate() < $.updateBy and $.updateBy > $.lastUpdate()."""
        expected_left = Exp.lt(Exp.last_update(), Exp.int_bin("updateBy"))
        result = parse_dsl("$.lastUpdate() < $.updateBy")
        assert result == expected_left

        expected_right = Exp.gt(Exp.int_bin("updateBy"), Exp.last_update())
        result = parse_dsl("$.updateBy > $.lastUpdate()")
        assert result == expected_right

    def test_since_update(self):
        """$.sinceUpdate() < 7200000, < $.intBin, < $.sinceUpdate; $.sinceUpdate > $.sinceUpdate()."""
        expected = Exp.lt(Exp.since_update(), Exp.int_val(7200000))
        result = parse_dsl("$.sinceUpdate() < 7200000")
        assert result == expected

        expected2 = Exp.lt(Exp.since_update(), Exp.int_bin("intBin"))
        result = parse_dsl("$.sinceUpdate() < $.intBin")
        assert result == expected2

        expected3 = Exp.lt(Exp.since_update(), Exp.int_bin("sinceUpdate"))
        result = parse_dsl("$.sinceUpdate() < $.sinceUpdate")
        assert result == expected3

        expected4 = Exp.gt(Exp.int_bin("sinceUpdate"), Exp.since_update())
        result = parse_dsl("$.sinceUpdate > $.sinceUpdate()")
        assert result == expected4

    def test_set_name(self):
        """$.setName() == 'groupA' or 'groupB'; $.mySetBin == $.setName()."""
        expected = Exp.or_([
            Exp.eq(Exp.set_name(), Exp.string_val("groupA")),
            Exp.eq(Exp.set_name(), Exp.string_val("groupB")),
        ])
        result = parse_dsl('$.setName() == "groupA" or $.setName() == "groupB"')
        assert result == expected

        expected2 = Exp.eq(Exp.string_bin("mySetBin"), Exp.set_name())
        result = parse_dsl("$.mySetBin == $.setName()")
        assert result == expected2

    def test_ttl(self):
        """$.ttl() <= 86400."""
        expected = Exp.le(Exp.ttl(), Exp.int_val(86400))
        result = parse_dsl("$.ttl() <= 86400")
        assert result == expected

    def test_void_time(self):
        """$.voidTime() == -1."""
        expected = Exp.eq(Exp.void_time(), Exp.int_val(-1))
        result = parse_dsl("$.voidTime() == -1")
        assert result == expected

    def test_metadata_with_logical_operators_expressions(self):
        """$.deviceSize() > 1024 and $.ttl() < 300; and OR variant."""
        expected_and = Exp.and_([
            Exp.gt(Exp.device_size(), Exp.int_val(1024)),
            Exp.lt(Exp.ttl(), Exp.int_val(300)),
        ])
        result = parse_dsl("$.deviceSize() > 1024 and $.ttl() < 300")
        assert result == expected_and

        expected_or = Exp.or_([
            Exp.gt(Exp.device_size(), Exp.int_val(1024)),
            Exp.lt(Exp.ttl(), Exp.int_val(300)),
        ])
        result = parse_dsl("$.deviceSize() > 1024 or $.ttl() < 300")
        assert result == expected_or

    def test_metadata_as_expression_with_logical_operator(self):
        """$.isTombstone() and $.ttl() < 300; $.ttl() < 300 or $.keyExists()."""
        expected_and = Exp.and_([
            Exp.is_tombstone(),
            Exp.lt(Exp.ttl(), Exp.int_val(300)),
        ])
        result = parse_dsl("$.isTombstone() and $.ttl() < 300")
        assert result == expected_and

        expected_or = Exp.or_([
            Exp.lt(Exp.ttl(), Exp.int_val(300)),
            Exp.key_exists(),
        ])
        result = parse_dsl("$.ttl() < 300 or $.keyExists()")
        assert result == expected_or
