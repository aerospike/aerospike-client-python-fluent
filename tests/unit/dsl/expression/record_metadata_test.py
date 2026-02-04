"""Unit tests for DSL record metadata expressions."""

from aerospike_fluent import Exp, parse_dsl


class TestRecordMetadata:
    """Test record metadata expressions."""

    def test_device_size(self):
        """Test $.deviceSize() > 1048576."""
        expected = Exp.gt(Exp.device_size(), Exp.int_val(1048576))
        result = parse_dsl("$.deviceSize() > 1048576")
        assert result == expected

    def test_record_size(self):
        """Test $.recordSize() > 1048576."""
        expected = Exp.gt(Exp.device_size(), Exp.int_val(1048576))
        result = parse_dsl("$.recordSize() > 1048576")
        assert result == expected

    def test_digest_modulo(self):
        """Test $.digestModulo(3) == 0."""
        expected = Exp.eq(Exp.digest_modulo(3), Exp.int_val(0))
        result = parse_dsl("$.digestModulo(3) == 0")
        assert result == expected

    def test_is_tombstone(self):
        """Test $.isTombstone()."""
        expected = Exp.is_tombstone()
        result = parse_dsl("$.isTombstone()")
        assert result == expected

    def test_key_exists(self):
        """Test $.keyExists()."""
        expected = Exp.key_exists()
        result = parse_dsl("$.keyExists()")
        assert result == expected

    def test_last_update(self):
        """Test $.lastUpdate() < $.updateBy."""
        expected = Exp.lt(Exp.last_update(), Exp.int_bin("updateBy"))
        result = parse_dsl("$.lastUpdate() < $.updateBy")
        assert result == expected

    def test_since_update(self):
        """Test $.sinceUpdate() < 7200000."""
        expected = Exp.lt(Exp.since_update(), Exp.int_val(7200000))
        result = parse_dsl("$.sinceUpdate() < 7200000")
        assert result == expected

    def test_set_name(self):
        """Test $.setName() == "groupA"."""
        expected = Exp.or_([
            Exp.eq(Exp.set_name(), Exp.string_val("groupA")),
            Exp.eq(Exp.set_name(), Exp.string_val("groupB"))
        ])
        result = parse_dsl('$.setName() == "groupA" or $.setName() == "groupB"')
        assert result == expected

    def test_ttl(self):
        """Test $.ttl() <= 86400."""
        expected = Exp.le(Exp.ttl(), Exp.int_val(86400))
        result = parse_dsl("$.ttl() <= 86400")
        assert result == expected

    def test_void_time(self):
        """Test $.voidTime() == -1."""
        expected = Exp.eq(Exp.void_time(), Exp.int_val(-1))
        result = parse_dsl("$.voidTime() == -1")
        assert result == expected

    def test_metadata_with_logical_operators(self):
        """Test $.deviceSize() > 1024 and $.ttl() < 300."""
        expected = Exp.and_([
            Exp.gt(Exp.device_size(), Exp.int_val(1024)),
            Exp.lt(Exp.ttl(), Exp.int_val(300))
        ])
        result = parse_dsl("$.deviceSize() > 1024 and $.ttl() < 300")
        assert result == expected

    def test_metadata_as_expression_with_logical_operator(self):
        """Test $.isTombstone() and $.ttl() < 300."""
        expected = Exp.and_([
            Exp.is_tombstone(),
            Exp.lt(Exp.ttl(), Exp.int_val(300))
        ])
        result = parse_dsl("$.isTombstone() and $.ttl() < 300")
        assert result == expected
