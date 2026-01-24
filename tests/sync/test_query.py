"""Tests for SyncFluentClient Query operations."""

import pytest
from aerospike_async import FilterExpression
from aerospike_fluent import DataSet, SyncFluentClient


@pytest.fixture
def client(aerospike_host):
    """Setup sync fluent client and test data for query tests."""
    with SyncFluentClient(seeds=aerospike_host) as client:
        # Clean up and insert test data
        # Clean up existing records
        for i in range(10):
            client.key_value("test", "query_test", i).delete()

        # Insert test records with age values
        for i in range(10):
            client.key_value("test", "query_test", i).put({"id": i, "age": 20 + i, "name": f"User{i}"})

        yield client

def test_query_basic(client):
    """Test basic query operation without filters."""
    recordset = client.query("test", "query_test").execute()
    count = 0
    for record in recordset:
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:  # Limit to first 5 for speed
            break

def test_query_with_dataset(client):
    """Test query using DataSet."""
    users = DataSet.of("test", "query_test")
    recordset = client.query(dataset=users).execute()
    count = 0
    for record in recordset:
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:
            break

def test_query_with_single_key(client):
    """Test query using a single Key."""
    users = DataSet.of("test", "query_test")
    key = users.id(5)

def test_query_with_multiple_keys(client):
    """Test query using multiple Keys."""
    users = DataSet.of("test", "query_test")
    keys = users.ids(6, 7)

def test_query_with_bins(client):
    """Test query with specific bin selection."""
    recordset = client.query("test", "query_test").bins(["name", "age"]).execute()
    count = 0
    for record in recordset:
        assert record is not None
        # Verify that at least one of the requested bins is present
        assert "name" in record.bins or "age" in record.bins
        count += 1
        if count >= 3:
            break

    recordset.close()
    assert count > 0

def test_query_with_filter_expression(client):
    """Test query with FilterExpression for server-side filtering."""
    # Create a filter expression for age >= 25
    filter_exp = FilterExpression.ge(
        FilterExpression.int_bin("age"),
        FilterExpression.int_val(25)
    )

    recordset = (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    for record in recordset:
        assert record is not None
        assert "age" in record.bins
        assert record.bins["age"] >= 25
        count += 1
        if count >= 5:
            break

    recordset.close()
    assert count > 0

def test_query_with_filter_expression_and(client):
    """Test query with FilterExpression using AND for multiple conditions."""
    # Create filter expression: age >= 25 AND age <= 27
    filter_exp = FilterExpression.and_([
        FilterExpression.ge(FilterExpression.int_bin("age"), FilterExpression.int_val(25)),
        FilterExpression.le(FilterExpression.int_bin("age"), FilterExpression.int_val(27))
    ])

    recordset = (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    for record in recordset:
        assert record is not None
        assert "age" in record.bins
        assert 25 <= record.bins["age"] <= 27
        count += 1
        if count >= 5:
            break

    recordset.close()
    assert count > 0
