# Reading Data

All reads go through `session.query()`, which returns a
[`QueryBuilder`](../api/query.md). Chain methods to configure the query, then
call `.execute()` to get a [`RecordStream`](../api/record-stream.md).

## Point Read (Single Key)

```python
users = DataSet.of("test", "users")

stream = await session.query(users.id(1)).execute()
result = await stream.first_or_raise()
print(result.record.bins)  # {'name': 'Alice', 'age': 30}
```

## Set Scan (All Records)

```python
stream = await session.query(users).execute()
async for result in stream:
    print(result.record.bins)
stream.close()
```

## Batch Read (Multiple Keys)

```python
stream = await session.query(*users.ids(1, 2, 3)).execute()
async for result in stream:
    print(result.record.key, result.record.bins)
stream.close()
```

## Selecting Bins

Return only specific bins to reduce network transfer:

```python
stream = await session.query(users).bins(["name", "age"]).execute()
```

Or exclude all bins (metadata only):

```python
stream = await session.query(users).with_no_bins().execute()
```

## Filtering with DSL

Use the [Expression DSL](expression-dsl.md) to filter records server-side:

```python
stream = await (
    session.query(users)
    .where("$.age > 25 and $.status == 'active'")
    .execute()
)
```

Or with a pre-built `FilterExpression`:

```python
from aerospike_fluent import Exp

expr = Exp.and_([
    Exp.gt(Exp.int_bin("age"), Exp.int_val(25)),
    Exp.eq(Exp.string_bin("status"), Exp.string_val("active")),
])
stream = await session.query(users).where(expr).execute()
```

## Partition Filtering

Query specific partitions for parallel consumption:

```python
stream = await (
    session.query(users)
    .on_partitions(0, 1, 2)
    .execute()
)
```

Or a contiguous range:

```python
stream = await (
    session.query(users)
    .on_partition_range(begin=0, count=1024)
    .execute()
)
```

## Query Policies

Fine-tune query behavior:

```python
from aerospike_async import QueryDuration

stream = await (
    session.query(users)
    .where("$.age > 18")
    .expected_duration(QueryDuration.LONG)
    .chunk_size(500)
    .execute()
)
```

## RecordResult

Each item in the stream is a [`RecordResult`](../api/record-result.md):

```python
async for result in stream:
    if result.is_ok:
        record = result.record
        print(record.key, record.bins, record.generation, record.expiration)
    else:
        print(f"Error: {result.result_code}")
```

Use `record_or_raise()` to raise on error results:

```python
async for result in stream:
    record = result.record_or_raise()
```

## Query Hints

Influence secondary index selection with [`QueryHint`](../api/query-hint.md):

```python
from aerospike_fluent import QueryHint

stream = await (
    session.query(users)
    .where("$.age > 25")
    .with_hint(QueryHint(index_name="age_idx"))
    .execute()
)
```
