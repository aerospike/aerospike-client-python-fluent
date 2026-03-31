# API Reference

The Aerospike Python Fluent Client is organized into three layers:

## Async API

The primary API. All operations are `async`/`await`.

| Class | Description |
|-------|-------------|
| [`FluentClient`](client.md) | Entry point — connect, create sessions, manage lifecycle |
| [`Session`](session.md) | Scoped reads and writes with a fixed `Behavior` |
| [`QueryBuilder`](query.md) | Build and execute read queries (point, set, batch) |
| [`WriteSegmentBuilder`](write-segment.md) | Build and execute writes (upsert, insert, update, replace, delete) |
| [`BatchOperationBuilder`](batch.md) | Low-level batch operation builder |
| [`IndexBuilder`](index-builder.md) | Create and drop secondary indexes |
| [`BackgroundTaskSession`](background.md) | Server-side background jobs (update, delete, touch, UDF) |
| [`UdfFunctionBuilder`](udf.md) | Foreground UDF execution |
| [`InfoCommands`](info.md) | Aerospike info protocol commands |
| [`TransactionalSession`](transactional-session.md) | Multi-record transactions |

## Sync API

Synchronous wrappers for the async API. Same functionality, no `async`/`await`.

| Class | Description |
|-------|-------------|
| [`SyncFluentClient`](sync/client.md) | Sync entry point |
| [`SyncSession`](sync/session.md) | Sync session |
| [`SyncQueryBuilder`](sync/query.md) | Sync query builder |

## Core

Shared types used by both async and sync APIs.

| Class | Description |
|-------|-------------|
| [`DataSet`](dataset.md) | Namespace + set pair, key factory |
| [`RecordResult`](record-result.md) | Single result from a query or batch |
| [`RecordStream`](record-stream.md) | Async iterator over query results |
| [`Behavior`](behavior.md) | Policy presets (timeouts, consistency) |
| [`ClusterDefinition`](cluster-definition.md) | Cluster connection configuration |
| [`ErrorStrategy`](error-strategy.md) | Error handling strategies |
| [`Exceptions`](exceptions.md) | Exception hierarchy |
| [`QueryHint`](query-hint.md) | Query optimization hints |
| [`IndexesMonitor`](indexes-monitor.md) | Background secondary index discovery |

## DSL

Expression parsing and filter generation.

| Class / Function | Description |
|-----------------|-------------|
| [`parse_dsl`](dsl-parser.md) | Parse DSL strings into filter expressions |
| [`FilterGenerator`](dsl-filter-gen.md) | Secondary index filter generation |
| [`Exp`](exp.md) | Programmatic expression builder |
