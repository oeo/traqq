# TODO

Rust rewrite of trk2. Single library crate + optional binary.
Legacy CoffeeScript/Redis implementation frozen in `legacy/`.

## Gates

```
[x] 1. Event parsing, validation, sanitization
[x] 2. Property extraction (string, number, boolean)
[x] 3. Metric generation (bmp, add, addv, top) -> StorageCommand structs
[x] 4. Time bucketing (daily + hourly)
[x] 5. Configuration system with limits + validation
[x] 6. Performance benchmarks + concurrent processing tests
[x] 7. Storage trait + memory backend
[x] 8. Traqq service struct (owns config + storage, record() wired)
[x] 9. Key tracking (day key sets for query discovery)
[x] 10. Missing metric types (top, addv summary, compound keys)
[x] 11. Query system (query, queryDays, find)
[x] 12. Redis backend (feature flag)
[x] 13. TCP server + client
[x] 14. CLI interface
```

## Design decisions

- HyperLogLog for bmp (not bitmaps) — scales to 10k+ events/sec, ~0.81% error acceptable
- Storage trait is sync, async can wrap it
- Memory backend is the default, Redis behind `redis-storage` feature flag
- `ProcessedEvent` stays pure — no storage coupling in the processing pipeline
- Key format: `prefix:type:bucket:timestamp:pattern`
- TCP protocol: newline-delimited JSON, one command per line
- CLI: `traqq serve`, `traqq record`, `traqq query`

## Deferred

- RocksDB backend (feature flag, same trait)
- Hourly query granularity (currently daily only in query path)
- Minute-level buckets
- Config file support (TOML)
- Web UI / dashboard
- Pub/sub for real-time streaming
- Query ignore/accept glob filtering

## Done

- [x] Event parsing and validation
- [x] Property sanitization (invalid chars, length limits)
- [x] Compound key generation (manual config + auto-generation)
- [x] Metric generation (bmp, add, addv, top -> StorageCommand structs)
- [x] Time bucketing (daily + optional hourly)
- [x] Configuration system with defaults and validation
- [x] Performance benchmarks (40k+ events/sec single thread)
- [x] Concurrent processing tests
- [x] Utility functions (sanitize, timezone, pattern validation)
- [x] Storage trait with hash, hyperloglog, sorted set, set operations
- [x] MemoryStorage backend (HashMap, BTreeMap, HashSet based)
- [x] Traqq service struct (record -> process -> persist pipeline)
- [x] Key tracking per time bucket for query discovery
- [x] `top` metric type (sorted set increment/top-N)
- [x] `addv` summary tracking (`:i` suffix with sum/count)
- [x] Compound key auto-generation from property pairs
- [x] Query system: query(min, max), query_days(n), QueryResult::find(), find_str()
- [x] Query types: MetricResult, MetricData (Count/Hash/FloatHash/Summary/Ranked)
- [x] Query merge support across days for all metric types
- [x] RedisStorage backend behind `redis-storage` feature flag
- [x] Redis: HINCRBY, HINCRBYFLOAT, HGETALL, PFADD, PFCOUNT, ZINCRBY, ZREVRANGEBYSCORE, SADD, SMEMBERS
- [x] Redis integration tests (5 tests, full round-trip with real Redis)
- [x] TCP server: thread-per-connection, JSON-line protocol, Record/Query/QueryDays/Find commands
- [x] TCP client: connect, record, query, query_days, find
- [x] CLI: `traqq serve` (memory/redis), `traqq record --event`, `traqq query --days`
- [x] 44 tests (default) + 5 redis integration tests (with feature flag)
