# traqq

High-performance event metrics system. Records JSON events into pluggable storage backends and queries them back with type-aware aggregation.

Rust rewrite of [trk2](legacy/) (CoffeeScript/Redis). Same concepts, better performance, backend-agnostic.

## What it does

You send JSON events. Traqq generates four types of metrics from each event:

- **bmp** (bitmap) - unique counts via HyperLogLog (e.g., unique IPs)
- **add** - counters per field value (e.g., purchases per event type)
- **adv** - value accumulators with sum/count summaries (e.g., revenue per campaign)
- **top** - sorted leaderboards (e.g., top geos by volume)

Compound keys are auto-generated from property pairs, so `event` + `geo` automatically creates `event~geo` metrics without extra config.

## Quick start

### As a library

```rust
use traqq::prelude::*;

let config = TraqqConfig {
    mapping: MappingConfig {
        bitmap: vec!["ip".into()],
        add: vec!["event".into(), "event~geo".into()],
        add_value: vec![AddValueConfig {
            key: "event~geo".into(),
            add_key: "amount".into(),
        }],
        top: vec!["geo".into()],
    },
    ..TraqqConfig::default()
};

let traqq = Traqq::new(config, Box::new(MemoryStorage::new()), "myapp").unwrap();

// record
traqq.record(IncomingEvent::from_json(serde_json::json!({
    "event": "purchase",
    "ip": "1.2.3.4",
    "geo": "US",
    "amount": 99.99
})).unwrap()).unwrap();

// query last 7 days
let result = traqq.query_days(7).unwrap();

// find specific metrics
let adds = result.find(FindOptions {
    metric_type: "add".into(),
    key: "event".into(),
    add_key: None,
    merge: true,
});

// shorthand
let top_geos = result.find_str("top/geo");
```

### As a server

```bash
# start with in-memory storage
traqq serve

# start with redis
traqq serve --storage redis --redis-url redis://127.0.0.1:6379

# record events
traqq record --event '{"event":"purchase","ip":"1.2.3.4","geo":"US","amount":99.99}'

# query last 10 days
traqq query --days 10
```

### TCP protocol

The server accepts newline-delimited JSON over TCP (default port 9876):

```json
{"cmd":"record","event":{"event":"purchase","ip":"1.2.3.4","amount":99.99}}
{"cmd":"query","min":1700000000,"max":1700086400}
{"cmd":"query_days","days":7}
{"cmd":"find","min":1700000000,"max":1700086400,"metric_type":"add","key":"event","merge":true}
```

Responses:

```json
{"success":true}
{"success":true,"data":[...]}
{"success":false,"error":"..."}
```

## Installation

```toml
[dependencies]
traqq = "0.2.0"

# with redis support
traqq = { version = "0.2.0", features = ["redis-storage"] }
```

## Storage backends

| Backend | Feature flag | Use case |
|---------|-------------|----------|
| Memory | default | Testing, embedded, ephemeral |
| Redis | `redis-storage` | Production, persistence |

The `Storage` trait is public. Implement it to add your own backend.

## Configuration

```rust
TraqqConfig {
    time: TimeConfig {
        store_hourly: false,          // also store hourly buckets
        timezone: "UTC".into(),       // bucket timezone
    },
    mapping: MappingConfig {
        bitmap: vec!["ip".into()],    // HyperLogLog unique counts
        add: vec!["event".into()],    // increment counters
        add_value: vec![...],         // value accumulators
        top: vec!["geo".into()],      // sorted set leaderboards
    },
    limits: LimitsConfig {
        max_field_length: 128,
        max_value_length: 512,
        max_combinations: 1000,
        max_metrics_per_event: 1000,
    },
}
```

## Key format

```
prefix:type:bucket:timestamp:pattern
```

Examples:
```
myapp:bmp:d:1700000000:ip
myapp:add:d:1700000000:event
myapp:adv:d:1700000000:amount:event~geo
myapp:adv:d:1700000000:amount:event~geo:i    (summary: sum + count)
myapp:top:d:1700000000:geo
myapp:k:d:1700000000                         (key tracking set)
```

## Performance

Apple M2 Max, single thread:

- 40,000+ events/sec processing
- ~6.7MB for 5,000 events
- Multi-threaded recording supported (Storage trait is Send + Sync)

```bash
cargo test -- --nocapture   # see benchmark output
```

## Project structure

```
src/
  lib.rs              # core types, Traqq struct, query system
  constants.rs        # defaults
  utils.rs            # sanitize, timezone, validation
  server.rs           # TCP server (JSON-line protocol)
  client.rs           # TCP client
  main.rs             # CLI
  tests.rs            # 49 tests
  storage/
    mod.rs            # Storage trait + tests
    memory.rs         # in-memory backend
    redis.rs          # redis backend (feature-gated)
```

## Tests

```bash
# default (44 tests, no external deps)
cargo test

# with redis integration tests (49 tests, requires running redis)
cargo test --features redis-storage
```

## License

MIT
