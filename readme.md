# traqq

`traqq` is a work-in-progress event processing system that transforms json events into
optimized redis commands for real-time analytics. it provides a flexible configuration
system for mapping event properties into different types of redis metrics, enabling
complex queries that would otherwise be difficult to achieve without aggregating events
or performing multiple queries and post-processing.

## overview

traqq takes json events and maps them to various redis data structures based on your configuration. it supports:

- bitmap metrics for unique counting
- counter metrics for aggregations
- value-based metrics for numerical aggregations
- automatic time-based bucketing (hourly and daily)

### example event
```bash
{
    "event": "conversion",
    "offer": "special10",
    "creative": "banner1",
    "channel": "email",
    "amount": 99.99,
    "ip": "127.0.0.1"
}

$ cargo run

processed event summary
----------------------
timestamp: 2024-11-03 02:24:08.557110 UTC
event_name: conversion

properties:
  - amount: 99.99
  - channel: email
  - creative: banner1
  - event: conversion
  - ip: 127.0.0.1
  - offer: special10

bitmap metrics:
  - 127.0.0.1

add metrics:
  - event:conversion: 1
  - event~offer:conversion~special10: 1
  - channel~event~offer:email~conversion~special10: 1
  - offer:special10: 1
  - creative~event~offer:banner1~conversion~special10: 1

add_value metrics:
  - amount:event~offer:conversion~special10: 99.99

redis commands queue:
  - Bitmap | key: bmp:d:1730505600:127.0.0.1 | value: 1.00
  - Bitmap | key: bmp:h:1730599200:127.0.0.1 | value: 1.00
  - IncrementBy | key: add:d:1730505600:event:conversion | value: 1.00
  - IncrementBy | key: add:h:1730599200:event:conversion | value: 1.00
  - IncrementBy | key: add:d:1730505600:offer:special10 | value: 1.00
  - IncrementBy | key: add:h:1730599200:offer:special10 | value: 1.00
  - IncrementBy | key: add:d:1730505600:event~offer:conversion~special10 | value: 1.00
  - IncrementBy | key: add:h:1730599200:event~offer:conversion~special10 | value: 1.00
  - IncrementBy | key: add:d:1730505600:creative~event~offer:banner1~conversion~special10 | value: 1.00
  - IncrementBy | key: add:h:1730599200:creative~event~offer:banner1~conversion~special10 | value: 1.00
  - IncrementBy | key: add:d:1730505600:channel~event~offer:email~conversion~special10 | value: 1.00
  - IncrementBy | key: add:h:1730599200:channel~event~offer:email~conversion~special10 | value: 1.00
  - IncrementBy | key: adv:d:1730505600:amount:event~offer:conversion~special10 | value: 99.99
  - IncrementBy | key: adv:h:1730599200:amount:event~offer:conversion~special10 | value: 99.99
```

## features

### time bucketing
events are automatically stored in time buckets:

- daily `d:timestamp:...` always enabled
- hourly `h:timestamp:...` optional, enabled via config

time buckets use the configured timezone (defaults to utc) and store data in unix timestamp format.

### metric types

#### bitmap metrics
used for counting unique values (e.g., unique ips, user ids). perfect for cardinality queries.

```rust
bitmap: vec!["ip".to_string()]  // will track unique ips
```

#### add metrics
counter metrics that can combine multiple fields using the `~` separator:

```rust
add: vec![
    "event".to_string(),                    // count by event
    "event~offer".to_string(),              // count by event + offer combinations
    "event~offer~creative".to_string(),     // count by event + offer + creative combinations
]
```

#### add_value metrics
numerical aggregations that combine a value with compound keys:

```rust
add_value: vec![AddValueConfig {
    key: "offer~event".to_string(),    // group by offer + event
    add_key: "amount".to_string(),     // sum the amount field
}]
```

### compound keys
fields can be combined using the `~` separator to create compound metrics. there's not a limit
for the number of fields in a compound key, although i would probably recommend keeping it to a
reasonable number, like 3-4 fields.

for example:

- `event~offer` tracks combinations of event and offer
- `event~offer~creative` tracks unique combinations of event, offer, and creative

the order of fields in compound keys is automatically sorted for consistency.

## performance

`traqq` is designed for high-performance event processing:

- processes 40,000+ events/second on a single thread
- supports concurrent processing across multiple threads
- memory efficient (~6.7mb for 5000 events)
- scales well with event complexity

### benchmarks

realistic event processing:
- processing latency: ~191µs per event
- generates ~28 redis operations per event

concurrent processing (4 threads):
- 41,649 events/second
- 6 redis commands/event average

scaling with complexity:
- simple events: 19,620 events/sec
- complex events (20 fields): 4,427 events/sec

```
realistic event benchmark:
========================
processing latency: 144.166µs

metrics generated:
bitmap metrics: 4
add metrics: 8
add value metrics: 2
total redis ops: 28
test tests::test_realistic_event_processing ... ok

concurrent processing test results:
==================================
threads: 4
events/thread: 250
total events: 1000
total duration: 23.810166ms
events/sec: 41998.87
total redis commands: 6000
avg redis commands/event: 6.00
test tests::test_concurrent_processing ... ok

complexity level: 1
events processed: 5000
total duration: 253.997167ms
avg duration/event: 34.506µs
events/sec: 19685.26
total redis commands: 30000
avg redis commands/event: 6.00
approximate memory usage: 6.68 mb

complexity level: 5
events processed: 5000
total duration: 445.874666ms
avg duration/event: 53.646µs
events/sec: 11213.91
total redis commands: 30000
avg redis commands/event: 6.00
approximate memory usage: 6.68 mb

complexity level: 10
events processed: 5000
total duration: 662.863667ms
avg duration/event: 73.751µs
events/sec: 7543.03
total redis commands: 30000
avg redis commands/event: 6.00
approximate memory usage: 6.68 mb

complexity level: 20
events processed: 5000
total duration: 1.151256875s
avg duration/event: 122.208µs
events/sec: 4343.08
total redis commands: 30000
avg redis commands/event: 6.00
approximate memory usage: 6.68 mb
```

## current status

this is a work in progress. currently implemented:

- event parsing and validation
- compound key generation (e.g., `event~offer~creative~channel`)
  - supports a configurable level of compound keys
- redis command generation
  - (bmp:) bitmap commands
  - (add:) increment commands
  - (adv:) incrementBy (add_value) commands

planned features:

- redis pipeline execution
- additional storage adapters
- http api interface
- query interface

## usage

currently, traqq serves as a library for generating redis commands from events. the main components are:

- `IncomingEvent` parses and validates json events
- `MetricsConfig` configures metric mapping rules
- `ProcessedEvent` generates redis commands based on the `MetricsConfig`

```rust
let config = MetricsConfig {
    time: TimeConfig {
        store_hourly: true,
        timezone: "America/New_York".to_string(),
    },
    mapping: MappingConfig {
        bitmap: vec!["ip".to_string()],
        add: vec![
            "event".to_string(),
            "offer".to_string(),
            "event~offer".to_string(),
            "event~offer~creative".to_string(),
            "event~offer~channel".to_string(),
        ],
        add_value: vec![AddValueConfig {
            key: "offer~event".to_string(),
            add_key: "amount".to_string(),
        }],
    },
    limits: LimitsConfig {
        max_field_length: constants::defaults::MAX_FIELD_LENGTH,
        max_value_length: constants::defaults::MAX_VALUE_LENGTH,
        max_combinations: constants::defaults::MAX_COMBINATIONS,
        max_metrics_per_event: constants::defaults::MAX_METRICS_PER_EVENT,
    },
};

// example event creation from json
let event = IncomingEvent::from_json(serde_json::json!({
    "event": "conversion",
    "offer": "SPECIAL10",
    "creative": "banner1",
    "channel": "email",
    "amount": 99.99,
    "ip": "127.0.0.1"
})).unwrap();

match ProcessedEvent::from_incoming(event, &config) {
    Ok(processed) => {
        processed.pretty_print();
    },
    Err(e) => {
        println!("Error processing event: {}", e);
    }
}
```

example configuration and usage can be found in `main.rs`.

## run stuff
```bash
# run main() example above
cargo run

# run tests
cargo test

# run tests with benchmarking output
cargo test -- --nocapture
```

## contrib

this project is in active development. issues and pull requests are welcome!

## license

mit
