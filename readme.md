<p>
    <img src="https://owij9.s3.amazonaws.com/ncZ5NFpXC.svg" height="125">
</p>

# traqq

`traqq` is a work-in-progress event processing system that transforms json events into
optimized redis commands for real-time analytics. it provides a flexible configuration
system for mapping event properties into different lists of redis commands, enabling
complex queries that would otherwise be difficult to achieve without aggregating events
or performing multiple queries and post-processing.

eventually, it will also serve as a query interface for the stored data and be exposed
via an http api as well as a wasm module for use as a native library in other projects.

it essentially serves as a redis command generator.. or a really funky database indexing
system if you want to think of it that way.

## overview

traqq takes flat json event objects and transforms them to various redis data commands
that will track different types of metrics based on your configuration. it supports:

- hyperloglog for unique value tracking
- increment for occurence tracking
- incrementBy for tracking sums, counts, and other aggregations

it supports compound keys for tracking combinations of event properties and
for aggregating numerical values. pretty much everything is configurable, including the
maximum field length, value length, and the maximum number of metrics generated from a single
event. it also supports configurable limits for the number of compound fields in a key and
if you want to track metrics at an hourly granularity or just daily.

## performance

traqq is designed for high-performance event processing:

- processes 40,000+ events/second on a single thread
- supports concurrent processing across multiple threads
- memory efficient (~6.7mb for 5000 events)
- scales well with event complexity

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

- daily `d:timestamp` always enabled
- hourly `h:timestamp` optional, enabled via config

time buckets use the configured timezone (defaults to utc) and store data in unix timestamp format.

### metric types

#### bitmap
used for counting unique values (e.g., unique ips, user ids). perfect for cardinality queries.

```rust
bitmap: vec!["ip".to_string()]  // will track unique ips
```

#### add
counter metrics that can combine multiple fields using the `~` separator:

```rust
add: vec![
    "event".to_string(),                    // count by event
    "event~offer".to_string(),              // count by occurence of combinations of event + offer
    "event~offer~creative".to_string(),     // count by occurence of combinations of event + offer + creative
]
```

#### add_value
numerical aggregations that combine a value with compound keys:

```rust
add_value: vec![AddValueConfig {
    key: "offer~event".to_string(),    // group by occurence of combinations of offer + event
    add_key: "amount".to_string(),     // sum of the amount field
}]
```

### compound keys
fields can be combined using the `~` delimiter to create compound metrics. there's not a limit
for the number of fields in a compound key, although i would probably recommend keeping it to a
reasonable number, like 3-4 fields.

for example:

- `event~offer` tracks combinations of event and offer
- `event~offer~creative` tracks unique combinations of event, offer, and creative

the order of fields in compound keys is automatically sorted for consistency.

## benchmarks

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
  - bitmap commands
  - increment commands
  - incrementBy (add_value) commands

planned features:

- redis pipeline execution
- redis query engine
- http api interface
- wasm module
- additional storage adapters

## components

currently, the main components are:

- `MetricsConfig` configures metric mapping rules
- `IncomingEvent` parses and validates json events
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
