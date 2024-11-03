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
```json
{
    "event": "conversion",
    "offer": "special10",
    "creative": "banner1",
    "channel": "email",
    "amount": 99.99,
    "ip": "127.0.0.1"
}
```

## features

### time bucketing
events are automatically stored in time buckets:

- daily (`d:timestamp:...`) - always enabled
- hourly (`h:timestamp:...`) - optional, enabled via config

time buckets use the configured timezone (defaults to utc) and store data in unix timestamp format.

### metric types

#### bitmap metrics (`bmp:`)
used for counting unique values (e.g., unique ips, user ids). perfect for cardinality queries.

```rust
bitmap: vec!["ip".to_string()]  // will track unique ips
```

#### add metrics (`add:`)
counter metrics that can combine multiple fields using the `~` separator:

```rust
add: vec![
    "event".to_string(),                    // count by event
    "event~offer".to_string(),              // count by event + offer combinations
    "event~offer~creative".to_string(),     // count by event + offer + creative
]
```

#### add value metrics (`adv:`)
numerical aggregations that combine a value with compound keys:

```rust
add_value: vec![AddValueConfig {
    key: "offer~event".to_string(),    // group by offer + event
    add_key: "amount".to_string(),     // sum the amount field
}]
```

### compound keys
fields can be combined using the `~` separator to create compound metrics. for example:

- `event~offer` tracks combinations of events and offers
- `event~offer~creative` tracks combinations of events, offers, and creatives

the order of fields in compound keys is automatically sorted for consistency.

## performance

`traqq` is designed for high-performance event processing:

- processes 40,000+ events/second on a single thread
- supports concurrent processing across multiple threads
- memory efficient (~6.7mb for 5000 events)
- scales well with event complexity

### benchmark results

```
realistic event processing:
- processing latency: ~191µs per event
- generates ~28 redis operations per event

concurrent processing (4 threads):
- 41,649 events/second
- 6 redis commands/event average

scaling with complexity:
- simple events (1 field): 19,620 events/sec
- complex events (20 fields): 4,427 events/sec
```

## current status

this is a work in progress. currently implemented:

- event parsing and validation
- compound key generation (e.g., event~offer~creative~channel)
  - supports a configurable level of compound keys
- redis command generation
  - (bmp) bitmap metrics
  - (add) increment metrics
  - (adv) incrementBy (add_value) metrics

planned features:

- additional storage adapters
- http api interface
- query interface

## usage

currently, traqq serves as a library for generating redis commands from events. the main components are:

- `IncomingEvent`: parses and validates json events
- `MetricsConfig`: configures metric mapping rules
- `ProcessedEvent`: generates redis commands based on the config

example configuration and usage can be found in `main.rs`.

## run stuff
```bash
# run main()
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
