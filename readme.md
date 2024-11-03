<p>
    <img src="https://owij9.s3.amazonaws.com/ncZ5NFpXC.svg" height="125">
</p>

# traqq

`traqq` is a high-performance event processing system that transforms JSON events into optimized Redis commands for real-time analytics. It provides a flexible configuration system for mapping event properties into different types of Redis commands, enabling complex queries without the need for post-processing.

## Features

### Core Functionality
- **Event Processing**: Processes 40,000+ events/second on a single thread
- **Concurrent Processing**: Supports multi-threaded event processing
- **Memory Efficient**: ~6.7MB for 5000 events
- **Configurable Time Buckets**: 
  - Daily buckets (always enabled)
  - Optional hourly buckets
  - Configurable timezone support

### Metric Types

#### 1. Bitmap Metrics (HyperLogLog)
Used for counting unique values:
```rust
bitmap: vec!["ip".to_string()]  // tracks unique IPs
```

#### 2. Add Metrics (Increment)
Counter metrics that can combine multiple fields:
```rust
add: vec![
    "event".to_string(),                    // count by event
    "event~offer".to_string(),              // count combinations of event + offer
    "event~offer~creative".to_string(),     // count combinations of event + offer + creative
]
```

#### 3. Add Value Metrics (IncrementBy)
Numerical aggregations with compound keys:
```rust
add_value: vec![AddValueConfig {
    key: "offer~event".to_string(),         // group by offer + event
    add_key: "amount".to_string(),          // sum of amount field
}]
```

### Configuration System

#### Time Configuration
```rust
TimeConfig {
    store_hourly: true,                     // enable hourly buckets
    timezone: "America/New_York".to_string(),// timezone for bucket calculation
}
```

#### Mapping Configuration
```rust
MappingConfig {
    bitmap: vec!["ip".to_string()],         // unique value tracking
    add: vec!["event".to_string()],         // counter metrics
    add_value: vec![/* value metrics */],   // numerical aggregations
}
```

#### Limits Configuration
```rust
LimitsConfig {
    max_field_length: 128,                  // maximum field name length
    max_value_length: 512,                  // maximum value length
    max_combinations: 1000,                 // maximum unique combinations
    max_metrics_per_event: 1000,           // maximum metrics per event
}
```

### Event Processing Pipeline

1. **Event Ingestion**: Parse and validate JSON events
2. **Sanitization**: Clean and validate field names and values
3. **Property Extraction**: Extract and type-cast event properties
4. **Metric Generation**: Generate metrics based on configuration
5. **Redis Command Generation**: Prepare commands for persistence

### Example Usage

```rust
let config = TraqqConfig {
    time: TimeConfig {
        store_hourly: true,
        timezone: "UTC".to_string(),
    },
    mapping: MappingConfig {
        bitmap: vec!["ip".to_string()],
        add: vec![
            "event".to_string(),
            "event~utm_source".to_string(),
            "event~utm_medium".to_string(),
        ],
        add_value: vec![
            AddValueConfig {
                key: "event".to_string(),
                add_key: "amount".to_string(),
            },
        ],
    },
    limits: LimitsConfig::default(),
};

// Process an event
let event = IncomingEvent::from_json(serde_json::json!({
    "event": "purchase",
    "amount": 99.99,
    "ip": "127.0.0.1",
    "utm_source": "google",
    "utm_medium": "cpc"
})).unwrap();

match ProcessedEvent::from_incoming(event, &config) {
    Ok(processed) => {
        processed.pretty_print();  // Display generated metrics
    },
    Err(e) => println!("Error: {}", e),
}
```

### Performance Benchmarks

```
Realistic Event Processing:
========================
Processing latency: ~144µs/event
Metrics generated per event:
- Bitmap metrics: 4
- Add metrics: 8
- Add value metrics: 2
Total Redis ops: 28

Concurrent Processing (4 threads):
==============================
Events/thread: 250
Total events: 1000
Total duration: ~24ms
Events/sec: ~42,000
Redis commands/event: 6

Scaling with Complexity:
=====================
Level 1: ~19,685 events/sec
Level 5: ~11,214 events/sec
Level 10: ~7,543 events/sec
Level 20: ~4,343 events/sec
Memory usage: ~6.7MB for 5000 events
```

### Components

#### Core Types
- `TraqqConfig`: Main configuration struct
- `IncomingEvent`: Raw event data
- `ProcessedEvent`: Processed event with generated metrics
- `RedisCommand`: Generated Redis commands
- `RedisCommandType`: Supported Redis operations

#### Utility Modules
- `constants.rs`: System constants and defaults
- `utils.rs`: Helper functions for validation and processing

## Development Status

### Implemented
- [x] Event parsing and validation
- [x] Property sanitization
- [x] Compound key generation
- [x] Metric generation
- [x] Redis command preparation
- [x] Concurrent processing
- [x] Performance benchmarking

### Planned Features
- [ ] Redis pipeline execution
- [ ] Query engine
- [ ] HTTP API interface
- [ ] WebAssembly module
- [ ] Additional storage adapters

### Running Tests
```bash
# Run all tests
cargo test

# Run tests with benchmark output
cargo test -- --nocapture

# Run example
cargo run
```

---

### Redis Command Generation & Storage

When events are processed, they generate different types of Redis commands based on the metric type. Here's an example event and its resulting Redis commands:

```rust
// Input Event
let event = IncomingEvent::from_json(serde_json::json!({
    "event": "purchase",
    "amount": 99.99,
    "ip": "127.0.0.1",
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "summer_sale"
})).unwrap();
```

#### 1. Bitmap Metrics (HyperLogLog)
Used for counting unique values. Keys are prefixed with `bmp:`.

```redis
# Format: bmp:<bucket_type>:<timestamp>:<field>
PFADD bmp:d:1696118400:ip 127.0.0.1
PFADD bmp:h:1696118400:ip 127.0.0.1

# To query unique counts:
PFCOUNT bmp:d:1696118400:ip          # Daily unique IPs
PFCOUNT bmp:h:1696118400:ip          # Hourly unique IPs
```

#### 2. Add Metrics (Increment)
Counter metrics for event combinations. Keys are prefixed with `add:`.

```redis
# Format: add:<bucket_type>:<timestamp>:<pattern>:<values>
INCR add:d:1696118400:event:purchase
INCR add:d:1696118400:event~utm_source:purchase~google
INCR add:d:1696118400:event~utm_campaign:purchase~summer_sale

# To query counts:
GET add:d:1696118400:event:purchase   # Daily purchase count
GET add:d:1696118400:event~utm_source:purchase~google  # Daily purchases from Google
```

#### 3. Add Value Metrics (IncrementBy)
Numerical aggregations. Keys are prefixed with `adv:`.

```redis
# Format: adv:<bucket_type>:<timestamp>:<pattern>:<values>
INCRBY adv:d:1696118400:event:purchase 99.99
INCRBY adv:d:1696118400:event~utm_source:purchase~google 99.99

# To query sums:
GET adv:d:1696118400:event:purchase   # Daily purchase amount
GET adv:d:1696118400:event~utm_source:purchase~google  # Daily purchase amount from Google
```

#### Time Bucket Examples
```redis
# Daily buckets (d:) - UTC midnight timestamp
bmp:d:1696118400:ip
add:d:1696118400:event:purchase
adv:d:1696118400:event:purchase

# Hourly buckets (h:) - UTC hour timestamp
bmp:h:1696118400:ip
add:h:1696118400:event:purchase
adv:h:1696118400:event:purchase
```

#### Query Patterns

Common query patterns for retrieving metrics:

```redis
# Unique values in time range
PFCOUNT bmp:d:1696118400:ip
PFCOUNT bmp:d:1696204800:ip
PFMERGE temp_key bmp:d:1696118400:ip bmp:d:1696204800:ip
PFCOUNT temp_key                    # Unique IPs across two days

# Aggregations by source
MGET add:d:1696118400:event~utm_source:purchase~google \
     add:d:1696118400:event~utm_source:purchase~facebook

# Value totals by campaign
MGET adv:d:1696118400:event~utm_campaign:purchase~summer_sale \
     adv:d:1696118400:event~utm_campaign:purchase~winter_sale
```

The Redis key structure follows the pattern:
- `<metric_type>:<bucket_type>:<timestamp>:<pattern>:<values>`
  - metric_type: bmp, add, or adv
  - bucket_type: d (daily) or h (hourly)
  - timestamp: Unix timestamp for the bucket
  - pattern: Field combinations being tracked
  - values: Actual values being counted/summed

---

## License

MIT