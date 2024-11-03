<p>
    <img src="https://owij9.s3.amazonaws.com/ncZ5NFpXC.svg" height="125">
</p>

# traqq

A high-performance event processing system that transforms JSON events into optimized Redis commands for real-time analytics, enabling complex queries without post-processing.

## Performance Highlights

- **Processing Speed**: 40,000+ events/second on a single thread
- **Memory Efficient**: ~6.7MB for 5000 events
- **Concurrent Support**: Multi-threaded event processing
- **Scaling Performance**:
  - Level 1: ~19,685 events/sec
  - Level 5: ~11,214 events/sec
  - Level 10: ~7,543 events/sec
  - Level 20: ~4,343 events/sec

## Quick Start

See [main.rs](src/main.js) for a basic usage demonstration.

```rust
use traqq::prelude::*;

let config = TraqqConfig::default();
let event = IncomingEvent::from_json(serde_json::json!({
    "event": "purchase",
    "amount": 99.99,
    "ip": "127.0.0.1",
    "utm_source": "google",
    "utm_medium": "cpc"
})).unwrap();

match ProcessedEvent::from_incoming(event, &config) {
    Ok(processed) => processed.pretty_print(),
    Err(e) => println!("Error: {}", e),
}
```

## Core Features

### 1. Configuration System

```rust
TraqqConfig {
    time: TimeConfig {
        store_hourly: true,
        timezone: "America/New_York".to_string(),
    },
    mapping: MappingConfig {
        bitmap: vec!["ip".to_string()],
        add: vec!["event".to_string()],
        add_value: vec![/* value metrics */],
    },
    limits: LimitsConfig {
        max_field_length: 128,
        max_value_length: 512,
        max_combinations: 1000,
        max_metrics_per_event: 1000,
    },
}
```

### 2. Event Processing Pipeline

1. Event Ingestion
2. Sanitization
3. Property Extraction
4. Metric Generation
5. Redis Command Generation

### 3. Redis Integration

#### Key Structure
```
<metric_type>:<bucket_type>:<timestamp>:<pattern>:<values>
```

#### Example Commands

```redis
PFADD bmp:d:1696118400:ip 127.0.0.1
INCR add:d:1696118400:event:purchase
INCRBY adv:d:1696118400:amount:event:purchase 99.99
```

## Components

- Core Types: `TraqqConfig`, `IncomingEvent`, `ProcessedEvent`, `RedisCommand`, `RedisCommandType`
- Utility Modules: `constants.rs`, `utils.rs`

## Development Status

### Implemented
- Event parsing and validation
- Property sanitization
- Compound key generation
- Metric generation
- Redis command preparation
- Concurrent processing
- Performance benchmarking

### Planned
- Redis pipeline execution
- Query engine
- HTTP API interface
- WebAssembly module
- Additional storage adapters

## Usage Guide

### Installation

```toml
[dependencies]
traqq = "0.1.1"
```

### Running Tests
```bash
cargo test
cargo test -- --nocapture
cargo run
```

## License

MIT

