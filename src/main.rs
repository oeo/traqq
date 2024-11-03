//! traqq: High-performance event tracking and analytics processing
//! 
//! This module provides the core functionality for processing and analyzing event data.
//! It handles event ingestion, validation, metric generation, and Redis command preparation.
//! 
//! # Architecture
//! 
//! - Events flow through validation and sanitization
//! - Metrics are generated based on configurable patterns
//! - Redis commands are prepared for persistence

use std::collections::{HashMap, HashSet};
use serde::Deserialize;
use serde_json::json;
use chrono::{DateTime, Utc, Timelike};

use traqq::utils;
use traqq::constants;

/// Represents the time bucket granularity for metric aggregation
#[derive(Debug, Clone)]
pub enum BucketType {
    /// Daily aggregation bucket
    Daily,
    /// Hourly aggregation bucket
    Hourly,
}

impl BucketType {
    /// Returns the string representation used in Redis keys
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Daily => "d",
            Self::Hourly => "h",
        }
    }
}

/// Configuration for time-based operations and aggregations
#[derive(Debug, Clone, Deserialize)]
pub struct TimeConfig {
    /// Whether to store hourly metrics in addition to daily
    pub store_hourly: bool,
    /// Timezone for bucket calculations (e.g., "UTC", "America/New_York")
    pub timezone: String,
}

impl Default for TimeConfig {
    fn default() -> Self {
        Self {
            store_hourly: false,
            timezone: constants::TIMEZONE.into(),
        }
    }
}

/// Configuration for metric pattern mapping
#[derive(Debug, Clone, Deserialize)]
pub struct MappingConfig {
    /// Patterns for bitmap (unique count) metrics
    pub bitmap: Vec<String>,
    /// Patterns for additive metrics
    pub add: Vec<String>,
    /// Patterns for value-based additive metrics
    pub add_value: Vec<AddValueConfig>,
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            bitmap: vec![],
            add: vec!["event".into()],
            add_value: vec![],
        }
    }
}

/// Configuration for value-based additive metrics
#[derive(Debug, Clone, Deserialize)]
pub struct AddValueConfig {
    /// Pattern for grouping the metric
    pub key: String,
    /// Field containing the numeric value to add
    pub add_key: String,
}

/// Configuration for processing limits and constraints
#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    /// Maximum length for field names
    pub max_field_length: usize,
    /// Maximum length for field values
    pub max_value_length: usize,
    /// Maximum number of unique metric combinations
    pub max_combinations: usize,
    /// Maximum metrics allowed per event
    pub max_metrics_per_event: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_field_length: constants::MAX_FIELD_LENGTH.into(),
            max_value_length: constants::MAX_VALUE_LENGTH.into(),
            max_combinations: constants::MAX_COMBINATIONS.into(),
            max_metrics_per_event: constants::MAX_METRICS_PER_EVENT.into(),
        }
    }
}

/// Primary configuration for the Traqq system
#[derive(Debug, Clone, Deserialize)]
pub struct TraqqConfig {
    /// Time-based configuration settings
    pub time: TimeConfig,
    /// Metric mapping configuration
    pub mapping: MappingConfig,
    /// Processing limits and constraints
    pub limits: LimitsConfig,
}

impl Default for TraqqConfig {
    fn default() -> Self {
        Self {
            time: TimeConfig::default(),
            mapping: MappingConfig::default(),
            limits: LimitsConfig::default(),
        }
    }
}

/// Represents an incoming event before processing
#[derive(Debug, Clone, Deserialize)]
pub struct IncomingEvent {
    /// The primary event identifier/name
    pub event: String,

    /// Additional event properties as arbitrary JSON
    #[serde(flatten)]
    pub properties: serde_json::Value,
}

/// Represents a fully processed event with generated metrics
#[derive(Debug, Clone)]
pub struct ProcessedEvent {
    /// Original event name after sanitization
    pub event_name: String,
    /// Timestamp when the event was processed
    pub timestamp: DateTime<Utc>,
    /// Raw event properties after sanitization
    pub raw_properties: HashMap<String, String>,
    /// Combined properties including derived values
    pub combined_properties: HashMap<String, String>,
    /// Numeric values extracted from properties
    pub numeric_values: HashMap<String, f64>,
    /// String values extracted from properties
    pub string_values: HashMap<String, String>,
    /// Boolean values extracted from properties
    pub boolean_values: HashMap<String, bool>,
    /// Generated bitmap metric values
    pub bitmap_metrics: Vec<String>,
    /// Generated additive metric values
    pub add_metrics: HashMap<String, i64>,
    /// Generated value-based additive metrics
    pub add_value_metrics: HashMap<String, f64>,
    /// Redis commands generated for persistence
    pub redis_commands: Vec<RedisCommand>,
}

/// Represents a command to be executed against Redis
#[derive(Debug, Clone)]
pub struct RedisCommand {
    /// Full Redis key for the operation
    pub key: String,
    /// Value to be stored/updated
    pub value: String,
    /// Type of Redis operation to perform
    pub command_type: RedisCommandType,
    /// Timestamp when the command was generated
    pub timestamp: DateTime<Utc>,
    /// Additional metadata about the metric
    pub metadata: RedisMetadata,
}

/// Enumerates the types of Redis operations supported
#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommandType {
    /// HyperLogLog operation for unique counting
    Bitmap,
    /// HyperLogLog operation (alias for Bitmap)
    HyperLogLog,
    /// Simple increment by 1 operation (INCR)
    Increment,
    /// Increment by specific amount (INCRBY)
    IncrementBy(f64),
}

/// Additional metadata for Redis commands
#[derive(Debug, Clone)]
pub struct RedisMetadata {
    /// Type of metric (e.g., "bmp", "add", "adv")
    pub metric_type: String,
    /// Keys used in the metric pattern
    pub keys: Vec<String>,
    /// Optional key for value-based metrics
    pub add_key: Option<String>,
}

impl TraqqConfig {
    /// Creates a new configuration with default values
    pub fn default() -> Self {
        Self {
            time: TimeConfig::default(),
            mapping: MappingConfig::default(),
            limits: LimitsConfig::default(),
        }
    }

    /// Validates the configuration settings
    /// 
    /// # Returns
    /// - `Ok(())` if configuration is valid
    /// - `Err(String)` with description if invalid
    /// 
    /// # Validation Rules
    /// - Timezone must be valid
    /// - Patterns must be unique
    /// - Mapping patterns must be valid
    pub fn validate(&self) -> Result<(), String> {
        // Validate timezone
        utils::parse_timezone(&self.time.timezone)?;

        // Track unique patterns
        let mut unique_patterns = HashSet::new();
        
        // Validate bitmap patterns
        for pattern in &self.mapping.bitmap {
            if !unique_patterns.insert(pattern) {
                return Err(format!("duplicate pattern: {}", pattern));
            }
        }

        // Validate additive patterns
        for pattern in &self.mapping.add {
            if !unique_patterns.insert(pattern) {
                return Err(format!("duplicate add pattern: {}", pattern));
            }
            utils::validate_mapping_pattern(pattern)?;
        }

        // Validate value-based patterns
        for config in &self.mapping.add_value {
            if !unique_patterns.insert(&config.key) {
                return Err(format!("duplicate pattern: {}", config.key));
            }
            utils::validate_mapping_pattern(&config.key)?;
        }

        Ok(())
    }

    pub fn get_time_buckets(&self, timestamp: DateTime<Utc>) -> Result<Vec<(i64, BucketType)>, String> {
        let tz = utils::parse_timezone(&self.time.timezone)?;
        let local_time = timestamp.with_timezone(&tz);
        let mut buckets = Vec::new();
        
        // daily bucket is enabled by default
        let daily = local_time
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| "failed to create daily timestamp".to_string())?
            .and_local_timezone(tz)
            .earliest()
            .ok_or_else(|| "failed to convert daily timestamp to UTC".to_string())?
            .timestamp();

        buckets.push((daily, BucketType::Daily));

        // hourly bucket is enabled by config
        if self.time.store_hourly {
            let hourly = local_time
                .date_naive()
                .and_hms_opt(local_time.hour(), 0, 0)
                .ok_or_else(|| "failed to create hourly timestamp".to_string())?
                .and_local_timezone(tz)
                .earliest()
                .ok_or_else(|| "failed to convert hourly timestamp to UTC".to_string())?
                .timestamp();

            buckets.push((hourly, BucketType::Hourly));
        }

        Ok(buckets)
    }
}

impl IncomingEvent {
    pub fn validate_and_sanitize(&mut self, config: &TraqqConfig) -> Result<(), String> {
        // Sanitize event name
        self.event = utils::sanitize_value(&self.event, config.limits.max_field_length)?
            .ok_or_else(|| "event name cannot be empty".to_string())?;

        // Collect required keys
        let mut required_keys: HashSet<String> = ["event"]
            .iter()
            .map(|&s| s.to_string())
            .chain(config.mapping.bitmap.iter().cloned())
            .collect();

        // Add pattern keys
        required_keys.extend(
            config.mapping.add.iter()
                .flat_map(|p| p.split('~'))
                .map(String::from)
        );

        // Add value keys
        for config in &config.mapping.add_value {
            required_keys.extend(config.key.split('~').map(String::from));
            required_keys.insert(config.add_key.clone());
        }

        // Sanitize properties
        if let serde_json::Value::Object(props) = &self.properties {
            let mut sanitized_props = serde_json::Map::new();
            
            for (key, value) in props {
                if !required_keys.contains(key) {
                    continue;
                }
                
                if let Some(sanitized) = match value {
                    serde_json::Value::String(s) => utils::sanitize_value(s, constants::MAX_VALUE_LENGTH)?
                        .map(|s| json!(s)),
                    _ => Some(value.clone()),
                } {
                    sanitized_props.insert(key.clone(), sanitized);
                }
            }

            sanitized_props.insert("event".to_string(), json!(self.event.clone()));
            self.properties = serde_json::Value::Object(sanitized_props);
        }

        Ok(())
    }

    pub fn from_json(json: serde_json::Value) -> Result<Self, String> {
        let properties = json.clone();
        if let Some(event) = json.get("event").and_then(|e| e.as_str()) {
            Ok(IncomingEvent {
                event: event.to_string(),
                properties,
            })
        } else {
            Err("missing required 'event' field".to_string())
        }
    }
}

impl ProcessedEvent {
    pub fn from_incoming(mut event: IncomingEvent, config: &TraqqConfig) -> Result<Self, String> {
        event.validate_and_sanitize(config)?;

        let mut processed = ProcessedEvent {
            event_name: event.event.clone(),
            timestamp: Utc::now(),
            raw_properties: HashMap::new(),
            combined_properties: HashMap::new(),
            numeric_values: HashMap::new(),
            string_values: HashMap::new(),
            boolean_values: HashMap::new(),
            bitmap_metrics: Vec::new(),
            add_metrics: HashMap::new(),
            add_value_metrics: HashMap::new(),
            redis_commands: Vec::new(),
        };

        // Extract properties once
        if let serde_json::Value::Object(props) = &event.properties {
            for (key, value) in props {
                match value {
                    serde_json::Value::String(s) => {
                        processed.raw_properties.insert(key.clone(), s.clone());
                        processed.string_values.insert(key.clone(), s.clone());
                        processed.combined_properties.insert(key.clone(), s.clone());
                    }
                    serde_json::Value::Number(n) => {
                        let val_str = n.to_string();
                        processed.raw_properties.insert(key.clone(), val_str.clone());
                        processed.combined_properties.insert(key.clone(), val_str);
                        if let Some(float_val) = n.as_f64() {
                            processed.numeric_values.insert(key.clone(), float_val);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        let val_str = b.to_string();
                        processed.raw_properties.insert(key.clone(), val_str.clone());
                        processed.combined_properties.insert(key.clone(), val_str);
                        processed.boolean_values.insert(key.clone(), *b);
                    }
                    _ => continue,
                }
            }
        }

        // Add event name to properties
        processed.raw_properties.insert("event".to_string(), event.event.clone());
        processed.combined_properties.insert("event".to_string(), event.event);

        processed.process_metrics(config)?;
        Ok(processed)
    }

    // create a list of redis commands to execute, in the future
    // this could be extended to other adapters or a custom persistence layer
    fn process_metrics(&mut self, config: &TraqqConfig) -> Result<(), String> {
        let buckets = config.get_time_buckets(self.timestamp)?;
        
        // bitmap (bmp:) using HyperLogLog
        for bitmap_key in &config.mapping.bitmap {
            if let Some(value) = self.raw_properties.get(bitmap_key) {
                if !value.is_empty() {
                    self.bitmap_metrics.push(value.clone());
                    
                    for (bucket, bucket_type) in &buckets {
                        self.redis_commands.push(RedisCommand {
                            key: format!("bmp:{}:{}:{}", bucket_type.as_str(), bucket, bitmap_key),
                            value: value.to_string(),
                            command_type: RedisCommandType::HyperLogLog,
                            timestamp: self.timestamp,
                            metadata: RedisMetadata {
                                metric_type: "bmp".to_string(),
                                keys: vec![bitmap_key.clone()],
                                add_key: None,
                            },
                        });
                    }
                }
            }
        }

        // add (add:) 
        for add_pattern in &config.mapping.add {
            let keys: Vec<String> = add_pattern.split('~')
                .map(String::from)
                .collect();
            let sorted_keys = utils::sort_keys(&keys);
            let mut values = Vec::new();
            let mut has_all_keys = true;
            
            for key in &sorted_keys {
                if let Some(value) = self.raw_properties.get(key) {
                    if value.is_empty() {
                        has_all_keys = false;
                        break;
                    }
                    values.push(value.clone());
                } else {
                    has_all_keys = false;
                    break;
                }
            }

            if has_all_keys {
                let metric_key = format!("{}:{}", sorted_keys.join("~"), values.join("~"));
                self.add_metrics.insert(metric_key.clone(), 1);
                
                for (bucket, bucket_type) in &buckets {
                    self.redis_commands.push(RedisCommand {
                        key: format!("add:{}:{}:{}", bucket_type.as_str(), bucket, metric_key),
                        value: "1".to_string(),
                        command_type: RedisCommandType::Increment,
                        timestamp: self.timestamp,
                        metadata: RedisMetadata {
                            metric_type: "add".to_string(),
                            keys: sorted_keys.clone(),
                            add_key: None,
                        },
                    });
                }
            }
        }

        // add_value (adv:) 
        for add_value_config in &config.mapping.add_value {
            let keys: Vec<String> = add_value_config.key.split('~')
                .map(String::from)
                .collect();
            let sorted_keys = utils::sort_keys(&keys);
            let mut values = Vec::new();
            let mut has_all_keys = true;
            
            for key in &sorted_keys {
                if let Some(value) = self.raw_properties.get(key) {
                    if value.is_empty() {
                        has_all_keys = false;
                        break;
                    }
                    values.push(value.clone());
                } else {
                    has_all_keys = false;
                    break;
                }
            }

            if has_all_keys {
                if let Some(value_str) = self.raw_properties.get(&add_value_config.add_key) {
                    if let Ok(amount) = value_str.parse::<f64>() {
                        let metric_key = format!("{}:{}:{}", 
                            add_value_config.add_key,
                            sorted_keys.join("~"),
                            values.join("~")
                        );

                        self.add_value_metrics.insert(metric_key.clone(), amount);
                        
                        for (bucket, bucket_type) in &buckets {
                            self.redis_commands.push(RedisCommand {
                                key: format!("adv:{}:{}:{}", bucket_type.as_str(), bucket, metric_key),
                                value: amount.to_string(),
                                command_type: RedisCommandType::IncrementBy(amount),
                                timestamp: self.timestamp,
                                metadata: RedisMetadata {
                                    metric_type: "adv".to_string(),
                                    keys: sorted_keys.clone(),
                                    add_key: Some(add_value_config.add_key.clone()),
                                },
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn pretty_print(&self) {
        println!("\nMetrics Summary:");
        println!("---------------");
        println!("Bitmap metrics: {}", self.bitmap_metrics.len());
        println!("Add metrics: {}", self.add_metrics.len());
        println!("Add value metrics: {}", self.add_value_metrics.len());
        println!("Total Redis ops: {}", self.redis_commands.len());

        println!("\nSample Redis commands:");
        for cmd in self.redis_commands.iter().take(5) {
            match &cmd.command_type {
                RedisCommandType::HyperLogLog | RedisCommandType::Bitmap => {
                    println!("  - HyperLogLog | key: {} | value: {}", cmd.key, cmd.value);
                }
                RedisCommandType::Increment => {
                    println!("  - Increment   | key: {} | value: 1", cmd.key);
                }
                RedisCommandType::IncrementBy(amount) => {
                    println!("  - IncrementBy | key: {} | value: {}", cmd.key, amount);
                }
            }
        }
    }
}

fn main() {
    let config = TraqqConfig {
        time: TimeConfig::default(),
        mapping: MappingConfig {
            bitmap: vec!["ip".to_string()],
            add: vec![
                "event".to_string(),
                "event~utm_campaign".to_string(),
                "event~utm_source~utm_medium".to_string(),
                "event~utm_source~utm_medium~utm_campaign".to_string(),
                "event~os".to_string(),
            ],
            add_value: vec![
                AddValueConfig { key: "event".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_campaign".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_source~utm_medium".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_source~utm_medium~utm_campaign".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~os".to_string(), add_key: "amount".to_string() },
            ],
        },
        limits: LimitsConfig::default(),
    };

    // example event creation from json
    let json_event = utils::create_test_event();
    let event = IncomingEvent::from_json(json_event).unwrap();

    match ProcessedEvent::from_incoming(event, &config) {
        Ok(processed) => {
            processed.pretty_print();
        },
        Err(e) => {
            println!("Error processing event: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{
        distributions::Alphanumeric,
        prelude::SliceRandom,
        thread_rng,
        Rng,
    };
    use tokio::time::*;

    #[derive(Debug)]
    struct BenchmarkResult {
        events_processed: usize,
        total_duration: Duration,
        avg_duration: Duration,
        events_per_second: f64,
        redis_commands_generated: usize,
        memory_usage: usize,
    }

    // Helper functions
    fn random_string(len: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn create_test_config(complexity: usize) -> TraqqConfig {
        TraqqConfig {
            time: TimeConfig {
                store_hourly: false,
                timezone: "UTC".to_string(),
            },
            mapping: MappingConfig {
                bitmap: vec!["ip".to_string()],
                add: generate_add_patterns(complexity),
                add_value: generate_value_patterns(complexity),
            },
            limits: LimitsConfig::default(),
        }
    }

    fn generate_add_patterns(complexity: usize) -> Vec<String> {
        let mut patterns = vec![
            "event".to_string(),
            "event~utm_source".to_string(),
            "event~utm_medium".to_string(),
            "event~utm_campaign".to_string(),
        ];

        if complexity >= 2 {
            patterns.push("event~utm_source~utm_medium".to_string());
        }
        if complexity >= 3 {
            patterns.push("event~utm_source~utm_medium~utm_campaign".to_string());
        }
        if complexity >= 4 {
            patterns.push("event~browser~device".to_string());
        }
        if complexity >= 5 {
            patterns.push("event~browser~device~utm_source".to_string());
        }

        for i in 6..complexity {
            patterns.push(format!("event~utm_source~utm_medium~utm_campaign~{}", i));
        }

        patterns
    }

    fn generate_value_patterns(complexity: usize) -> Vec<AddValueConfig> {
        let mut patterns = vec![
            AddValueConfig {
                key: "event".to_string(),
                add_key: "amount".to_string(),
            },
            AddValueConfig {
                key: "event~utm_source".to_string(),
                add_key: "amount".to_string(),
            },
        ];

        if complexity >= 2 {
            patterns.push(AddValueConfig {
                key: "event~utm_source~utm_medium".to_string(),
                add_key: "amount".to_string(),
            });
        }

        if complexity >= 3 {
            patterns.push(AddValueConfig {
                key: "event~utm_source~utm_medium~utm_campaign".to_string(),
                add_key: "amount".to_string(),
            });
        }

        for i in 4..complexity {
            patterns.push(AddValueConfig {
                key: format!("event~utm_source~utm_medium~{}", i),
                add_key: "amount".to_string(),
            });
        }

        patterns
    }

    fn create_realistic_event() -> IncomingEvent {
        let mut rng = thread_rng();
        let mut props = serde_json::Map::new();
        
        // Core event data
        props.insert("event".to_string(), json!("conversion"));
        props.insert("offer_id".to_string(), json!(format!("OFFER_{}", rng.gen_range(1000..9999))));
        props.insert("amount".to_string(), json!(rng.gen_range(10.0..500.0)));
        
        // User data
        props.insert("user_agent".to_string(), json!(utils::SAMPLE_USER_AGENTS.choose(&mut rng).unwrap()));
        props.insert("ip".to_string(), json!(format!("{}.{}.{}.{}", 
            rng.gen_range(1..255),
            rng.gen_range(1..255),
            rng.gen_range(1..255),
            rng.gen_range(1..255)
        )));
        
        // UTM parameters
        props.insert("utm_source".to_string(), json!(utils::SAMPLE_UTM_SOURCES.choose(&mut rng).unwrap()));
        props.insert("utm_medium".to_string(), json!(utils::SAMPLE_UTM_MEDIUMS.choose(&mut rng).unwrap()));
        props.insert("utm_campaign".to_string(), json!(utils::SAMPLE_UTM_CAMPAIGNS.choose(&mut rng).unwrap()));
        
        // Additional metadata
        props.insert("browser".to_string(), json!("chrome"));
        props.insert("device".to_string(), json!("desktop"));
        props.insert("country".to_string(), json!("US"));
        props.insert("language".to_string(), json!("en-US"));
        props.insert("page_url".to_string(), json!("https://example.com/checkout"));
        props.insert("referrer".to_string(), json!("https://google.com"));
        
        // Random noise that should be filtered
        props.insert("session_id".to_string(), json!(random_string(32)));
        props.insert("timestamp_ms".to_string(), json!(Utc::now().timestamp_millis()));
        props.insert("client_random".to_string(), json!(random_string(10)));

        IncomingEvent {
            event: "conversion".to_string(),
            properties: serde_json::Value::Object(props),
        }
    }

    fn create_realistic_config() -> TraqqConfig {
        TraqqConfig {
            time: TimeConfig {
                store_hourly: true,
                timezone: "UTC".to_string(),
            },
            mapping: MappingConfig {
                bitmap: vec![
                    "ip".to_string(),
                    "browser".to_string(),
                    "device".to_string(),
                    "country".to_string(),
                ],
                add: vec![
                    "event".to_string(),
                    "event~utm_source".to_string(),
                    "event~utm_medium".to_string(),
                    "event~utm_campaign".to_string(),
                    "event~utm_source~utm_medium".to_string(),
                    "event~offer_id".to_string(),
                    "event~country".to_string(),
                    "event~device".to_string(),
                ],
                add_value: vec![
                    AddValueConfig {
                        key: "event~utm_source".to_string(),
                        add_key: "amount".to_string(),
                    },
                    AddValueConfig {
                        key: "event~offer_id".to_string(),
                        add_key: "amount".to_string(),
                    },
                ],
            },
            limits: LimitsConfig::default(),
        }
    }

    fn run_benchmark(event_complexity: usize, num_events: usize) -> BenchmarkResult {
        let config = create_test_config(event_complexity);
        let mut total_duration = Duration::new(0, 0);
        let mut total_commands = 0;
        let mut total_memory = 0;

        let start = Instant::now();
        
        for _ in 0..num_events {
            let json_event = utils::create_test_event();
            let event = IncomingEvent::from_json(json_event).unwrap();
            
            let event_start = Instant::now();
            match ProcessedEvent::from_incoming(event, &config) {
                Ok(processed) => {
                    total_commands += processed.redis_commands.len();
                    total_memory += std::mem::size_of_val(&processed) +
                        processed.redis_commands.len() * std::mem::size_of::<RedisCommand>() +
                        processed.combined_properties.len() * 64;
                }
                Err(e) => panic!("Failed to process event: {}", e),
            }
            total_duration += event_start.elapsed();
        }

        BenchmarkResult {
            events_processed: num_events,
            total_duration,
            avg_duration: total_duration / num_events as u32,
            events_per_second: num_events as f64 / start.elapsed().as_secs_f64(),
            redis_commands_generated: total_commands,
            memory_usage: total_memory,
        }
    }

    mod validation_tests {
        use super::*;

        #[test]
        fn test_empty_event_validation() {
            let config = TraqqConfig::default();
            let event = IncomingEvent {
                event: "".to_string(),
                properties: serde_json::json!({
                    "ip": "127.0.0.1",
                    "amount": 49.99
                }),
            };

            let result = ProcessedEvent::from_incoming(event, &config);
            assert!(result.is_err(), "empty event names should fail validation");
            assert_eq!(
                result.unwrap_err(),
                "event name cannot be empty".to_string()
            );
        }

        #[test]
        fn test_whitespace_event_validation() {
            let config = TraqqConfig::default();
            let event = IncomingEvent {
                event: "   ".to_string(),
                properties: serde_json::json!({
                    "ip": "127.0.0.1",
                    "amount": 49.99
                }),
            };

            let result = ProcessedEvent::from_incoming(event, &config);
            assert!(result.is_err(), "should fail with whitespace-only event name");
        }

        #[test]
        fn test_sanitize_value_string_manipulation() {
            let test_cases = vec![
                ("test~event:name", Some("test_event_name")),
                ("purchase", Some("purchase")),
                ("test_event", Some("test_event")),
                ("", None),
                ("   ", None),
            ];

            for (input, expected) in test_cases {
                let result = utils::sanitize_value(input, 100).unwrap();
                assert_eq!(result.as_deref(), expected, 
                    "sanitization failed for '{}' - expected '{:?}', got '{:?}'", 
                    input, expected, result);
            }
        }
    }

    mod performance_tests {
        use super::*;
        use std::sync::Arc;

        #[test]
        fn test_performance_scaling() {
            let complexities = [1, 2, 3, 5, 10, 20];
            let events_per_test = 5000;

            println!("\nPerformance Scaling Test Results:");
            println!("================================");
            
            for &complexity in &complexities {
                let result = run_benchmark(complexity, events_per_test);
                
                println!("\nComplexity Level: {}", complexity);
                println!("Events Processed: {}", result.events_processed);
                println!("Total Duration: {:?}", result.total_duration);
                println!("Avg Duration/Event: {:?}", result.avg_duration);
                println!("Events/sec: {:.2}", result.events_per_second);
                println!("Total Redis Commands: {}", result.redis_commands_generated);
                println!("Avg Redis Commands/Event: {:.2}", 
                    result.redis_commands_generated as f64 / events_per_test as f64);
                println!("Approximate Memory Usage: {:.2} MB", 
                    result.memory_usage as f64 / (1024.0 * 1024.0));
            }
        }

        #[test]
        fn test_concurrent_processing() {
            let config = Arc::new(create_test_config(5));
            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let events_per_thread = 150;
            
            let start = Instant::now();
            let mut handles = vec![];
            
            for _ in 0..num_threads {
                let config = Arc::clone(&config);
                handles.push(std::thread::spawn(move || {
                    let mut durations = vec![];
                    let mut commands = 0;
                    
                    for _ in 0..events_per_thread {
                        let json_event = utils::create_test_event();
                        let event = IncomingEvent::from_json(json_event).unwrap();
                        let event_start = Instant::now();
                        
                        if let Ok(processed) = ProcessedEvent::from_incoming(event, &config) {
                            commands += processed.redis_commands.len();
                        }
                        
                        durations.push(event_start.elapsed());
                    }
                    
                    (durations, commands)
                }));
            }

            let mut total_commands = 0;
            let mut all_durations = vec![];
            
            for handle in handles {
                let (durations, commands) = handle.join().unwrap();
                all_durations.extend(durations);
                total_commands += commands;
            }

            let total_duration = start.elapsed();
            let total_events = num_threads * events_per_thread;
            
            println!("\nConcurrent Processing Test Results:");
            println!("==================================");
            println!("Threads: {}", num_threads);
            println!("Events/Thread: {}", events_per_thread);
            println!("Total Events: {}", total_events);
            println!("Total Duration: {:?}", total_duration);
            println!("Events/sec: {:.2}", total_events as f64 / total_duration.as_secs_f64());
            println!("Total Redis Commands: {}", total_commands);
            println!("Avg Redis Commands/Event: {:.2}", 
                total_commands as f64 / total_events as f64);
        }
    }

    mod integration_tests {
        use super::*;

        #[test]
        fn test_realistic_event_processing() {
            let config = create_realistic_config();
            let event = create_realistic_event();
            
            let start = Instant::now();
            let processed = ProcessedEvent::from_incoming(event, &config).unwrap();
            let duration = start.elapsed();

            println!("\nRealistic Event Benchmark:");
            println!("========================");
            println!("Processing Latency: {:?}", duration);
            println!("\nMetrics Generated:");
            println!("Bitmap Metrics: {}", processed.bitmap_metrics.len());
            println!("Add Metrics: {}", processed.add_metrics.len());
            println!("Add Value Metrics: {}", processed.add_value_metrics.len());
            println!("Total Redis Ops: {}", processed.redis_commands.len());
            
            assert!(processed.bitmap_metrics.len() > 0);
            assert!(processed.add_metrics.len() > 0);
            assert!(processed.add_value_metrics.len() > 0);
        }

        #[test]
        fn test_complex_event_processing() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: true,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string(), "event~campaign".to_string()],
                    add_value: vec![AddValueConfig {
                        key: "event~campaign".to_string(),
                        add_key: "amount".to_string(),
                    }],
                },
                limits: LimitsConfig::default(),
            };

            let event = IncomingEvent {
                event: "purchase".to_string(),
                properties: serde_json::json!({
                    "ip": "127.0.0.1",
                    "campaign": "summer_sale",
                    "amount": 99.99
                }),
            };

            let processed = ProcessedEvent::from_incoming(event, &config).unwrap();
            
            assert_eq!(processed.redis_commands.len(), 8, 
                "Expected 8 Redis commands (2 bitmap + 4 add + 2 add_value)");

            // Verify command structure
            let hyperloglog_commands: Vec<_> = processed.redis_commands.iter()
                .filter(|cmd| matches!(cmd.command_type, RedisCommandType::HyperLogLog))
                .collect();

            for cmd in hyperloglog_commands {
                assert!(cmd.key.starts_with("bmp:"), "Key should start with 'bmp:'");
                assert!(cmd.key.contains(":ip"), "Key should contain ':ip'");
                assert_eq!(cmd.value, "127.0.0.1", "Value should be the IP address");
                assert_eq!(cmd.metadata.metric_type, "bmp");
                assert_eq!(cmd.metadata.keys, vec!["ip"]);
                assert!(cmd.metadata.add_key.is_none());
            }
        }
    }
}
