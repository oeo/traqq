//! # traqq
//!
//! High-performance event processing system for Redis analytics.
//!
//! ## Overview
//!
//! `traqq` transforms JSON events into optimized Redis commands for real-time analytics.
//! It provides a flexible configuration system for mapping event properties into different
//! types of Redis commands, enabling complex queries without post-processing.

pub mod utils;
pub mod constants;

use std::collections::{HashMap, HashSet};
use serde::Deserialize;
use serde_json::json;
use chrono::{DateTime, Utc, Timelike};

pub use crate::constants::*;

// Re-export commonly used types
pub mod prelude {
    pub use crate::{
        BucketType,
        TimeConfig,
        MappingConfig,
        AddValueConfig,
        LimitsConfig,
        TraqqConfig,
        IncomingEvent,
        ProcessedEvent,
        RedisCommand,
        RedisCommandType,
        RedisMetadata,
    };
}

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
    pub const fn as_str(&self) -> &'static str {
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
        for cmd in self.redis_commands.iter() {
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

#[cfg(test)]
mod tests;
