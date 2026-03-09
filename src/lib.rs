//! # traqq
//!
//! High-performance event processing system for Redis analytics.
//!
//! ## Overview
//!
//! `traqq` transforms JSON events into optimized Redis commands for real-time analytics.
//! It provides a flexible configuration system for mapping event properties into different
//! types of Redis commands, enabling complex queries without post-processing.

pub mod client;
pub mod constants;
pub mod server;
pub mod storage;
pub mod utils;

use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};

pub use crate::constants::*;

// re-export commonly used types
pub mod prelude {
    #[cfg(feature = "redis-storage")]
    pub use crate::storage::redis::RedisStorage;
    pub use crate::{
        storage::{memory::MemoryStorage, Storage, StorageError},
        AddValueConfig, BucketType, CommandMetadata, DayResult, FindOptions, IncomingEvent,
        LimitsConfig, MappingConfig, MetricData, MetricResult, ProcessedEvent, QueryResult,
        StorageCommand, StorageCommandType, TimeConfig, Traqq, TraqqConfig,
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
    /// Patterns for sorted set (top-N / leaderboard) metrics
    pub top: Vec<String>,
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            bitmap: vec![],
            add: vec!["event".into()],
            add_value: vec![],
            top: vec![],
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
    /// Generated top/sorted set metrics
    pub top_metrics: HashMap<String, f64>,
    /// storage commands generated for persistence
    pub commands: Vec<StorageCommand>,
}

/// a backend-agnostic storage operation produced by event processing
#[derive(Debug, Clone)]
pub struct StorageCommand {
    /// full storage key for the operation
    pub key: String,
    /// value to be stored/updated
    pub value: String,
    /// type of storage operation to perform
    pub command_type: StorageCommandType,
    /// timestamp when the command was generated
    pub timestamp: DateTime<Utc>,
    /// additional metadata about the metric
    pub metadata: CommandMetadata,
}

/// the types of storage operations supported
#[derive(Debug, Clone, PartialEq)]
pub enum StorageCommandType {
    /// hyperloglog add for unique counting
    HyperLogLog,
    /// hash field increment by 1
    HashIncrement,
    /// hash field increment by float amount
    HashIncrementFloat(f64),
    /// sorted set member increment by amount
    SortedSetIncrement(f64),
}

/// metadata attached to each storage command
#[derive(Debug, Clone)]
pub struct CommandMetadata {
    /// type of metric (e.g., "bmp", "add", "adv")
    pub metric_type: String,
    /// keys used in the metric pattern
    pub keys: Vec<String>,
    /// optional key for value-based metrics
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

        // Validate top patterns
        for pattern in &self.mapping.top {
            utils::validate_mapping_pattern(pattern)?;
        }

        Ok(())
    }

    pub fn get_time_buckets(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<(i64, BucketType)>, String> {
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
            config
                .mapping
                .add
                .iter()
                .flat_map(|p| p.split('~'))
                .map(String::from),
        );

        // Add value keys
        for config in &config.mapping.add_value {
            required_keys.extend(config.key.split('~').map(String::from));
            required_keys.insert(config.add_key.clone());
        }

        // Add top pattern keys
        required_keys.extend(
            config
                .mapping
                .top
                .iter()
                .flat_map(|p| p.split('~'))
                .map(String::from),
        );

        // Sanitize properties
        if let serde_json::Value::Object(props) = &self.properties {
            let mut sanitized_props = serde_json::Map::new();

            for (key, value) in props {
                if !required_keys.contains(key) {
                    continue;
                }

                if let Some(sanitized) = match value {
                    serde_json::Value::String(s) => {
                        utils::sanitize_value(s, constants::MAX_VALUE_LENGTH)?.map(|s| json!(s))
                    }
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
            top_metrics: HashMap::new(),
            commands: Vec::new(),
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
                        processed
                            .raw_properties
                            .insert(key.clone(), val_str.clone());
                        processed.combined_properties.insert(key.clone(), val_str);
                        if let Some(float_val) = n.as_f64() {
                            processed.numeric_values.insert(key.clone(), float_val);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        let val_str = b.to_string();
                        processed
                            .raw_properties
                            .insert(key.clone(), val_str.clone());
                        processed.combined_properties.insert(key.clone(), val_str);
                        processed.boolean_values.insert(key.clone(), *b);
                    }
                    _ => continue,
                }
            }
        }

        // Add event name to properties
        processed
            .raw_properties
            .insert("event".to_string(), event.event.clone());
        processed
            .combined_properties
            .insert("event".to_string(), event.event);

        // auto-generate compound keys from property pairs.
        // for each pair (x, y) where neither key contains '~',
        // create x~y = val_x~val_y (sorted alphabetically).
        // this matches legacy trk2 behavior.
        let base_keys: Vec<String> = processed
            .raw_properties
            .keys()
            .filter(|k| !k.contains('~'))
            .cloned()
            .collect();

        for i in 0..base_keys.len() {
            for j in (i + 1)..base_keys.len() {
                let mut pair = [base_keys[i].clone(), base_keys[j].clone()];
                pair.sort();
                let compound_key = pair.join("~");

                if !processed.raw_properties.contains_key(&compound_key) {
                    if let (Some(v0), Some(v1)) = (
                        processed.raw_properties.get(&pair[0]),
                        processed.raw_properties.get(&pair[1]),
                    ) {
                        let compound_value = format!("{}~{}", v0, v1);
                        processed
                            .raw_properties
                            .insert(compound_key.clone(), compound_value.clone());
                        processed
                            .combined_properties
                            .insert(compound_key, compound_value);
                    }
                }
            }
        }

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
                        self.commands.push(StorageCommand {
                            key: format!("bmp:{}:{}:{}", bucket_type.as_str(), bucket, bitmap_key),
                            value: value.to_string(),
                            command_type: StorageCommandType::HyperLogLog,
                            timestamp: self.timestamp,
                            metadata: CommandMetadata {
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
            let keys: Vec<String> = add_pattern.split('~').map(String::from).collect();
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
                let pattern_key = sorted_keys.join("~");
                let field_value = values.join("~");
                self.add_metrics
                    .insert(format!("{}:{}", pattern_key, field_value), 1);

                for (bucket, bucket_type) in &buckets {
                    self.commands.push(StorageCommand {
                        key: format!("add:{}:{}:{}", bucket_type.as_str(), bucket, pattern_key),
                        value: field_value.clone(),
                        command_type: StorageCommandType::HashIncrement,
                        timestamp: self.timestamp,
                        metadata: CommandMetadata {
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
            let keys: Vec<String> = add_value_config.key.split('~').map(String::from).collect();
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
                        let label_value = values.join("~");
                        let metric_key =
                            format!("{}:{}", add_value_config.add_key, sorted_keys.join("~"),);

                        self.add_value_metrics
                            .insert(format!("{}:{}", metric_key, label_value), amount);

                        for (bucket, bucket_type) in &buckets {
                            // per-label hash: field is the label value, increment by amount
                            self.commands.push(StorageCommand {
                                key: format!(
                                    "adv:{}:{}:{}",
                                    bucket_type.as_str(),
                                    bucket,
                                    metric_key
                                ),
                                value: label_value.clone(),
                                command_type: StorageCommandType::HashIncrementFloat(amount),
                                timestamp: self.timestamp,
                                metadata: CommandMetadata {
                                    metric_type: "adv".to_string(),
                                    keys: sorted_keys.clone(),
                                    add_key: Some(add_value_config.add_key.clone()),
                                },
                            });

                            // summary hash (:i suffix): sum and count
                            let summary_key =
                                format!("adv:{}:{}:{}:i", bucket_type.as_str(), bucket, metric_key);
                            self.commands.push(StorageCommand {
                                key: summary_key.clone(),
                                value: "sum".to_string(),
                                command_type: StorageCommandType::HashIncrementFloat(amount),
                                timestamp: self.timestamp,
                                metadata: CommandMetadata {
                                    metric_type: "adv".to_string(),
                                    keys: sorted_keys.clone(),
                                    add_key: Some(add_value_config.add_key.clone()),
                                },
                            });
                            self.commands.push(StorageCommand {
                                key: summary_key,
                                value: "count".to_string(),
                                command_type: StorageCommandType::HashIncrement,
                                timestamp: self.timestamp,
                                metadata: CommandMetadata {
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

        // top (top:) using sorted sets
        for top_pattern in &config.mapping.top {
            let keys: Vec<String> = top_pattern.split('~').map(String::from).collect();
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
                let member = values.join("~");
                let metric_key = sorted_keys.join("~");
                self.top_metrics
                    .insert(format!("{}:{}", metric_key, member), 1.0);

                for (bucket, bucket_type) in &buckets {
                    self.commands.push(StorageCommand {
                        key: format!("top:{}:{}:{}", bucket_type.as_str(), bucket, metric_key),
                        value: member.clone(),
                        command_type: StorageCommandType::SortedSetIncrement(1.0),
                        timestamp: self.timestamp,
                        metadata: CommandMetadata {
                            metric_type: "top".to_string(),
                            keys: sorted_keys.clone(),
                            add_key: None,
                        },
                    });
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
        println!("Top metrics: {}", self.top_metrics.len());
        println!("Total storage ops: {}", self.commands.len());

        println!("\nStorage commands:");
        for cmd in self.commands.iter() {
            match &cmd.command_type {
                StorageCommandType::HyperLogLog => {
                    println!(
                        "  - HyperLogLog      | key: {} | value: {}",
                        cmd.key, cmd.value
                    );
                }
                StorageCommandType::HashIncrement => {
                    println!("  - HashIncrement    | key: {} | value: 1", cmd.key);
                }
                StorageCommandType::HashIncrementFloat(amount) => {
                    println!(
                        "  - HashIncrFloat    | key: {} | value: {}",
                        cmd.key, amount
                    );
                }
                StorageCommandType::SortedSetIncrement(amount) => {
                    println!(
                        "  - SortedSetIncr    | key: {} | member: {} | by: {}",
                        cmd.key, cmd.value, amount
                    );
                }
            }
        }
    }
}

/// a single metric result from a query
#[derive(Debug, Clone, Serialize)]
pub struct MetricResult {
    /// the metric type: "bmp", "add", "adv", "top"
    pub metric_type: String,
    /// the pattern keys (e.g., ["event", "geo"])
    pub key: Vec<String>,
    /// the full storage key this was read from
    pub location: String,
    /// for addv: the value key (e.g., "amount")
    pub add_key: Option<String>,
    /// the result data, varies by type
    pub result: MetricData,
}

/// the data payload of a metric result, varies by type
#[derive(Debug, Clone, Serialize)]
pub enum MetricData {
    /// bmp: unique count
    Count(u64),
    /// add: field -> count hash
    Hash(HashMap<String, i64>),
    /// adv: field -> float amount hash
    FloatHash(HashMap<String, f64>),
    /// adv summary: sum and count
    Summary { sum: f64, count: i64 },
    /// top: ranked members with scores (descending)
    Ranked(Vec<(String, f64)>),
}

/// results for a single day
#[derive(Debug, Clone, Serialize)]
pub struct DayResult {
    /// ISO date string (YYYY-MM-DD)
    pub date: String,
    /// unix timestamp of the day bucket
    pub timestamp: i64,
    /// all metric results for this day
    pub results: Vec<MetricResult>,
}

/// options for finding specific metrics in query results
pub struct FindOptions {
    /// metric type: "bmp", "add", "adv", "top"
    pub metric_type: String,
    /// pattern key to match (e.g., "event" or "event~geo")
    pub key: String,
    /// for addv: filter by add_key
    pub add_key: Option<String>,
    /// merge results across all days into a single result
    pub merge: bool,
}

/// the complete result of a query, organized by day
pub struct QueryResult {
    pub days: Vec<DayResult>,
}

impl QueryResult {
    /// find metrics matching the given options.
    /// returns per-day results, or a merged single result.
    pub fn find(&self, opts: FindOptions) -> Vec<MetricResult> {
        let target_key: Vec<String> = {
            let mut k: Vec<String> = opts.key.split('~').map(String::from).collect();
            k.sort();
            k
        };

        let mut matches: Vec<MetricResult> = Vec::new();

        for day in &self.days {
            for result in &day.results {
                if result.metric_type != opts.metric_type {
                    continue;
                }

                let mut result_key = result.key.clone();
                result_key.sort();

                if result_key != target_key {
                    continue;
                }

                if opts.metric_type == "adv" {
                    if let Some(ref want_add_key) = opts.add_key {
                        if result.add_key.as_ref() != Some(want_add_key) {
                            continue;
                        }
                    }
                }

                matches.push(result.clone());
            }
        }

        if opts.merge && !matches.is_empty() {
            vec![Self::merge_results(&matches, &opts.metric_type)]
        } else {
            matches
        }
    }

    /// shorthand find: "type/key" or "type/key/add_key"
    pub fn find_str(&self, query: &str) -> Vec<MetricResult> {
        let parts: Vec<&str> = query.split('/').collect();
        if parts.len() < 2 {
            return Vec::new();
        }
        let opts = FindOptions {
            metric_type: parts[0].to_string(),
            key: parts[1].to_string(),
            add_key: parts.get(2).map(|s| s.to_string()),
            merge: false,
        };
        self.find(opts)
    }

    fn merge_results(results: &[MetricResult], metric_type: &str) -> MetricResult {
        let first = &results[0];
        let merged_data = match metric_type {
            "bmp" => {
                let total: u64 = results
                    .iter()
                    .map(|r| {
                        if let MetricData::Count(c) = &r.result {
                            *c
                        } else {
                            0
                        }
                    })
                    .sum();
                MetricData::Count(total)
            }
            "add" => {
                let mut merged: HashMap<String, i64> = HashMap::new();
                for r in results {
                    if let MetricData::Hash(h) = &r.result {
                        for (k, v) in h {
                            *merged.entry(k.clone()).or_insert(0) += v;
                        }
                    }
                }
                MetricData::Hash(merged)
            }
            "adv" => {
                // check if these are summaries or per-label hashes
                if matches!(&first.result, MetricData::Summary { .. }) {
                    let mut total_sum = 0.0f64;
                    let mut total_count = 0i64;
                    for r in results {
                        if let MetricData::Summary { sum, count } = &r.result {
                            total_sum += sum;
                            total_count += count;
                        }
                    }
                    MetricData::Summary {
                        sum: total_sum,
                        count: total_count,
                    }
                } else {
                    let mut merged: HashMap<String, f64> = HashMap::new();
                    for r in results {
                        if let MetricData::FloatHash(h) = &r.result {
                            for (k, v) in h {
                                *merged.entry(k.clone()).or_insert(0.0) += v;
                            }
                        }
                    }
                    MetricData::FloatHash(merged)
                }
            }
            "top" => {
                let mut merged: HashMap<String, f64> = HashMap::new();
                for r in results {
                    if let MetricData::Ranked(pairs) = &r.result {
                        for (k, v) in pairs {
                            *merged.entry(k.clone()).or_insert(0.0) += v;
                        }
                    }
                }
                let mut sorted: Vec<(String, f64)> = merged.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                MetricData::Ranked(sorted)
            }
            _ => first.result.clone(),
        };

        MetricResult {
            metric_type: first.metric_type.clone(),
            key: first.key.clone(),
            location: String::new(),
            add_key: first.add_key.clone(),
            result: merged_data,
        }
    }
}

/// the primary interface for recording and querying metrics.
/// owns a config and storage backend.
pub struct Traqq {
    pub config: TraqqConfig,
    storage: Box<dyn storage::Storage>,
    /// key prefix for all storage keys
    prefix: String,
}

impl Traqq {
    pub fn new(
        config: TraqqConfig,
        storage: Box<dyn storage::Storage>,
        prefix: &str,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            config,
            storage,
            prefix: prefix.to_string(),
        })
    }

    /// record an event: validate, process, and persist to storage.
    /// returns the processed event for inspection if needed.
    pub fn record(&self, event: IncomingEvent) -> Result<ProcessedEvent, String> {
        let processed = ProcessedEvent::from_incoming(event, &self.config)?;
        self.execute_commands(&processed)?;
        Ok(processed)
    }

    /// execute all storage commands from a processed event and track keys
    fn execute_commands(&self, processed: &ProcessedEvent) -> Result<(), String> {
        let mut tracked_keys: Vec<String> = Vec::new();

        for cmd in &processed.commands {
            let prefixed_key = format!("{}:{}", self.prefix, cmd.key);

            match &cmd.command_type {
                StorageCommandType::HyperLogLog => {
                    self.storage
                        .hyperloglog_add(&prefixed_key, &cmd.value)
                        .map_err(|e| e.to_string())?;
                }
                StorageCommandType::HashIncrement => {
                    self.storage
                        .hash_increment(&prefixed_key, &cmd.value, 1)
                        .map_err(|e| e.to_string())?;
                }
                StorageCommandType::HashIncrementFloat(amount) => {
                    self.storage
                        .hash_increment_float(&prefixed_key, &cmd.value, *amount)
                        .map_err(|e| e.to_string())?;
                }
                StorageCommandType::SortedSetIncrement(amount) => {
                    self.storage
                        .sorted_set_increment(&prefixed_key, &cmd.value, *amount)
                        .map_err(|e| e.to_string())?;
                }
            }

            tracked_keys.push(prefixed_key);
        }

        // key tracking: store all keys generated for each time bucket
        // so the query path can discover what to read
        let buckets = self.config.get_time_buckets(processed.timestamp)?;
        for (bucket, bucket_type) in &buckets {
            let keys_key = format!("{}:k:{}:{}", self.prefix, bucket_type.as_str(), bucket);
            if !tracked_keys.is_empty() {
                self.storage
                    .set_add(&keys_key, &tracked_keys)
                    .map_err(|e| e.to_string())?;
            }
        }

        Ok(())
    }

    /// query metrics for a range of timestamps (unix seconds).
    /// returns results organized by day.
    pub fn query(&self, min: i64, max: i64) -> Result<QueryResult, String> {
        // validate timezone early
        let _tz = utils::parse_timezone(&self.config.time.timezone)?;
        let mut days: Vec<DayResult> = Vec::new();

        // walk day-by-day from min to max
        let day_secs: i64 = 86400;
        let mut current = min;

        while current <= max {
            let day_results = self.query_day(current, "d")?;
            days.push(day_results);
            current += day_secs;
        }

        // if hourly is enabled, also query hourly buckets
        // for now we only return daily results in the query output
        // hourly data is stored and can be queried separately

        Ok(QueryResult { days })
    }

    /// convenience: query the last N days
    pub fn query_days(&self, num_days: i32) -> Result<QueryResult, String> {
        let tz = utils::parse_timezone(&self.config.time.timezone)?;
        let now = Utc::now().with_timezone(&tz);
        let today = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| "failed to create timestamp".to_string())?
            .and_local_timezone(tz)
            .earliest()
            .ok_or_else(|| "failed to convert timestamp".to_string())?
            .timestamp();

        let day_secs: i64 = 86400;
        let min = today - (num_days.unsigned_abs() as i64 * day_secs);
        let max = today;

        self.query(min, max)
    }

    /// query all metrics for a single day bucket
    fn query_day(&self, timestamp: i64, bucket_type: &str) -> Result<DayResult, String> {
        let keys_key = format!("{}:k:{}:{}", self.prefix, bucket_type, timestamp);

        let tracked_keys = self
            .storage
            .set_members(&keys_key)
            .map_err(|e| e.to_string())?;

        let date = chrono::DateTime::from_timestamp(timestamp, 0)
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_default();

        let mut results: Vec<MetricResult> = Vec::new();

        // deduplicate keys (same key may appear multiple times from different events)
        let unique_keys: HashSet<String> = tracked_keys.into_iter().collect();

        for key in unique_keys {
            if let Some(result) = self.read_metric_key(&key)? {
                results.push(result);
            }
        }

        Ok(DayResult {
            date,
            timestamp,
            results,
        })
    }

    /// parse a storage key and read the appropriate data.
    /// key format: prefix:type:bucket:timestamp:pattern[:extra]
    fn read_metric_key(&self, key: &str) -> Result<Option<MetricResult>, String> {
        // strip our prefix
        let unprefixed = key
            .strip_prefix(&format!("{}:", self.prefix))
            .unwrap_or(key);

        // parse: type:bucket:timestamp:rest
        let parts: Vec<&str> = unprefixed.splitn(4, ':').collect();
        if parts.len() < 4 {
            return Ok(None);
        }

        let metric_type = parts[0];
        // parts[1] = bucket type (d/h)
        // parts[2] = timestamp
        let rest = parts[3];

        match metric_type {
            "bmp" => {
                // rest = pattern (e.g., "ip")
                let count = self
                    .storage
                    .hyperloglog_count(key)
                    .map_err(|e| e.to_string())?;

                Ok(Some(MetricResult {
                    metric_type: "bmp".to_string(),
                    key: rest.split('~').map(String::from).collect(),
                    location: key.to_string(),
                    add_key: None,
                    result: MetricData::Count(count),
                }))
            }
            "add" => {
                // rest = pattern (e.g., "event" or "event~geo")
                let fields = self.storage.hash_get_all(key).map_err(|e| e.to_string())?;

                if fields.is_empty() {
                    return Ok(None);
                }

                let hash: HashMap<String, i64> = fields
                    .into_iter()
                    .filter_map(|(k, v)| v.parse::<i64>().ok().map(|n| (k, n)))
                    .collect();

                Ok(Some(MetricResult {
                    metric_type: "add".to_string(),
                    key: rest.split('~').map(String::from).collect(),
                    location: key.to_string(),
                    add_key: None,
                    result: MetricData::Hash(hash),
                }))
            }
            "adv" => {
                // rest = add_key:pattern or add_key:pattern:i
                let is_summary = rest.ends_with(":i");
                let clean_rest = if is_summary {
                    &rest[..rest.len() - 2]
                } else {
                    rest
                };

                // parse add_key:pattern
                let adv_parts: Vec<&str> = clean_rest.splitn(2, ':').collect();
                if adv_parts.len() < 2 {
                    return Ok(None);
                }

                let add_key = adv_parts[0];
                let pattern = adv_parts[1];

                let fields = self.storage.hash_get_all(key).map_err(|e| e.to_string())?;

                if fields.is_empty() {
                    return Ok(None);
                }

                if is_summary {
                    let sum: f64 = fields
                        .get("sum")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0.0);
                    let count: i64 = fields
                        .get("count")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);

                    Ok(Some(MetricResult {
                        metric_type: "adv".to_string(),
                        key: pattern.split('~').map(String::from).collect(),
                        location: key.to_string(),
                        add_key: Some(add_key.to_string()),
                        result: MetricData::Summary { sum, count },
                    }))
                } else {
                    let hash: HashMap<String, f64> = fields
                        .into_iter()
                        .filter_map(|(k, v)| v.parse::<f64>().ok().map(|n| (k, n)))
                        .collect();

                    Ok(Some(MetricResult {
                        metric_type: "adv".to_string(),
                        key: pattern.split('~').map(String::from).collect(),
                        location: key.to_string(),
                        add_key: Some(add_key.to_string()),
                        result: MetricData::FloatHash(hash),
                    }))
                }
            }
            "top" => {
                // rest = pattern (e.g., "geo" or "geo~offer")
                let pairs = self
                    .storage
                    .sorted_set_top(key, 250)
                    .map_err(|e| e.to_string())?;

                if pairs.is_empty() {
                    return Ok(None);
                }

                Ok(Some(MetricResult {
                    metric_type: "top".to_string(),
                    key: rest.split('~').map(String::from).collect(),
                    location: key.to_string(),
                    add_key: None,
                    result: MetricData::Ranked(pairs),
                }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
