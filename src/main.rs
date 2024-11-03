use std::collections::{HashMap, HashSet};
use serde::Deserialize;
use serde_json::json;
use chrono::{DateTime, Utc, Timelike};

use traqq::utils;
use traqq::constants;

#[derive(Debug, Clone)]
pub enum BucketType {
    Daily,
    Hourly,
}

impl BucketType {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Daily => "d",
            Self::Hourly => "h",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeConfig {
    pub store_hourly: bool,
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

#[derive(Debug, Clone, Deserialize)]
pub struct MappingConfig {
    pub bitmap: Vec<String>,
    pub add: Vec<String>,
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

#[derive(Debug, Clone, Deserialize)]
pub struct AddValueConfig {
    pub key: String,
    pub add_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    pub max_field_length: usize,
    pub max_value_length: usize,
    pub max_combinations: usize,
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

#[derive(Debug, Clone, Deserialize)]
pub struct TraqqConfig {
    pub time: TimeConfig,
    pub mapping: MappingConfig,
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

#[derive(Debug, Clone, Deserialize)]
pub struct IncomingEvent {
    pub event: String,

    #[serde(flatten)]
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ProcessedEvent {
    pub event_name: String,
    pub timestamp: DateTime<Utc>,
    pub raw_properties: HashMap<String, String>,
    pub combined_properties: HashMap<String, String>,
    pub numeric_values: HashMap<String, f64>,
    pub string_values: HashMap<String, String>,
    pub boolean_values: HashMap<String, bool>,
    pub bitmap_metrics: Vec<String>,
    pub add_metrics: HashMap<String, i64>,
    pub add_value_metrics: HashMap<String, f64>,
    pub redis_commands: Vec<RedisCommand>,
}

#[derive(Debug, Clone)]
pub struct RedisCommand {
    pub key: String,
    pub value: String,
    pub command_type: RedisCommandType,
    pub timestamp: DateTime<Utc>,
    pub metadata: RedisMetadata,
}

#[derive(Debug, Clone)]
pub enum RedisCommandType {
    Bitmap,
    HyperLogLog,
    Increment,
    IncrementBy,
}

#[derive(Debug, Clone)]
pub struct RedisMetadata {
    pub metric_type: String,
    pub keys: Vec<String>,
    pub add_key: Option<String>,
}

impl TraqqConfig {
    pub fn default() -> Self {
        Self {
            time: TimeConfig::default(),
            mapping: MappingConfig::default(),
            limits: LimitsConfig::default(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        utils::parse_timezone(&self.time.timezone)?;

        let mut unique_patterns = HashSet::new();
        
        for pattern in &self.mapping.bitmap {
            if !unique_patterns.insert(pattern) {
                return Err(format!("duplicate pattern: {}", pattern));
            }
        }

        for pattern in &self.mapping.add {
            if !unique_patterns.insert(pattern) {
                return Err(format!("duplicate add pattern: {}", pattern));
            }
            utils::validate_mapping_pattern(pattern)?;
        }

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
                        command_type: RedisCommandType::IncrementBy,
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
                                command_type: RedisCommandType::IncrementBy,
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
        // Only print essential metrics
        println!("\nMetrics Summary:");
        println!("---------------");
        println!("Bitmap metrics: {}", self.bitmap_metrics.len());
        println!("Add metrics: {}", self.add_metrics.len());
        println!("Add value metrics: {}", self.add_value_metrics.len());
        println!("Total Redis ops: {}", self.redis_commands.len());

        // Only show first 5 Redis commands as a sample
        println!("\nSample Redis commands:");
        for cmd in self.redis_commands.iter().take(5) {
            match cmd.command_type {
                RedisCommandType::HyperLogLog => {
                    println!("  - HyperLogLog | key: {} | value: {}", cmd.key, cmd.value);
                }
                RedisCommandType::IncrementBy => {
                    println!("  - IncrementBy | key: {} | value: {}", cmd.key, cmd.value);
                }
                _ => println!("  - {:?} | key: {} | value: {}", cmd.command_type, cmd.key, cmd.value),
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
    use tokio::time::*;
    use rand::Rng;
    use rand::thread_rng;
    use rand::prelude::SliceRandom;
    use rand::distributions::Alphanumeric;

    const SAMPLE_USER_AGENTS: &[&str] = &[
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36", 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.62 Mobile Safari/537.36"
    ];

    const SAMPLE_UTM_SOURCES: &[&str] = &[
        "google",
        "facebook", 
        "twitter",
        "linkedin",
        "email",
        "direct"
    ];

    const SAMPLE_UTM_MEDIUMS: &[&str] = &[
        "cpc",
        "organic",
        "social", 
        "email",
        "referral",
        "none"
    ];

    const SAMPLE_UTM_CAMPAIGNS: &[&str] = &[
        "spring_sale",
        "product_launch",
        "brand_awareness",
        "retargeting",
        "newsletter"
    ];

    #[derive(Debug)]
    struct BenchmarkResult {
        events_processed: usize,
        total_duration: Duration,
        avg_duration: Duration,
        events_per_second: f64,
        redis_commands_generated: usize,
        memory_usage: usize,
        sample_commands: Option<Vec<RedisCommand>>,
    }

    fn random_string(len: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

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
            "event name cannot be empty".to_string(),
            "validation should return correct error message"
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
        let input = "test~event:name";
        let result = utils::sanitize_value(input, 100).unwrap();
        assert_eq!(result.as_deref(), Some("test_event_name"));
        
        let input = "purchase";
        let result = utils::sanitize_value(input, 100).unwrap();
        assert_eq!(result.as_deref(), Some("purchase"));
        
        let input = "test_event";
        let result = utils::sanitize_value(input, 100).unwrap();
        assert_eq!(result.as_deref(), Some("test_event"));
    }

    #[test]
    fn test_value_sanitization() {
        let config = TraqqConfig {
            time: TimeConfig {
                timezone: "UTC".to_string(),
                ..Default::default()
            },
            mapping: MappingConfig {
                bitmap: vec!["ip".to_string(), "custom_field".to_string()],
                add: vec!["event".to_string()],
                add_value: vec![],
            },
            limits: LimitsConfig::default(),
        };

        let mut event = IncomingEvent {
            event: "  test~event  ".to_string(),
            properties: serde_json::json!({
                "ip": "127.0.0.1",
                "custom_field": "  value~with:special~chars  "
            }),
        };

        event.validate_and_sanitize(&config).unwrap();
        assert_eq!(event.event, "test_event", "event name should be sanitized");

        if let serde_json::Value::Object(props) = &event.properties {
            assert_eq!(
                props.get("ip").unwrap().as_str().unwrap(),
                "127.0.0.1",
                "IP address should not be sanitized"
            );
            assert_eq!(
                props.get("custom_field").unwrap().as_str().unwrap(),
                "value_with_special_chars",
                "custom field should be sanitized"
            );
        } else {
            panic!("properties should be an object");
        }
    }

    #[test]
    fn test_sanitize_value_basic() {
        assert_eq!(
            utils::sanitize_value("test~value:here", 100).unwrap(),
            Some("test_value_here".to_string())
        );
        assert_eq!(
            utils::sanitize_value("  spaced  ", 100).unwrap(),
            Some("spaced".to_string())
        );
        assert_eq!(utils::sanitize_value("", 100).unwrap(), None);
        assert_eq!(utils::sanitize_value("   ", 100).unwrap(), None);
        assert_eq!(utils::sanitize_value("test_event", 100).unwrap(), Some("test_event".to_string()));
        assert_eq!(
            utils::sanitize_value("1234567890extra", 10).unwrap(),
            Some("1234567890".to_string())
        );
    }

    #[test]
    fn test_sanitize_value_basic_cases() {
        let test_cases = vec![
            ("purchase", Some("purchase")),
            ("test~event", Some("test_event")),
            ("test:event", Some("test_event")),
            ("  spaced  ", Some("spaced")),
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

    fn create_test_config(complexity: usize) -> TraqqConfig {
        TraqqConfig {
            time: TimeConfig {
                store_hourly: false,
                timezone: "UTC".to_string(),
            },
            mapping: MappingConfig {
                bitmap: vec![
                    "ip".to_string(),
                ],
                add: {
                    let mut patterns = vec![
                        "event".to_string(),
                        "event~utm_source".to_string(),
                        "event~utm_medium".to_string(),
                        "event~utm_campaign".to_string(),
                    ];
                    
                    // Add more complex patterns based on complexity level
                    if complexity > 1 {
                        patterns.push("event~utm_source~utm_medium".to_string());
                    }
                    if complexity > 2 {
                        patterns.push("event~utm_source~utm_medium~utm_campaign".to_string());
                    }
                    if complexity > 3 {
                        patterns.push("event~browser~device".to_string());
                    }
                    if complexity > 4 {
                        patterns.push("event~browser~device~utm_source".to_string());
                    }
                    if complexity > 5 {
                        patterns.push("event~browser~device~utm_source~utm_medium".to_string());
                    }
                    // Add more complex combinations for higher complexity levels
                    for i in 6..complexity {
                        patterns.push(format!("event~utm_source~utm_medium~utm_campaign~{}", i));
                    }
                    
                    patterns
                },
                add_value: {
                    let mut value_patterns = vec![
                        AddValueConfig {
                            key: "event".to_string(),
                            add_key: "amount".to_string(),
                        },
                        AddValueConfig {
                            key: "event~utm_source".to_string(),
                            add_key: "amount".to_string(),
                        },
                    ];
                    
                    // Add more value patterns based on complexity
                    if complexity > 1 {
                        value_patterns.push(AddValueConfig {
                            key: "event~utm_source~utm_medium".to_string(),
                            add_key: "amount".to_string(),
                        });
                    }
                    if complexity > 2 {
                        value_patterns.push(AddValueConfig {
                            key: "event~utm_source~utm_medium~utm_campaign".to_string(),
                            add_key: "amount".to_string(),
                        });
                    }
                    // Add more value patterns for higher complexity levels
                    for i in 3..complexity {
                        value_patterns.push(AddValueConfig {
                            key: format!("event~utm_source~utm_medium~{}", i),
                            add_key: "amount".to_string(),
                        });
                    }
                    
                    value_patterns
                },
            },
            limits: LimitsConfig::default(),
        }
    }

    fn run_benchmark(event_complexity: usize, num_events: usize) -> BenchmarkResult {
        let config = create_test_config(event_complexity);
        let mut total_duration = Duration::new(0, 0);
        let mut total_commands = 0;
        let mut total_memory = 0;
        let mut sample_commands = None;

        let start = Instant::now();
        
        for i in 0..num_events {
            let json_event = utils::create_test_event();
            let event = IncomingEvent::from_json(json_event).unwrap();
            
            let event_start = Instant::now();
            match ProcessedEvent::from_incoming(event, &config) {
                Ok(processed) => {
                    total_commands += processed.redis_commands.len();
                    if i == 0 {
                        sample_commands = Some(processed.redis_commands.clone());
                    }
                    total_memory += std::mem::size_of_val(&processed) +
                        processed.redis_commands.len() * std::mem::size_of::<RedisCommand>() +
                        processed.combined_properties.len() * 64;
                }
                Err(e) => panic!("Failed to process event: {}", e),
            }
            total_duration += event_start.elapsed();
        }

        let total_elapsed = start.elapsed();
        let events_per_second = num_events as f64 / total_elapsed.as_secs_f64();

        println!("\nComplexity level: {}", event_complexity);
        println!("Events processed: {}", num_events);
        println!("Total duration: {:?}", total_elapsed);
        println!("Avg duration/event: {:?}", total_duration / num_events as u32);
        println!("Events/sec: {:.2}", events_per_second);
        println!("Total Redis commands: {}", total_commands);
        println!("Avg Redis commands/event: {:.2}", total_commands as f64 / num_events as f64);
        println!("Approximate memory usage: {:.2} mb", total_memory as f64 / 1024.0 / 1024.0);

        if let Some(commands) = &sample_commands {
            println!("\nSample Redis commands (first 5):");
            for cmd in commands.iter().take(5) {
                match cmd.command_type {
                    RedisCommandType::HyperLogLog => {
                        println!("  - HyperLogLog | key: {} | value: {}", cmd.key, cmd.value);
                    }
                    RedisCommandType::IncrementBy => {
                        println!("  - IncrementBy | key: {} | value: {}", cmd.key, cmd.value);
                    }
                    _ => println!("  - {:?} | key: {} | value: {}", cmd.command_type, cmd.key, cmd.value),
                }
            }
        }

        BenchmarkResult {
            events_processed: num_events,
            total_duration,
            avg_duration: total_duration / num_events as u32,
            events_per_second,
            redis_commands_generated: total_commands,
            memory_usage: total_memory,
            sample_commands,
        }
    }

    #[test]
    fn test_performance_scaling() {
        let complexities = [1, 2, 3, 5, 10, 20];
        let events_per_test = 5000;

        println!("\nperformance scaling test results:");
        println!("================================");
        
        for &complexity in &complexities {
            let result = run_benchmark(complexity, events_per_test);
            
            println!("\ncomplexity level: {}", complexity);
            println!("events processed: {}", result.events_processed);
            println!("total duration: {:?}", result.total_duration);
            println!("avg duration/event: {:?}", result.avg_duration);
            println!("events/sec: {:.2}", result.events_per_second);
            println!("total redis commands: {}", result.redis_commands_generated);
            println!("avg redis commands/event: {:.2}", 
                result.redis_commands_generated as f64 / events_per_test as f64);
            println!("approximate memory usage: {:.2} mb", 
                result.memory_usage as f64 / (1024.0 * 1024.0));

            if let Some(sample_commands) = &result.sample_commands {
                println!("\nsample redis commands (first 10):");
                for cmd in sample_commands.iter().take(10) {
                    match cmd.command_type {
                        RedisCommandType::HyperLogLog => {
                            println!("  - HyperLogLog | key: {} | value: {}", 
                                cmd.key,
                                cmd.value);
                        },
                        RedisCommandType::IncrementBy => {
                            println!("  - IncrementBy | key: {} | value: {}", 
                                cmd.key,
                                cmd.value);
                        },
                        _ => {
                            println!("  - {:?} | key: {} | value: {}", 
                                cmd.command_type,
                                cmd.key,
                                cmd.value);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_concurrent_processing() {
        use std::thread;
        use std::sync::Arc;

        let config = Arc::new(create_test_config(5));
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let events_per_thread = 150;
        
        let start = Instant::now();
        let mut handles = vec![];
        
        println!("\nrunning concurrent test with {} threads (auto-detected)", num_threads);
        
        for _ in 0..num_threads {
            let config = Arc::clone(&config);
            
            handles.push(thread::spawn(move || {
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
        let events_per_second = total_events as f64 / total_duration.as_secs_f64();
        
        println!("\nconcurrent processing test results:");
        println!("==================================");
        println!("threads: {}", num_threads);
        println!("events/thread: {}", events_per_thread);
        println!("total events: {}", total_events);
        println!("total duration: {:?}", total_duration);
        println!("events/sec: {:.2}", events_per_second);
        println!("total redis commands: {}", total_commands);
        println!("avg redis commands/event: {:.2}", 
            total_commands as f64 / total_events as f64);
    }

    #[test]
    fn test_event_validation() {
        let config = TraqqConfig {
            time: TimeConfig {
                timezone: "UTC".to_string(),
                ..Default::default()
            },
            mapping: MappingConfig {
                bitmap: vec![
                    "ip".to_string()
                ],
                add: vec![
                    "event".to_string(),
                    "event~source".to_string()
                ],
                add_value: vec![AddValueConfig {
                    key: "event~source".to_string(),
                    add_key: "amount".to_string(),
                }],
            },
            limits: LimitsConfig::default(),
        };

        let mut event = IncomingEvent {
            event: "test_event".to_string(),
            properties: serde_json::json!({
                "ip": "127.0.0.1",
                "event": "test_event",
                "source": "google",
                "amount": 99.99,

                // properties that should be filtered out
                // because they are not in the mapping config
                "unused": "should_be_removed",
                "random": "also_removed"
            }),
        };

        if let Err(e) = event.validate_and_sanitize(&config) {
            panic!("event validation failed: {}", e);
        }

        if let serde_json::Value::Object(props) = &event.properties {

            // check that needed properties are kept
            for key in ["ip", "event", "source", "amount"] {
                assert!(props.contains_key(key), "{} should be kept", key);
            }

            // check that unused properties are removed
            for key in ["unused", "random"] {
                assert!(!props.contains_key(key), "{} should be removed", key);
            }
        }
    }

    #[test]
    fn test_complex_event_processing() {
        let config = TraqqConfig {
            time: TimeConfig {
                store_hourly: true,
                timezone: "UTC".to_string(),
                ..Default::default()
            },
            mapping: MappingConfig {
                bitmap: vec![
                    "ip".to_string()
                ],
                add: vec![
                    "event".to_string(),
                    "event~campaign".to_string()
                ],
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
        
        // should generate:
        // - 2 bitmap commands (daily, hourly) for ip 
        // - 4 add commands (2 patterns * 2 time buckets)
        // - 2 add_value commands (1 pattern * 2 time buckets)
        assert_eq!(processed.redis_commands.len(), 8, 
            "redis command count mismatch - expected 8:\n{:#?}", processed.redis_commands);
    }

    fn create_realistic_event() -> IncomingEvent {
        let mut rng = thread_rng();
        
        let mut props = serde_json::Map::new();
        
        // Core event data
        props.insert("event".to_string(), json!("conversion"));
        props.insert("offer_id".to_string(), json!(format!("OFFER_{}", rng.gen_range(1000..9999))));
        props.insert("amount".to_string(), json!(rng.gen_range(10.0..500.0)));
        
        // User data
        props.insert("user_agent".to_string(), json!(SAMPLE_USER_AGENTS.choose(&mut rng).unwrap()));
        props.insert("ip".to_string(), json!(format!("{}.{}.{}.{}", 
            rng.gen_range(1..255),
            rng.gen_range(1..255),
            rng.gen_range(1..255),
            rng.gen_range(1..255)
        )));
        
        // UTM parameters
        props.insert("utm_source".to_string(), json!(SAMPLE_UTM_SOURCES.choose(&mut rng).unwrap()));
        props.insert("utm_medium".to_string(), json!(SAMPLE_UTM_MEDIUMS.choose(&mut rng).unwrap()));
        props.insert("utm_campaign".to_string(), json!(SAMPLE_UTM_CAMPAIGNS.choose(&mut rng).unwrap()));
        
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

    // Helper to create a realistic config
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

    #[test]
    fn test_realistic_event_processing() {
        let config = create_realistic_config();
        let event = create_realistic_event();
        
        let start = Instant::now();
        let processed = ProcessedEvent::from_incoming(event, &config).unwrap();
        let duration = start.elapsed();

        println!("\nrealistic event benchmark:");
        println!("========================");
        println!("processing latency: {:?}", duration);
        println!("\nmetrics generated:");
        println!("bitmap metrics: {}", processed.bitmap_metrics.len());
        println!("add metrics: {}", processed.add_metrics.len());
        println!("add value metrics: {}", processed.add_value_metrics.len());
        println!("total redis ops: {}", processed.redis_commands.len());
        
        // Add specific assertions here
        assert!(processed.bitmap_metrics.len() > 0);
        assert!(processed.add_metrics.len() > 0);
        assert!(processed.add_value_metrics.len() > 0);
    }

    #[test]
    fn test_partial_event_processing() {
        println!("\ndebug partial event processing:");
        
        let config = TraqqConfig {
            time: TimeConfig {
                store_hourly: true,
                timezone: "UTC".to_string(),
            },
            mapping: MappingConfig {
                bitmap: vec!["ip".to_string()],
                add: vec!["event".to_string()],
                add_value: vec![],
            },
            limits: LimitsConfig::default(),
        };

        let event = IncomingEvent {
            event: "purchase".to_string(),
            properties: serde_json::json!({
                "ip": "127.0.0.1",
                "amount": "49.99"
            }),
        };

        let processed = ProcessedEvent::from_incoming(event, &config).unwrap();
        
        println!("raw properties: {}", serde_json::to_string(&processed.raw_properties).unwrap());
        println!("bitmap metrics: {:?}", processed.bitmap_metrics);
        println!("add metrics: {:?}", processed.add_metrics);
        println!("add value metrics: {:?}", processed.add_value_metrics);
        println!("redis commands: {:?}", processed.redis_commands);

        // Count HyperLogLog operations
        let bitmap_ops = processed.redis_commands.iter()
            .filter(|cmd| matches!(cmd.command_type, RedisCommandType::HyperLogLog))
            .count();

        assert_eq!(bitmap_ops, 2, "expected 2 bitmap ops (daily/hourly)");

        // Also verify the commands are correct
        let hyperloglog_commands: Vec<_> = processed.redis_commands.iter()
            .filter(|cmd| matches!(cmd.command_type, RedisCommandType::HyperLogLog))
            .collect();

        // Verify the first HyperLogLog command (daily)
        assert_eq!(hyperloglog_commands[0].key, "bmp:d:1730592000:ip");
        assert_eq!(hyperloglog_commands[0].value, "127.0.0.1");

        // Verify the second HyperLogLog command (hourly)
        assert_eq!(hyperloglog_commands[1].key, "bmp:h:1730613600:ip");
        assert_eq!(hyperloglog_commands[1].value, "127.0.0.1");
    }
}
