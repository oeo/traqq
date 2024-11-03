#[cfg(test)]
mod tests {
    use crate::*;

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