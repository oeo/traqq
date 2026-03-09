#[cfg(test)]
mod tests {
    use crate::*;

    use rand::{distributions::Alphanumeric, prelude::SliceRandom, thread_rng, Rng};
    use tokio::time::*;

    #[derive(Debug)]
    struct BenchmarkResult {
        events_processed: usize,
        total_duration: Duration,
        avg_duration: Duration,
        events_per_second: f64,
        commands_generated: usize,
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
                top: vec![],
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
        props.insert(
            "offer_id".to_string(),
            json!(format!("OFFER_{}", rng.gen_range(1000..9999))),
        );
        props.insert("amount".to_string(), json!(rng.gen_range(10.0..500.0)));

        // User data
        props.insert(
            "user_agent".to_string(),
            json!(utils::SAMPLE_USER_AGENTS.choose(&mut rng).unwrap()),
        );
        props.insert(
            "ip".to_string(),
            json!(format!(
                "{}.{}.{}.{}",
                rng.gen_range(1..255),
                rng.gen_range(1..255),
                rng.gen_range(1..255),
                rng.gen_range(1..255)
            )),
        );

        // UTM parameters
        props.insert(
            "utm_source".to_string(),
            json!(utils::SAMPLE_UTM_SOURCES.choose(&mut rng).unwrap()),
        );
        props.insert(
            "utm_medium".to_string(),
            json!(utils::SAMPLE_UTM_MEDIUMS.choose(&mut rng).unwrap()),
        );
        props.insert(
            "utm_campaign".to_string(),
            json!(utils::SAMPLE_UTM_CAMPAIGNS.choose(&mut rng).unwrap()),
        );

        // Additional metadata
        props.insert("browser".to_string(), json!("chrome"));
        props.insert("device".to_string(), json!("desktop"));
        props.insert("country".to_string(), json!("US"));
        props.insert("language".to_string(), json!("en-US"));
        props.insert(
            "page_url".to_string(),
            json!("https://example.com/checkout"),
        );
        props.insert("referrer".to_string(), json!("https://google.com"));

        // Random noise that should be filtered
        props.insert("session_id".to_string(), json!(random_string(32)));
        props.insert(
            "timestamp_ms".to_string(),
            json!(Utc::now().timestamp_millis()),
        );
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
                top: vec![],
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
                    total_commands += processed.commands.len();
                    total_memory += std::mem::size_of_val(&processed)
                        + processed.commands.len() * std::mem::size_of::<StorageCommand>()
                        + processed.combined_properties.len() * 64;
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
            commands_generated: total_commands,
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
            assert!(
                result.is_err(),
                "should fail with whitespace-only event name"
            );
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
                assert_eq!(
                    result.as_deref(),
                    expected,
                    "sanitization failed for '{}' - expected '{:?}', got '{:?}'",
                    input,
                    expected,
                    result
                );
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
                println!("Total Commands: {}", result.commands_generated);
                println!(
                    "Avg Commands/Event: {:.2}",
                    result.commands_generated as f64 / events_per_test as f64
                );
                println!(
                    "Approximate Memory Usage: {:.2} MB",
                    result.memory_usage as f64 / (1024.0 * 1024.0)
                );
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
                            commands += processed.commands.len();
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
            println!(
                "Events/sec: {:.2}",
                total_events as f64 / total_duration.as_secs_f64()
            );
            println!("Total Commands: {}", total_commands);
            println!(
                "Avg Commands/Event: {:.2}",
                total_commands as f64 / total_events as f64
            );
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
            println!("Total Ops: {}", processed.commands.len());

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
                    top: vec![],
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

            assert_eq!(
                processed.commands.len(),
                12,
                "expected 12 commands (2 bmp + 4 add + 6 adv [2 per-label + 2 sum + 2 count])"
            );

            let hyperloglog_commands: Vec<_> = processed
                .commands
                .iter()
                .filter(|cmd| matches!(cmd.command_type, StorageCommandType::HyperLogLog))
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

    mod traqq_service_tests {
        use super::*;
        use crate::storage::memory::MemoryStorage;

        fn make_traqq(config: TraqqConfig) -> Traqq {
            Traqq::new(config, Box::new(MemoryStorage::new()), "test").unwrap()
        }

        #[test]
        fn test_record_persists_to_storage() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string()],
                    add_value: vec![],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            let event = IncomingEvent {
                event: "purchase".to_string(),
                properties: serde_json::json!({
                    "ip": "1.2.3.4",
                }),
            };

            let processed = t.record(event).unwrap();

            // verify bmp was written
            let bmp_cmd = processed
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "bmp")
                .unwrap();
            let bmp_key = format!("test:{}", bmp_cmd.key);
            let count = t.storage.hyperloglog_count(&bmp_key).unwrap();
            assert_eq!(count, 1, "hyperloglog should have 1 unique value");

            // verify add was written
            let add_cmd = processed
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "add")
                .unwrap();
            let add_key = format!("test:{}", add_cmd.key);
            let fields = t.storage.hash_get_all(&add_key).unwrap();
            assert_eq!(fields.get(&add_cmd.value).unwrap(), "1");
        }

        #[test]
        fn test_record_increments_on_duplicate() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string()],
                    add_value: vec![],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            // record same event 3 times
            for _ in 0..3 {
                let event = IncomingEvent {
                    event: "click".to_string(),
                    properties: serde_json::json!({ "ip": "5.6.7.8" }),
                };
                t.record(event).unwrap();
            }

            // bmp should still be 1 (same ip)
            let processed = ProcessedEvent::from_incoming(
                IncomingEvent {
                    event: "click".to_string(),
                    properties: serde_json::json!({ "ip": "5.6.7.8" }),
                },
                &t.config,
            )
            .unwrap();

            let bmp_cmd = processed
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "bmp")
                .unwrap();
            let bmp_key = format!("test:{}", bmp_cmd.key);
            let count = t.storage.hyperloglog_count(&bmp_key).unwrap();
            assert_eq!(count, 1, "same ip should not increase unique count");

            // add counter should be 3
            let add_cmd = processed
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "add")
                .unwrap();
            let add_key = format!("test:{}", add_cmd.key);
            let fields = t.storage.hash_get_all(&add_key).unwrap();
            assert_eq!(fields.get(&add_cmd.value).unwrap(), "3");
        }

        #[test]
        fn test_record_addv_accumulates_amounts() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec![],
                    add: vec![],
                    add_value: vec![AddValueConfig {
                        key: "event".to_string(),
                        add_key: "amount".to_string(),
                    }],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({ "amount": 50.0 }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({ "amount": 25.5 }),
            })
            .unwrap();

            // find the adv key by processing a dummy event to get the key format
            let dummy = ProcessedEvent::from_incoming(
                IncomingEvent {
                    event: "sale".to_string(),
                    properties: serde_json::json!({ "amount": 1.0 }),
                },
                &t.config,
            )
            .unwrap();

            let adv_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "adv")
                .unwrap();
            let adv_key = format!("test:{}", adv_cmd.key);
            let fields = t.storage.hash_get_all(&adv_key).unwrap();

            // the value field stores the compound value "sale"
            // total should be 50.0 + 25.5 = 75.5
            let total: f64 = fields
                .values()
                .map(|v| v.parse::<f64>().unwrap_or(0.0))
                .sum();
            assert!(
                (total - 75.5).abs() < 0.01,
                "addv should accumulate to 75.5, got {}",
                total
            );
        }

        #[test]
        fn test_key_tracking() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string()],
                    add_value: vec![],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            let event = IncomingEvent {
                event: "view".to_string(),
                properties: serde_json::json!({ "ip": "10.0.0.1" }),
            };

            let processed = t.record(event).unwrap();

            // find the daily key tracking set
            let buckets = t.config.get_time_buckets(processed.timestamp).unwrap();
            let (daily_ts, daily_bt) = &buckets[0];
            let keys_key = format!("test:k:{}:{}", daily_bt.as_str(), daily_ts);

            let tracked = t.storage.set_members(&keys_key).unwrap();
            assert!(!tracked.is_empty(), "key tracking set should not be empty");

            // every command key should be tracked
            for cmd in &processed.commands {
                let prefixed = format!("test:{}", cmd.key);
                assert!(
                    tracked.contains(&prefixed),
                    "tracked keys should contain {}",
                    prefixed
                );
            }
        }

        #[test]
        fn test_invalid_config_rejected() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "Not/A/Timezone".to_string(),
                },
                mapping: MappingConfig::default(),
                limits: LimitsConfig::default(),
            };

            let result = Traqq::new(config, Box::new(MemoryStorage::new()), "test");
            assert!(result.is_err());
        }

        #[test]
        fn test_top_metric_persists_to_sorted_set() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec![],
                    add: vec![],
                    add_value: vec![],
                    top: vec!["geo".to_string(), "offer".to_string()],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            for _ in 0..3 {
                t.record(IncomingEvent {
                    event: "click".to_string(),
                    properties: serde_json::json!({
                        "geo": "US",
                        "offer": "offer_a",
                    }),
                })
                .unwrap();
            }

            t.record(IncomingEvent {
                event: "click".to_string(),
                properties: serde_json::json!({
                    "geo": "UK",
                    "offer": "offer_b",
                }),
            })
            .unwrap();

            // find the top:geo key by processing a dummy to get the key format
            let dummy = ProcessedEvent::from_incoming(
                IncomingEvent {
                    event: "click".to_string(),
                    properties: serde_json::json!({ "geo": "US", "offer": "x" }),
                },
                &t.config,
            )
            .unwrap();

            let geo_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "top" && c.metadata.keys == vec!["geo"])
                .unwrap();
            let geo_key = format!("test:{}", geo_cmd.key);

            let top_geos = t.storage.sorted_set_top(&geo_key, 10).unwrap();
            assert_eq!(top_geos.len(), 2);
            assert_eq!(top_geos[0].0, "US");
            assert!((top_geos[0].1 - 3.0).abs() < f64::EPSILON);
            assert_eq!(top_geos[1].0, "UK");
            assert!((top_geos[1].1 - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_addv_summary_tracking() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec![],
                    add: vec![],
                    add_value: vec![AddValueConfig {
                        key: "event".to_string(),
                        add_key: "amount".to_string(),
                    }],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            t.record(IncomingEvent {
                event: "purchase".to_string(),
                properties: serde_json::json!({ "amount": 100.0 }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "purchase".to_string(),
                properties: serde_json::json!({ "amount": 50.0 }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "refund".to_string(),
                properties: serde_json::json!({ "amount": 25.0 }),
            })
            .unwrap();

            // find the summary key (:i suffix)
            let dummy = ProcessedEvent::from_incoming(
                IncomingEvent {
                    event: "purchase".to_string(),
                    properties: serde_json::json!({ "amount": 1.0 }),
                },
                &t.config,
            )
            .unwrap();

            // get the summary command (has :i in key)
            let summary_cmd = dummy
                .commands
                .iter()
                .find(|c| c.key.ends_with(":i") && c.value == "sum")
                .unwrap();
            let summary_key = format!("test:{}", summary_cmd.key);
            let summary = t.storage.hash_get_all(&summary_key).unwrap();

            // sum = 100 + 50 + 25 = 175, count = 3
            let sum: f64 = summary.get("sum").unwrap().parse().unwrap();
            let count: i64 = summary.get("count").unwrap().parse().unwrap();
            assert!(
                (sum - 175.0).abs() < 0.01,
                "sum should be 175.0, got {}",
                sum
            );
            assert_eq!(count, 3, "count should be 3, got {}", count);

            // per-label hash: purchase=150, refund=25
            let label_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "adv" && !c.key.ends_with(":i"))
                .unwrap();
            let label_key = format!("test:{}", label_cmd.key);
            let labels = t.storage.hash_get_all(&label_key).unwrap();

            let purchase_amt: f64 = labels.get("purchase").unwrap().parse().unwrap();
            let refund_amt: f64 = labels.get("refund").unwrap().parse().unwrap();
            assert!((purchase_amt - 150.0).abs() < 0.01);
            assert!((refund_amt - 25.0).abs() < 0.01);
        }

        #[test]
        fn test_compound_key_auto_generation() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec![],
                    add: vec!["event~offer".to_string()],
                    add_value: vec![],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            // event~offer pattern requires both fields, but we rely on
            // auto-generation to create the compound property
            let event = IncomingEvent {
                event: "click".to_string(),
                properties: serde_json::json!({
                    "offer": "offer_123",
                }),
            };

            let processed = ProcessedEvent::from_incoming(event, &config).unwrap();

            // compound key should have been auto-generated
            assert!(
                processed.raw_properties.contains_key("event~offer"),
                "should auto-generate event~offer compound key"
            );

            let compound_val = processed.raw_properties.get("event~offer").unwrap();
            assert_eq!(compound_val, "click~offer_123");

            // add command should have been generated for the compound pattern
            let add_cmds: Vec<_> = processed
                .commands
                .iter()
                .filter(|c| c.metadata.metric_type == "add")
                .collect();
            assert!(
                !add_cmds.is_empty(),
                "should generate add commands for compound pattern"
            );

            // the hash field should be the compound value
            assert_eq!(add_cmds[0].value, "click~offer_123");
        }

        #[test]
        fn test_compound_keys_generated_from_multiple_properties() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec![],
                    add: vec![
                        "event~geo".to_string(),
                        "event~offer".to_string(),
                        "geo~offer".to_string(),
                    ],
                    add_value: vec![],
                    top: vec![],
                },
                limits: LimitsConfig::default(),
            };

            let event = IncomingEvent {
                event: "click".to_string(),
                properties: serde_json::json!({
                    "geo": "US",
                    "offer": "offer_123",
                }),
            };

            let processed = ProcessedEvent::from_incoming(event, &config).unwrap();

            // auto-generated compounds from all pairs of required properties
            assert!(processed.raw_properties.contains_key("event~geo"));
            assert!(processed.raw_properties.contains_key("event~offer"));
            assert!(processed.raw_properties.contains_key("geo~offer"));

            // verify event~geo produced add commands with correct compound value
            let add_cmds: Vec<_> = processed
                .commands
                .iter()
                .filter(|c| {
                    c.metadata.metric_type == "add" && c.metadata.keys == vec!["event", "geo"]
                })
                .collect();
            assert!(!add_cmds.is_empty());
            assert_eq!(add_cmds[0].value, "click~US");
        }

        #[test]
        fn test_full_round_trip_all_metric_types() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string(), "event~geo".to_string()],
                    add_value: vec![AddValueConfig {
                        key: "event~ip".to_string(),
                        add_key: "amount".to_string(),
                    }],
                    top: vec!["geo".to_string()],
                },
                limits: LimitsConfig::default(),
            };

            let t = make_traqq(config);

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({
                    "ip": "10.0.0.1",
                    "geo": "US",
                    "amount": 99.99,
                }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({
                    "ip": "10.0.0.2",
                    "geo": "US",
                    "amount": 50.0,
                }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({
                    "ip": "10.0.0.1",
                    "geo": "UK",
                    "amount": 25.0,
                }),
            })
            .unwrap();

            // verify via a dummy event to discover keys
            let dummy = ProcessedEvent::from_incoming(
                IncomingEvent {
                    event: "sale".to_string(),
                    properties: serde_json::json!({
                        "ip": "x", "geo": "x", "amount": 1.0,
                    }),
                },
                &t.config,
            )
            .unwrap();

            // bmp: 2 unique IPs
            let bmp_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "bmp")
                .unwrap();
            let bmp_count = t
                .storage
                .hyperloglog_count(&format!("test:{}", bmp_cmd.key))
                .unwrap();
            assert_eq!(bmp_count, 2);

            // add event: 3 total
            let add_event_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "add" && c.metadata.keys == vec!["event"])
                .unwrap();
            let add_fields = t
                .storage
                .hash_get_all(&format!("test:{}", add_event_cmd.key))
                .unwrap();
            assert_eq!(add_fields.get("sale").unwrap(), "3");

            // top geo: US=2, UK=1
            let top_cmd = dummy
                .commands
                .iter()
                .find(|c| c.metadata.metric_type == "top")
                .unwrap();
            let top = t
                .storage
                .sorted_set_top(&format!("test:{}", top_cmd.key), 10)
                .unwrap();
            assert_eq!(top[0].0, "US");
            assert!((top[0].1 - 2.0).abs() < f64::EPSILON);
            assert_eq!(top[1].0, "UK");
            assert!((top[1].1 - 1.0).abs() < f64::EPSILON);

            // addv summary: sum=174.99, count=3
            let summary_cmd = dummy
                .commands
                .iter()
                .find(|c| c.key.ends_with(":i") && c.value == "sum")
                .unwrap();
            let summary = t
                .storage
                .hash_get_all(&format!("test:{}", summary_cmd.key))
                .unwrap();
            let sum: f64 = summary.get("sum").unwrap().parse().unwrap();
            let count: i64 = summary.get("count").unwrap().parse().unwrap();
            assert!((sum - 174.99).abs() < 0.01);
            assert_eq!(count, 3);
        }
    }

    mod query_tests {
        use super::*;
        use crate::storage::memory::MemoryStorage;

        fn make_traqq(config: TraqqConfig) -> Traqq {
            Traqq::new(config, Box::new(MemoryStorage::new()), "q").unwrap()
        }

        fn test_config() -> TraqqConfig {
            TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string()],
                    add_value: vec![AddValueConfig {
                        key: "event~geo".to_string(),
                        add_key: "amount".to_string(),
                    }],
                    top: vec!["geo".to_string()],
                },
                limits: LimitsConfig::default(),
            }
        }

        fn record_test_events(t: &Traqq) {
            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({
                    "ip": "1.1.1.1",
                    "geo": "US",
                    "amount": 100.0,
                }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "sale".to_string(),
                properties: serde_json::json!({
                    "ip": "2.2.2.2",
                    "geo": "US",
                    "amount": 50.0,
                }),
            })
            .unwrap();

            t.record(IncomingEvent {
                event: "click".to_string(),
                properties: serde_json::json!({
                    "ip": "1.1.1.1",
                    "geo": "UK",
                    "amount": 25.0,
                }),
            })
            .unwrap();
        }

        #[test]
        fn test_query_returns_day_results() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            // query using today's timestamp range
            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            assert!(!result.days.is_empty(), "should have at least one day");
            let day = &result.days[0];
            assert!(!day.results.is_empty(), "day should have metric results");
        }

        #[test]
        fn test_query_find_bmp() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            let bmp = result.find(FindOptions {
                metric_type: "bmp".to_string(),
                key: "ip".to_string(),
                add_key: None,
                merge: false,
            });

            assert_eq!(bmp.len(), 1);
            if let MetricData::Count(count) = &bmp[0].result {
                assert_eq!(*count, 2, "should have 2 unique IPs");
            } else {
                panic!("expected Count data for bmp");
            }
        }

        #[test]
        fn test_query_find_add() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            let add = result.find(FindOptions {
                metric_type: "add".to_string(),
                key: "event".to_string(),
                add_key: None,
                merge: false,
            });

            assert_eq!(add.len(), 1);
            if let MetricData::Hash(h) = &add[0].result {
                assert_eq!(h.get("sale"), Some(&2));
                assert_eq!(h.get("click"), Some(&1));
            } else {
                panic!("expected Hash data for add");
            }
        }

        #[test]
        fn test_query_find_top() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            let top = result.find(FindOptions {
                metric_type: "top".to_string(),
                key: "geo".to_string(),
                add_key: None,
                merge: false,
            });

            assert_eq!(top.len(), 1);
            if let MetricData::Ranked(pairs) = &top[0].result {
                assert_eq!(pairs[0].0, "US");
                assert!((pairs[0].1 - 2.0).abs() < f64::EPSILON);
                assert_eq!(pairs[1].0, "UK");
                assert!((pairs[1].1 - 1.0).abs() < f64::EPSILON);
            } else {
                panic!("expected Ranked data for top");
            }
        }

        #[test]
        fn test_query_find_addv_summary() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            // find adv summaries (they have Summary data)
            let adv_results = result.find(FindOptions {
                metric_type: "adv".to_string(),
                key: "event~geo".to_string(),
                add_key: Some("amount".to_string()),
                merge: false,
            });

            // should have both per-label and summary results
            let summaries: Vec<_> = adv_results
                .iter()
                .filter(|r| matches!(r.result, MetricData::Summary { .. }))
                .collect();

            assert!(!summaries.is_empty(), "should have at least one summary");

            if let MetricData::Summary { sum, count } = &summaries[0].result {
                // sum = 100 + 50 + 25 = 175, count = 3
                assert!((sum - 175.0).abs() < 0.01, "sum should be 175, got {}", sum);
                assert_eq!(*count, 3, "count should be 3, got {}", count);
            }
        }

        #[test]
        fn test_query_find_addv_per_label() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            let adv_results = result.find(FindOptions {
                metric_type: "adv".to_string(),
                key: "event~geo".to_string(),
                add_key: Some("amount".to_string()),
                merge: false,
            });

            let labels: Vec<_> = adv_results
                .iter()
                .filter(|r| matches!(r.result, MetricData::FloatHash(_)))
                .collect();

            assert!(!labels.is_empty(), "should have per-label results");

            if let MetricData::FloatHash(h) = &labels[0].result {
                // sale~US=150, click~UK=25, sale~UK would not exist
                let total: f64 = h.values().sum();
                assert!(
                    (total - 175.0).abs() < 0.01,
                    "per-label total should be 175, got {}",
                    total
                );
            }
        }

        #[test]
        fn test_query_find_shorthand() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start, day_start).unwrap();

            let top = result.find_str("top/geo");
            assert!(!top.is_empty(), "shorthand find should return results");

            let bmp = result.find_str("bmp/ip");
            assert!(
                !bmp.is_empty(),
                "shorthand find for bmp should return results"
            );
        }

        #[test]
        fn test_query_empty_range() {
            let t = make_traqq(test_config());
            // don't record anything

            // query a range far in the past
            let result = t.query(0, 86400).unwrap();

            assert!(
                !result.days.is_empty(),
                "should return day structures even if empty"
            );
            for day in &result.days {
                assert!(day.results.is_empty(), "empty days should have no results");
            }
        }

        #[test]
        fn test_query_find_merge() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            // query 2 days (today appears once in results)
            let result = t.query(day_start - 86400, day_start).unwrap();

            let merged = result.find(FindOptions {
                metric_type: "add".to_string(),
                key: "event".to_string(),
                add_key: None,
                merge: true,
            });

            // merge should produce exactly one result
            assert_eq!(merged.len(), 1, "merge should produce one result");
            if let MetricData::Hash(h) = &merged[0].result {
                assert_eq!(h.get("sale"), Some(&2));
                assert_eq!(h.get("click"), Some(&1));
            } else {
                panic!("expected Hash data after merge");
            }
        }

        #[test]
        fn test_query_find_merge_top() {
            let t = make_traqq(test_config());
            record_test_events(&t);

            let now = Utc::now().timestamp();
            let day_start = now - (now % 86400);
            let result = t.query(day_start - 86400, day_start).unwrap();

            let merged = result.find(FindOptions {
                metric_type: "top".to_string(),
                key: "geo".to_string(),
                add_key: None,
                merge: true,
            });

            assert_eq!(merged.len(), 1);
            if let MetricData::Ranked(pairs) = &merged[0].result {
                assert_eq!(pairs[0].0, "US");
                assert!((pairs[0].1 - 2.0).abs() < f64::EPSILON);
            } else {
                panic!("expected Ranked data after merge");
            }
        }
    }

    mod server_client_tests {
        use super::*;
        use crate::client::Client;
        use crate::server;
        use crate::storage::memory::MemoryStorage;
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        #[test]
        fn test_server_client_record_and_query() {
            let config = TraqqConfig {
                time: TimeConfig {
                    store_hourly: false,
                    timezone: "UTC".to_string(),
                },
                mapping: MappingConfig {
                    bitmap: vec!["ip".to_string()],
                    add: vec!["event".to_string()],
                    add_value: vec![],
                    top: vec!["geo".to_string()],
                },
                limits: LimitsConfig::default(),
            };

            let traqq =
                Arc::new(Traqq::new(config, Box::new(MemoryStorage::new()), "srv").unwrap());

            // start server on a random-ish port
            let addr = "127.0.0.1:19876";
            let server_traqq = Arc::clone(&traqq);
            let _server_handle = thread::spawn(move || {
                let _ = server::run(server_traqq, addr);
            });

            // give the server a moment to bind
            thread::sleep(Duration::from_millis(50));

            let mut client = Client::connect(addr).unwrap();

            // record events
            let r = client
                .record(serde_json::json!({
                    "event": "sale",
                    "ip": "1.1.1.1",
                    "geo": "US",
                }))
                .unwrap();
            assert!(r.success, "record should succeed");

            let r = client
                .record(serde_json::json!({
                    "event": "click",
                    "ip": "2.2.2.2",
                    "geo": "UK",
                }))
                .unwrap();
            assert!(r.success);

            // query days
            let r = client.query_days(1).unwrap();
            assert!(r.success, "query_days should succeed");
            assert!(r.data.is_some(), "query should return data");

            // find specific metrics
            let now = chrono::Utc::now().timestamp();
            let day_start = now - (now % 86400);

            let r = client
                .find(day_start, day_start, "add", "event", None, false)
                .unwrap();
            assert!(r.success, "find should succeed");

            let data = r.data.unwrap();
            let results: Vec<serde_json::Value> = serde_json::from_value(data).unwrap();
            assert!(!results.is_empty(), "find should return results");

            // the server thread will block on accept() after we drop the client,
            // so we just let it be (it's a daemon thread effectively)
        }
    }
}
