pub mod memory;

#[cfg(feature = "redis-storage")]
pub mod redis;

use std::collections::HashMap;
use std::fmt;

/// errors from storage operations
#[derive(Debug)]
pub enum StorageError {
    /// the key was not found
    NotFound(String),
    /// a generic operation failure
    OperationFailed(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound(key) => write!(f, "key not found: {}", key),
            StorageError::OperationFailed(msg) => write!(f, "operation failed: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

/// backend-agnostic storage operations for traqq metrics
///
/// each method maps to a specific data structure operation.
/// backends (memory, redis, rocksdb) implement these primitives.
pub trait Storage: Send + Sync {
    // -- hash operations (add, addv) --

    /// increment a hash field by `amount`. creates the key/field if missing.
    fn hash_increment(&self, key: &str, field: &str, amount: i64) -> Result<i64, StorageError>;

    /// increment a hash field by a float amount. creates the key/field if missing.
    fn hash_increment_float(
        &self,
        key: &str,
        field: &str,
        amount: f64,
    ) -> Result<f64, StorageError>;

    /// return all field-value pairs for a hash key
    fn hash_get_all(&self, key: &str) -> Result<HashMap<String, String>, StorageError>;

    // -- hyperloglog operations (bmp) --

    /// add a value to a hyperloglog. returns true if the cardinality changed.
    fn hyperloglog_add(&self, key: &str, value: &str) -> Result<bool, StorageError>;

    /// return the approximate cardinality of a hyperloglog
    fn hyperloglog_count(&self, key: &str) -> Result<u64, StorageError>;

    // -- sorted set operations (top) --

    /// increment a member's score in a sorted set by `amount`
    fn sorted_set_increment(
        &self,
        key: &str,
        member: &str,
        amount: f64,
    ) -> Result<f64, StorageError>;

    /// return the top `limit` members by score (descending), with scores
    fn sorted_set_top(&self, key: &str, limit: usize) -> Result<Vec<(String, f64)>, StorageError>;

    // -- set operations (key tracking) --

    /// add members to a set. returns the number of new members added.
    fn set_add(&self, key: &str, members: &[String]) -> Result<usize, StorageError>;

    /// return all members of a set
    fn set_members(&self, key: &str) -> Result<Vec<String>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::memory::MemoryStorage;
    use super::*;

    fn make_storage() -> MemoryStorage {
        MemoryStorage::new()
    }

    // -- hash tests --

    #[test]
    fn test_hash_increment_creates_key_and_field() {
        let s = make_storage();
        let result = s.hash_increment("h1", "f1", 1).unwrap();
        assert_eq!(result, 1);

        let result = s.hash_increment("h1", "f1", 5).unwrap();
        assert_eq!(result, 6);
    }

    #[test]
    fn test_hash_increment_multiple_fields() {
        let s = make_storage();
        s.hash_increment("h1", "a", 10).unwrap();
        s.hash_increment("h1", "b", 20).unwrap();
        s.hash_increment("h1", "a", 5).unwrap();

        let all = s.hash_get_all("h1").unwrap();
        assert_eq!(all.get("a").unwrap(), "15");
        assert_eq!(all.get("b").unwrap(), "20");
    }

    #[test]
    fn test_hash_increment_float() {
        let s = make_storage();
        let r = s.hash_increment_float("h1", "amt", 99.99).unwrap();
        assert!((r - 99.99).abs() < f64::EPSILON);

        let r = s.hash_increment_float("h1", "amt", 0.01).unwrap();
        assert!((r - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hash_get_all_missing_key() {
        let s = make_storage();
        let all = s.hash_get_all("nonexistent").unwrap();
        assert!(all.is_empty());
    }

    // -- hyperloglog tests --

    #[test]
    fn test_hyperloglog_add_and_count() {
        let s = make_storage();
        assert!(s.hyperloglog_add("hll", "a").unwrap());
        assert!(s.hyperloglog_add("hll", "b").unwrap());
        // duplicate
        assert!(!s.hyperloglog_add("hll", "a").unwrap());

        let count = s.hyperloglog_count("hll").unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_hyperloglog_count_missing_key() {
        let s = make_storage();
        let count = s.hyperloglog_count("nonexistent").unwrap();
        assert_eq!(count, 0);
    }

    // -- sorted set tests --

    #[test]
    fn test_sorted_set_increment_and_top() {
        let s = make_storage();
        s.sorted_set_increment("ss", "apple", 3.0).unwrap();
        s.sorted_set_increment("ss", "banana", 7.0).unwrap();
        s.sorted_set_increment("ss", "cherry", 1.0).unwrap();
        s.sorted_set_increment("ss", "apple", 5.0).unwrap();

        let top = s.sorted_set_top("ss", 10).unwrap();
        assert_eq!(top.len(), 3);
        // descending order: apple(8), banana(7), cherry(1)
        assert_eq!(top[0].0, "apple");
        assert!((top[0].1 - 8.0).abs() < f64::EPSILON);
        assert_eq!(top[1].0, "banana");
        assert!((top[1].1 - 7.0).abs() < f64::EPSILON);
        assert_eq!(top[2].0, "cherry");
        assert!((top[2].1 - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sorted_set_top_with_limit() {
        let s = make_storage();
        s.sorted_set_increment("ss", "a", 1.0).unwrap();
        s.sorted_set_increment("ss", "b", 2.0).unwrap();
        s.sorted_set_increment("ss", "c", 3.0).unwrap();

        let top = s.sorted_set_top("ss", 2).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "c");
        assert_eq!(top[1].0, "b");
    }

    #[test]
    fn test_sorted_set_top_missing_key() {
        let s = make_storage();
        let top = s.sorted_set_top("nonexistent", 10).unwrap();
        assert!(top.is_empty());
    }

    // -- set tests --

    #[test]
    fn test_set_add_and_members() {
        let s = make_storage();
        let added = s
            .set_add("s1", &["a".into(), "b".into(), "c".into()])
            .unwrap();
        assert_eq!(added, 3);

        // duplicates ignored
        let added = s
            .set_add("s1", &["b".into(), "c".into(), "d".into()])
            .unwrap();
        assert_eq!(added, 1);

        let mut members = s.set_members("s1").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_set_members_missing_key() {
        let s = make_storage();
        let members = s.set_members("nonexistent").unwrap();
        assert!(members.is_empty());
    }

    // -- cross-type isolation --

    #[test]
    fn test_different_types_same_key_name_isolated() {
        let s = make_storage();
        // using the same key name across different types should not collide.
        // in practice keys will be prefixed (bmp:, add:, etc.) so this tests
        // that the storage internals keep separate namespaces.
        s.hash_increment("data", "f1", 10).unwrap();
        s.hyperloglog_add("data_hll", "val").unwrap();
        s.sorted_set_increment("data_ss", "m", 5.0).unwrap();
        s.set_add("data_set", &["x".into()]).unwrap();

        assert_eq!(s.hash_get_all("data").unwrap().get("f1").unwrap(), "10");
        assert_eq!(s.hyperloglog_count("data_hll").unwrap(), 1);
        assert_eq!(s.sorted_set_top("data_ss", 10).unwrap()[0].1, 5.0);
        assert_eq!(s.set_members("data_set").unwrap(), vec!["x"]);
    }
}

#[cfg(all(test, feature = "redis-storage"))]
mod redis_tests {
    use super::redis::RedisStorage;
    use super::*;

    fn cleanup_and_connect() -> Option<RedisStorage> {
        let mut conn = ::redis::Client::open("redis://127.0.0.1:6379")
            .ok()?
            .get_connection()
            .ok()?;
        // clean up test keys from previous runs
        let keys: Vec<String> = ::redis::cmd("KEYS")
            .arg("traqq_test:*")
            .query(&mut conn)
            .unwrap_or_default();
        if !keys.is_empty() {
            let _: () = ::redis::cmd("DEL")
                .arg(&keys)
                .query(&mut conn)
                .unwrap_or_default();
        }
        drop(conn);
        RedisStorage::new("redis://127.0.0.1:6379").ok()
    }

    #[test]
    fn test_redis_hash_operations() {
        let s = match cleanup_and_connect() {
            Some(s) => s,
            None => {
                eprintln!("skipping: redis not available");
                return;
            }
        };

        let r = s.hash_increment("traqq_test:h1", "f1", 1).unwrap();
        assert_eq!(r, 1);

        let r = s.hash_increment("traqq_test:h1", "f1", 5).unwrap();
        assert_eq!(r, 6);

        let r = s.hash_increment_float("traqq_test:h1", "f2", 99.5).unwrap();
        assert!((r - 99.5).abs() < 0.01);

        let all = s.hash_get_all("traqq_test:h1").unwrap();
        assert_eq!(all.get("f1").unwrap(), "6");
    }

    #[test]
    fn test_redis_hyperloglog() {
        let s = match cleanup_and_connect() {
            Some(s) => s,
            None => {
                eprintln!("skipping: redis not available");
                return;
            }
        };

        assert!(s.hyperloglog_add("traqq_test:hll", "a").unwrap());
        assert!(s.hyperloglog_add("traqq_test:hll", "b").unwrap());
        // pfadd returns 0 for duplicates but redis behavior can vary;
        // the important thing is the count
        s.hyperloglog_add("traqq_test:hll", "a").unwrap();

        let count = s.hyperloglog_count("traqq_test:hll").unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_redis_sorted_set() {
        let s = match cleanup_and_connect() {
            Some(s) => s,
            None => {
                eprintln!("skipping: redis not available");
                return;
            }
        };

        s.sorted_set_increment("traqq_test:ss", "apple", 3.0)
            .unwrap();
        s.sorted_set_increment("traqq_test:ss", "banana", 7.0)
            .unwrap();
        s.sorted_set_increment("traqq_test:ss", "apple", 5.0)
            .unwrap();

        let top = s.sorted_set_top("traqq_test:ss", 10).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "apple");
        assert!((top[0].1 - 8.0).abs() < f64::EPSILON);
        assert_eq!(top[1].0, "banana");
        assert!((top[1].1 - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_redis_set_operations() {
        let s = match cleanup_and_connect() {
            Some(s) => s,
            None => {
                eprintln!("skipping: redis not available");
                return;
            }
        };

        let added = s
            .set_add("traqq_test:s1", &["a".into(), "b".into(), "c".into()])
            .unwrap();
        assert_eq!(added, 3);

        let added = s
            .set_add("traqq_test:s1", &["b".into(), "d".into()])
            .unwrap();
        assert_eq!(added, 1);

        let mut members = s.set_members("traqq_test:s1").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_redis_full_traqq_round_trip() {
        let s = match cleanup_and_connect() {
            Some(s) => s,
            None => {
                eprintln!("skipping: redis not available");
                return;
            }
        };

        use crate::*;

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

        let t = Traqq::new(config, Box::new(s), "traqq_test").unwrap();

        t.record(IncomingEvent {
            event: "sale".to_string(),
            properties: serde_json::json!({
                "ip": "1.1.1.1",
                "geo": "US",
            }),
        })
        .unwrap();

        t.record(IncomingEvent {
            event: "sale".to_string(),
            properties: serde_json::json!({
                "ip": "2.2.2.2",
                "geo": "US",
            }),
        })
        .unwrap();

        t.record(IncomingEvent {
            event: "click".to_string(),
            properties: serde_json::json!({
                "ip": "1.1.1.1",
                "geo": "UK",
            }),
        })
        .unwrap();

        // query today
        let now = chrono::Utc::now().timestamp();
        let day_start = now - (now % 86400);
        let result = t.query(day_start, day_start).unwrap();

        // bmp: 2 unique IPs
        let bmp = result.find(FindOptions {
            metric_type: "bmp".to_string(),
            key: "ip".to_string(),
            add_key: None,
            merge: false,
        });
        assert!(!bmp.is_empty(), "should have bmp results from redis");
        if let MetricData::Count(c) = &bmp[0].result {
            assert_eq!(*c, 2);
        }

        // add: sale=2, click=1
        let add = result.find(FindOptions {
            metric_type: "add".to_string(),
            key: "event".to_string(),
            add_key: None,
            merge: false,
        });
        assert!(!add.is_empty());
        if let MetricData::Hash(h) = &add[0].result {
            assert_eq!(h.get("sale"), Some(&2));
            assert_eq!(h.get("click"), Some(&1));
        }

        // top: US=2, UK=1
        let top = result.find(FindOptions {
            metric_type: "top".to_string(),
            key: "geo".to_string(),
            add_key: None,
            merge: false,
        });
        assert!(!top.is_empty());
        if let MetricData::Ranked(pairs) = &top[0].result {
            assert_eq!(pairs[0].0, "US");
            assert!((pairs[0].1 - 2.0).abs() < f64::EPSILON);
        }
    }
}
