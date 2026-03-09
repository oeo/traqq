use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::RwLock;

use super::{Storage, StorageError};

/// in-memory storage backend for testing and embedded use.
///
/// uses rwlock-wrapped hashmaps internally. each data structure type
/// (hash, hyperloglog, sorted set, set) has its own namespace to avoid
/// key collisions.
pub struct MemoryStorage {
    hashes: RwLock<HashMap<String, HashMap<String, f64>>>,
    hyperloglogs: RwLock<HashMap<String, HashSet<String>>>,
    sorted_sets: RwLock<HashMap<String, BTreeMap<String, f64>>>,
    sets: RwLock<HashMap<String, HashSet<String>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            hashes: RwLock::new(HashMap::new()),
            hyperloglogs: RwLock::new(HashMap::new()),
            sorted_sets: RwLock::new(HashMap::new()),
            sets: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn hash_increment(&self, key: &str, field: &str, amount: i64) -> Result<i64, StorageError> {
        let mut hashes = self
            .hashes
            .write()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let entry = hashes
            .entry(key.to_string())
            .or_default()
            .entry(field.to_string())
            .or_insert(0.0);
        *entry += amount as f64;
        Ok(*entry as i64)
    }

    fn hash_increment_float(
        &self,
        key: &str,
        field: &str,
        amount: f64,
    ) -> Result<f64, StorageError> {
        let mut hashes = self
            .hashes
            .write()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let entry = hashes
            .entry(key.to_string())
            .or_default()
            .entry(field.to_string())
            .or_insert(0.0);
        *entry += amount;
        Ok(*entry)
    }

    fn hash_get_all(&self, key: &str) -> Result<HashMap<String, String>, StorageError> {
        let hashes = self
            .hashes
            .read()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        match hashes.get(key) {
            Some(fields) => {
                let result = fields
                    .iter()
                    .map(|(k, v)| {
                        // render integers without decimal point
                        let s = if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
                            format!("{}", *v as i64)
                        } else {
                            v.to_string()
                        };
                        (k.clone(), s)
                    })
                    .collect();
                Ok(result)
            }
            None => Ok(HashMap::new()),
        }
    }

    fn hyperloglog_add(&self, key: &str, value: &str) -> Result<bool, StorageError> {
        let mut hlls = self
            .hyperloglogs
            .write()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let set = hlls.entry(key.to_string()).or_default();
        Ok(set.insert(value.to_string()))
    }

    fn hyperloglog_count(&self, key: &str) -> Result<u64, StorageError> {
        let hlls = self
            .hyperloglogs
            .read()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        match hlls.get(key) {
            Some(set) => Ok(set.len() as u64),
            None => Ok(0),
        }
    }

    fn sorted_set_increment(
        &self,
        key: &str,
        member: &str,
        amount: f64,
    ) -> Result<f64, StorageError> {
        let mut ss = self
            .sorted_sets
            .write()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let entry = ss
            .entry(key.to_string())
            .or_default()
            .entry(member.to_string())
            .or_insert(0.0);
        *entry += amount;
        Ok(*entry)
    }

    fn sorted_set_top(&self, key: &str, limit: usize) -> Result<Vec<(String, f64)>, StorageError> {
        let ss = self
            .sorted_sets
            .read()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        match ss.get(key) {
            Some(members) => {
                let mut pairs: Vec<(String, f64)> =
                    members.iter().map(|(k, v)| (k.clone(), *v)).collect();
                // descending by score
                pairs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                pairs.truncate(limit);
                Ok(pairs)
            }
            None => Ok(Vec::new()),
        }
    }

    fn set_add(&self, key: &str, members: &[String]) -> Result<usize, StorageError> {
        let mut sets = self
            .sets
            .write()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let set = sets.entry(key.to_string()).or_default();
        let mut added = 0;
        for m in members {
            if set.insert(m.clone()) {
                added += 1;
            }
        }
        Ok(added)
    }

    fn set_members(&self, key: &str) -> Result<Vec<String>, StorageError> {
        let sets = self
            .sets
            .read()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        match sets.get(key) {
            Some(set) => Ok(set.iter().cloned().collect()),
            None => Ok(Vec::new()),
        }
    }
}
