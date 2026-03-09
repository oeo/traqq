use std::collections::HashMap;

use redis::{Client, Commands, Connection};
use std::sync::Mutex;

use super::{Storage, StorageError};

/// redis storage backend.
///
/// uses a sync redis connection behind a mutex.
/// each trait method maps directly to one or two redis commands.
pub struct RedisStorage {
    conn: Mutex<Connection>,
}

impl RedisStorage {
    /// connect to redis at the given url (e.g., "redis://127.0.0.1:6379")
    pub fn new(url: &str) -> Result<Self, StorageError> {
        let client = Client::open(url)
            .map_err(|e| StorageError::OperationFailed(format!("redis connect: {}", e)))?;
        let conn = client
            .get_connection()
            .map_err(|e| StorageError::OperationFailed(format!("redis connection: {}", e)))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl Storage for RedisStorage {
    fn hash_increment(&self, key: &str, field: &str, amount: i64) -> Result<i64, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: i64 = conn
            .hincr(key, field, amount)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn hash_increment_float(
        &self,
        key: &str,
        field: &str,
        amount: f64,
    ) -> Result<f64, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: f64 = redis::cmd("HINCRBYFLOAT")
            .arg(key)
            .arg(field)
            .arg(amount)
            .query(&mut *conn)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn hash_get_all(&self, key: &str) -> Result<HashMap<String, String>, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: HashMap<String, String> = conn
            .hgetall(key)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn hyperloglog_add(&self, key: &str, value: &str) -> Result<bool, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: i32 = conn
            .pfadd(key, value)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result > 0)
    }

    fn hyperloglog_count(&self, key: &str) -> Result<u64, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: u64 = conn
            .pfcount(key)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn sorted_set_increment(
        &self,
        key: &str,
        member: &str,
        amount: f64,
    ) -> Result<f64, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: f64 = conn
            .zincr(key, member, amount)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn sorted_set_top(&self, key: &str, limit: usize) -> Result<Vec<(String, f64)>, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        // ZREVRANGEBYSCORE key +inf -inf WITHSCORES LIMIT 0 limit
        let result: Vec<(String, f64)> = redis::cmd("ZREVRANGEBYSCORE")
            .arg(key)
            .arg("+inf")
            .arg("-inf")
            .arg("WITHSCORES")
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query(&mut *conn)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn set_add(&self, key: &str, members: &[String]) -> Result<usize, StorageError> {
        if members.is_empty() {
            return Ok(0);
        }
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: usize = conn
            .sadd(key, members)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }

    fn set_members(&self, key: &str) -> Result<Vec<String>, StorageError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        let result: Vec<String> = conn
            .smembers(key)
            .map_err(|e| StorageError::OperationFailed(e.to_string()))?;
        Ok(result)
    }
}
