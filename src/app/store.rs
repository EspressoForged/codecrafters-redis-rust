use crate::app::error::AppError;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub const WRONGTYPE_ERROR: AppError = AppError::WrongType;

/// Represents the different data types that can be stored.
#[derive(Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
}

#[derive(Debug)]
struct StoreValue {
    data: DataType,
    expires_at: Option<Instant>,
}

/// A thread-safe, in-memory key-value store using DashMap for high concurrency.
#[derive(Debug, Default)]
pub struct Store {
    data: DashMap<Bytes, StoreValue>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    // --- String Commands ---

    pub fn get_string(&self, key: &Bytes) -> Result<Option<Bytes>, AppError> {
        let entry = self.data.get(key);
        if let Some(value) = entry {
            if Self::is_expired(&value) {
                // To be fully correct, we should remove the expired key here.
                // This requires a write lock, so we'll get the key again.
                drop(value); // release the read lock
                self.data.remove(key);
                return Ok(None);
            }
            match &value.data {
                DataType::String(s) => Ok(Some(s.clone())),
                _ => Err(WRONGTYPE_ERROR),
            }
        } else {
            Ok(None)
        }
    }

    pub fn set_string(&self, key: Bytes, value: Bytes, expiry: Option<Duration>) -> Result<(), AppError> {
        // DashMap's `entry` API is perfect here, but for simplicity with WRONGTYPE checks,
        // we'll use a slightly more explicit get/insert.
        if let Some(mut entry) = self.data.get_mut(&key) {
            if !matches!(entry.data, DataType::String(_)) {
                return Err(WRONGTYPE_ERROR);
            }
            let expires_at = expiry.and_then(|d| Instant::now().checked_add(d));
            entry.data = DataType::String(value);
            entry.expires_at = expires_at;
            return Ok(());
        }

        let expires_at = expiry.and_then(|d| Instant::now().checked_add(d));
        let value = StoreValue {
            data: DataType::String(value),
            expires_at,
        };
        self.data.insert(key, value);
        Ok(())
    }

    pub fn incr(&self, key: &Bytes) -> Result<i64, AppError> {
        let mut entry = self.data.entry(key.clone()).or_insert_with(|| StoreValue {
            data: DataType::String(Bytes::from_static(b"0")),
            expires_at: None,
        });

        match &mut entry.value_mut().data {
            DataType::String(bytes) => {
                let current_val = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or(AppError::ParseError("value is not an integer or out of range".into()))?;
                
                let new_val = current_val + 1;
                *bytes = Bytes::from(new_val.to_string());
                Ok(new_val)
            }
            _ => Err(WRONGTYPE_ERROR),
        }
    }

    // --- List Commands ---

    pub fn lpush(&self, key: Bytes, values: &[Bytes]) -> Result<usize, AppError> {
        let mut entry = self.data.entry(key).or_insert_with(|| StoreValue {
            data: DataType::List(VecDeque::new()),
            expires_at: None,
        });

        match &mut entry.value_mut().data {
            DataType::List(list) => {
                for value in values {
                    list.push_front(value.clone());
                }
                Ok(list.len())
            }
            _ => Err(WRONGTYPE_ERROR),
        }
    }

    pub fn rpush(&self, key: Bytes, values: &[Bytes]) -> Result<usize, AppError> {
        let mut entry = self.data.entry(key).or_insert_with(|| StoreValue {
            data: DataType::List(VecDeque::new()),
            expires_at: None,
        });

        match &mut entry.value_mut().data {
            DataType::List(list) => {
                for value in values {
                    list.push_back(value.clone());
                }
                Ok(list.len())
            }
            _ => Err(WRONGTYPE_ERROR),
        }
    }

    pub fn lpop(&self, key: &Bytes, count: usize) -> Result<Option<Vec<Bytes>>, AppError> {
        let mut entry = match self.data.get_mut(key) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        if Self::is_expired(&entry) {
            drop(entry);
            self.data.remove(key);
            return Ok(None);
        }

        match &mut entry.value_mut().data {
            DataType::List(list) => {
                if list.is_empty() {
                    return Ok(None);
                }
                let mut popped = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some(val) = list.pop_front() {
                        popped.push(val);
                    } else {
                        break;
                    }
                }
                Ok(Some(popped))
            }
            _ => Err(WRONGTYPE_ERROR),
        }
    }

    pub fn llen(&self, key: &Bytes) -> Result<usize, AppError> {
        let entry = self.data.get(key);
        if let Some(value) = entry {
            if Self::is_expired(&value) {
                drop(value);
                self.data.remove(key);
                return Ok(0);
            }
            match &value.data {
                DataType::List(list) => Ok(list.len()),
                _ => Err(WRONGTYPE_ERROR),
            }
        } else {
            Ok(0)
        }
    }

    pub fn lrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>, AppError> {
        let entry = self.data.get(key);
        if let Some(value) = entry {
            if Self::is_expired(&value) {
                drop(value);
                self.data.remove(key);
                return Ok(vec![]);
            }
            match &value.data {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    let start = Self::normalize_index(start, len);
                    let stop = Self::normalize_index(stop, len);

                    if start > stop || start >= (len as usize) {
                        return Ok(Vec::new());
                    }

                    let items = list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .collect();
                    Ok(items)
                }
                _ => Err(WRONGTYPE_ERROR),
            }
        } else {
            Ok(vec![])
        }
    }

    // --- Helper Functions ---

    fn is_expired(value: &StoreValue) -> bool {
        matches!(value.expires_at, Some(t) if Instant::now() > t)
    }
    
    fn normalize_index(index: i64, len: i64) -> usize {
        if index >= 0 {
            index as usize
        } else {
            (len + index).max(0) as usize
        }
    }
}