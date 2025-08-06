use crate::app::error::{AppError, WRONGTYPE_ERROR};
use crate::app::stream::{Stream, StreamEntry, StreamId};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(Stream),
}

#[derive(Debug, Clone)]
pub struct StoreValue {
    pub data: DataType,
    pub expires_at: Option<Instant>,
}

#[derive(Debug, Default)]
pub struct Store {
    data: DashMap<Bytes, StoreValue>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_from_rdb(&self, rdb_store: HashMap<Bytes, StoreValue>) {
        for (key, value) in rdb_store {
            self.data.insert(key, value);
        }
    }
    
    pub fn get_all_keys(&self) -> Vec<Bytes> {
        self.data
            .iter()
            .filter(|entry| !Self::is_expired(entry.value()))
            .map(|entry| entry.key().clone())
            .collect()
    }
    
    pub fn get_type(&self, key: &Bytes) -> String {
        match self.data.get(key) {
            Some(entry) if !Self::is_expired(&entry) => match entry.data {
                DataType::String(_) => "string".to_string(),
                DataType::List(_) => "list".to_string(),
                DataType::Stream(_) => "stream".to_string(),
            },
            _ => "none".to_string(),
        }
    }

    // --- String Commands ---

    pub fn get_string(&self, key: &Bytes) -> Result<Option<Bytes>, AppError> {
        let entry = self.data.get(key);
        if let Some(value) = entry {
            if Self::is_expired(&value) {
                drop(value);
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
                    .ok_or(AppError::ValueError(
                        "value is not an integer or out of range".into(),
                    ))?;
                
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

    // --- Stream Commands ---

    pub fn xadd(&self, key: Bytes, id_spec: &str, fields: Vec<(Bytes, Bytes)>) -> Result<StreamId, AppError> {
        let mut entry = self.data.entry(key).or_insert_with(|| StoreValue {
            data: DataType::Stream(Stream::new()),
            expires_at: None,
        });

        match &mut entry.value_mut().data {
            DataType::Stream(stream) => stream.add(id_spec, fields),
            _ => Err(WRONGTYPE_ERROR),
        }
    }

    pub fn xrange(&self, key: &Bytes, start: &str, end: &str) -> Result<Vec<StreamEntry>, AppError> {
        match self.data.get(key) {
            Some(entry) if !Self::is_expired(&entry) => match &entry.data {
                DataType::Stream(stream) => stream.range(start, end),
                _ => Err(WRONGTYPE_ERROR),
            },
            _ => Ok(vec![]),
        }
    }
    
    pub fn xread(&self, keys_and_ids: &[(&Bytes, &str)]) -> Result<Option<Vec<(Bytes, Vec<StreamEntry>)>>, AppError> {
        let mut results = Vec::new();
        for (key, id_spec) in keys_and_ids {
            let stream_data = match self.data.get(*key) {
                Some(entry) if !Self::is_expired(&entry) => match &entry.data {
                    DataType::Stream(stream) => {
                        let entries = stream.read_from(id_spec)?;
                        if !entries.is_empty() {
                            Some(((*key).clone(), entries))
                        } else { None }
                    }
                    _ => return Err(WRONGTYPE_ERROR),
                },
                _ => None,
            };
            if let Some(data) = stream_data {
                results.push(data);
            }
        }
        
        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(results))
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