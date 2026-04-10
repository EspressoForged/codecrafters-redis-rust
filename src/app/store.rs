use crate::app::sorted_set::SortedSet;
use crate::app::stream::Stream;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::Instant;

#[derive(Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(Stream),
    SortedSet(RwLock<SortedSet>),
}

#[derive(Debug)]
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
            .filter(|entry| !self.is_expired(entry.value()))
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn get_type(&self, key: &Bytes) -> String {
        match self.data.get(key) {
            Some(entry) if !self.is_expired(&entry) => match entry.data {
                DataType::String(_) => "string".to_string(),
                DataType::List(_) => "list".to_string(),
                DataType::Stream(_) => "stream".to_string(),
                DataType::SortedSet(_) => "zset".to_string(),
            },
            _ => "none".to_string(),
        }
    }

    pub fn db(&self) -> &DashMap<Bytes, StoreValue> {
        &self.data
    }

    pub fn is_expired(&self, value: &StoreValue) -> bool {
        matches!(value.expires_at, Some(t) if Instant::now() > t)
    }

    pub fn normalize_index(&self, index: i64, len: i64) -> usize {
        if index >= 0 {
            index as usize
        } else {
            (len + index).max(0) as usize
        }
    }
}
