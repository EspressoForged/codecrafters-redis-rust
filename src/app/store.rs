use bytes::Bytes;
use dashmap::DashMap;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct StoreValue {
    data: Bytes,
    expires_at: Option<Instant>,
}

/// A thread-safe, in-memory key-value store using DashMap for high concurrency.
#[derive(Debug, Default)]
pub struct Store {
    data: DashMap<Bytes, StoreValue>,
}

impl Store {
    /// Creates a new, empty `Store`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a key to a value with an optional expiry.
    ///
    /// The `DashMap::insert` method handles locking internally.
    pub fn set(&self, key: Bytes, value: Bytes, expiry: Option<Duration>) {
        let expires_at = expiry.and_then(|d| Instant::now().checked_add(d));

        let value = StoreValue {
            data: value,
            expires_at,
        };

        self.data.insert(key, value);
    }

    /// Gets the value for a key.
    ///
    /// Returns `None` if the key does not exist or has expired.
    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        // `DashMap::get` returns a `Ref`, a smart pointer that holds a read lock
        // for a specific shard. The lock is released when `value` goes out of scope.
        if let Some(value) = self.data.get(key) {
            if let Some(expires_at) = value.expires_at {
                if Instant::now() > expires_at {
                    // To be fully correct, we should also remove the key here,
                    // but for simplicity, we'll rely on "get" to filter expired keys.
                    // A background task could handle active cleanup.
                    return None;
                }
            }
            // We clone the `Bytes` object, which is cheap (it's a reference-counted pointer).
            return Some(value.data.clone());
        }
        None
    }
}