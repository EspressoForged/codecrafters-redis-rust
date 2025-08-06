use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{self, Duration};
use tracing::debug;

/// A registry for tasks waiting for list notifications (for BLPOP).
/// It also serves as a general notification mechanism for the WAIT command.
#[derive(Debug, Default)]
pub struct WaiterRegistry {
    /// Maps a list key to a queue of `Notify` objects (for BLPOP).
    waiters: DashMap<Bytes, VecDeque<Arc<Notify>>>,
    /// A set of `Notify` objects for tasks specifically waiting for `WAIT` conditions.
    // We use a DashMap here mapping a dummy key to a VecDeque, to reuse existing logic.
    wait_command_notifiers: DashMap<u8, VecDeque<Arc<Notify>>>, // Using a dummy key like 0 for a single queue.
}

impl WaiterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes up one waiting task for a given key by sending it the value (for BLPOP).
    pub fn notify_one(&self, key: &Bytes) { // Removed value as it's not needed by Notify
        if let Some(mut queue) = self.waiters.get_mut(key) {
            if let Some(notify) = queue.pop_front() {
                debug!("notifying BLPOP waiter for key: {:?}", key);
                notify.notify_one();
            }
            if queue.is_empty() {
                drop(queue);
                self.waiters.remove(key);
            }
        }
    }

    /// Blocks the current task until notified for one of the given keys or until the timeout expires (for BLPOP).
    pub async fn wait_for_any(&self, keys: &[Bytes], timeout: Option<Duration>) {
        let notify = Arc::new(Notify::new());

        for key in keys {
            self.waiters.entry(key.clone()).or_default().push_back(notify.clone());
        }

        let notified_future = notify.notified();

        if let Some(duration) = timeout {
            let _ = time::timeout(duration, notified_future).await;
        } else {
            notified_future.await;
        }

        for key in keys {
             if let Some(mut queue) = self.waiters.get_mut(key) {
                queue.retain(|n| !Arc::ptr_eq(n, &notify));
                 if queue.is_empty() {
                    drop(queue);
                    self.waiters.remove(key);
                 }
             }
        }
    }

    /// Registers a `Notify` object for a `WAIT` command.
    pub async fn register_wait_notifier(&self, notify: Arc<Notify>) {
        self.wait_command_notifiers.entry(0).or_default().push_back(notify);
    }

    /// Notifies all tasks waiting on a `WAIT` command (e.g., after a propagation).
    pub fn notify_all(&self) {
        if let Some(mut queue) = self.wait_command_notifiers.get_mut(&0) {
            // Drain and notify all current waiters.
            // New WAIT commands will register their own notify objects.
            for notify in queue.drain(..) {
                notify.notify_one();
            }
        }
    }
}