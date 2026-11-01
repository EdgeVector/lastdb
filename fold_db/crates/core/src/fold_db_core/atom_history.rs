//! Bounded history expiry. The handle follows the database shutdown lifecycle.
use crate::db_operations::{record_store::time_queue::QueueReader, DbOperations};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) struct BackgroundAtomHistory {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BackgroundAtomHistory {
    pub(crate) async fn start(db: Arc<DbOperations>, offline: bool) -> Self {
        if offline {
            return Self {
                task: Mutex::new(None),
            };
        }
        // Read once on the boot caller, which already does storage I/O. An
        // unused expiry task must not warm another worker's allocator heaps.
        // An unreadable marker preserves the existing expiry retry behavior.
        let enabled = db
            .records()
            .atom_history_enabled()
            .await
            .unwrap_or_else(|error| {
                tracing::error!(%error, "history activation check failed; expiry retries");
                true
            });
        // lint:spawn-bare-ok process-lifetime expiry task, stopped with the database.
        let task = tokio::spawn(async move {
            let records = db.records();
            if !enabled {
                records.history_activation().notified().await;
            }
            let mut reader = QueueReader::new(
                crate::db_operations::record_store::atom_history::queue(),
                32,
            );
            let mut ticks = 0u64;
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                if ticks.is_multiple_of(60) {
                    reader.restart();
                }
                ticks = ticks.wrapping_add(1);
                if let Err(error) = records.atom_history_expire_step(&mut reader).await {
                    tracing::error!(%error, "atom history expiry failed; records remain");
                }
            }
        });
        Self {
            task: Mutex::new(Some(task)),
        }
    }

    pub(crate) async fn stop(&self) -> Result<(), String> {
        let task = self.task.lock().expect("history task lock").take();
        if let Some(task) = task {
            task.abort();
            if let Err(error) = task.await {
                if !error.is_cancelled() {
                    return Err(format!("atom history worker failed: {error}"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn abort(&self) {
        if let Some(task) = self.task.lock().expect("history task lock").take() {
            task.abort();
        }
    }
}

impl Drop for BackgroundAtomHistory {
    fn drop(&mut self) {
        self.abort();
    }
}
