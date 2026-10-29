//! Bounded history expiry. The handle follows the database shutdown lifecycle.
use crate::db_operations::{atom_store::time_queue::QueueReader, DbOperations};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) struct BackgroundAtomHistory {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BackgroundAtomHistory {
    pub(crate) fn start(db: Arc<DbOperations>, offline: bool) -> Self {
        if offline {
            return Self {
                task: Mutex::new(None),
            };
        }
        // lint:spawn-bare-ok process-lifetime expiry task, stopped with the database.
        let task = tokio::spawn(async move {
            let atoms = db.atoms();
            let mut reader =
                QueueReader::new(crate::db_operations::atom_store::atom_history::queue(), 32);
            let mut ticks = 0u64;
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                if ticks.is_multiple_of(60) {
                    reader.restart();
                }
                ticks = ticks.wrapping_add(1);
                if let Err(error) = atoms.atom_history_expire_step(&mut reader).await {
                    tracing::error!(%error, "atom history expiry failed; records remain");
                }
            }
        });
        Self {
            task: Mutex::new(Some(task)),
        }
    }

    pub(crate) fn stop(&self) {
        if let Some(task) = self.task.lock().expect("history task lock").take() {
            task.abort();
        }
    }
}

impl Drop for BackgroundAtomHistory {
    fn drop(&mut self) {
        self.stop();
    }
}
