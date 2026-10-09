use super::Ledger;
use std::io;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Owns the session heartbeat thread until a clean shutdown joins it.
pub struct SessionHeartbeat {
    stop_tx: Sender<()>,
    thread: JoinHandle<()>,
}

impl SessionHeartbeat {
    /// Wait for any in-progress marker write before the clean stamp and receipt.
    pub fn stop(self) -> io::Result<()> {
        let Self { stop_tx, thread } = self;
        drop(stop_tx);
        thread
            .join()
            .map_err(|_| io::Error::other("session heartbeat thread panicked"))
    }
}

impl Ledger {
    /// Start the heartbeat thread. The caller must stop it before a clean stamp.
    pub fn spawn_heartbeat(&self, interval: Duration) -> io::Result<SessionHeartbeat> {
        let ledger = self.clone();
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("folddb-session-heartbeat".to_string())
            .spawn(move || {
                while let Err(RecvTimeoutError::Timeout) = stop_rx.recv_timeout(interval) {
                    if let Err(error) = ledger.heartbeat() {
                        tracing::debug!(
                            target: "lastdbd::session_ledger",
                            error = %error,
                            "session heartbeat write failed; will retry next tick"
                        );
                    }
                }
            })?;
        Ok(SessionHeartbeat { stop_tx, thread })
    }
}
