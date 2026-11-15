//! Thread-safe cloud sync status snapshot exposed via `GET /api/status`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{futures::Notified, Notify};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CloudSyncStatus {
    pub connected: bool,
    pub environment: Option<String>,
    pub captures_accepted: bool,
    pub payment_hold: bool,
    pub last_error: Option<String>,
}

pub(crate) struct CloudSyncTracker {
    state: Mutex<CloudSyncStatus>,
    wake_notify: Arc<Notify>,
}

impl CloudSyncTracker {
    pub(crate) fn from_boot(home: &Path, captures_accepted: bool) -> Self {
        let (connected, environment, last_error) = read_home_connection(home);
        Self {
            state: Mutex::new(CloudSyncStatus {
                connected,
                environment,
                captures_accepted,
                payment_hold: false,
                last_error,
            }),
            wake_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn wake(&self) {
        self.wake_notify.notify_one();
    }

    pub(crate) fn notified(&self) -> Notified<'_> {
        self.wake_notify.notified()
    }

    pub(crate) fn refresh(&self, home: &Path, captures_accepted: bool) {
        let (connected, environment, cred_err) = read_home_connection(home);
        let mut guard = self.lock();
        guard.connected = connected;
        guard.environment = environment;
        guard.captures_accepted = captures_accepted;
        if let Some(err) = cred_err {
            guard.last_error = Some(err);
        } else if !connected {
            guard.payment_hold = false;
            guard.last_error = None;
        }
    }

    pub(crate) fn record_payment_hold(&self) {
        let mut guard = self.lock();
        guard.payment_hold = true;
        guard.last_error = Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE.to_owned());
    }

    pub(crate) fn record_error(&self, error: impl Into<String>) {
        let message = error.into();
        let mut guard = self.lock();
        if message == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
            guard.payment_hold = true;
        }
        guard.last_error = Some(message);
    }

    pub(crate) fn record_healthy(&self) {
        let mut guard = self.lock();
        guard.payment_hold = false;
        guard.last_error = None;
    }

    pub(crate) fn clear_payment_hold(&self) {
        let mut guard = self.lock();
        if guard.payment_hold {
            guard.payment_hold = false;
            if guard.last_error.as_deref() == Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE) {
                guard.last_error = None;
            }
        }
    }

    pub(crate) fn snapshot(&self) -> CloudSyncStatus {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CloudSyncStatus> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn read_home_connection(home: &Path) -> (bool, Option<String>, Option<String>) {
    match crate::cloud::load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let env = saved
                .connection
                .as_ref()
                .map(|c| c.environment.clone())
                .or_else(|| {
                    crate::profile::endpoints::environment_for_exemem_api_url(&saved.api_url)
                        .map(|e| e.as_str().to_owned())
                });
            (true, env, None)
        }
        Ok(None) => (false, None, None),
        Err(err) => (false, None, Some(err)),
    }
}
