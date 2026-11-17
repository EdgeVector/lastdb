//! Sessions for apps on the app socket. A session names one installed app and
//! the schemas and extensions it declared. Each app holds a few live sessions.
//! A new session replaces that app's oldest once that number is full.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};

/// An app can run for a long time, so its session outlives a database session.
/// The launcher asks for a new one at each start.
pub(crate) const APP_SESSION_LIFETIME: Duration = Duration::from_secs(24 * 3600);
const MAX_APP_SESSIONS: usize = 1024;

/// An app can have a few processes running at once, for example a command
/// and a background process.
const MAX_SESSIONS_PER_APP: usize = 4;

/// What one app may do on the app socket: exactly what it declared.
#[derive(Debug)]
pub(crate) struct AppGrant {
    pub(crate) app: String,
    pub(crate) read: BTreeSet<String>,
    pub(crate) write: BTreeSet<String>,
    pub(crate) extensions: BTreeSet<String>,
    expires: Instant,
}

impl AppGrant {
    pub(crate) fn new(
        app: String,
        read: BTreeSet<String>,
        write: BTreeSet<String>,
        extensions: BTreeSet<String>,
    ) -> Self {
        Self {
            app,
            read,
            write,
            extensions,
            expires: Instant::now() + APP_SESSION_LIFETIME,
        }
    }
}

struct Held {
    grant: Arc<AppGrant>,
    order: u64,
}

#[derive(Default)]
struct Store {
    sessions: HashMap<[u8; 32], Held>,
    next_order: u64,
}

impl Store {
    fn drop_expired(&mut self, now: Instant) {
        self.sessions.retain(|_, held| held.grant.expires > now);
    }

    /// Drop this app's oldest live session when it already holds the per-app limit.
    fn drop_oldest_if_full(&mut self, app: &str) {
        let live = self
            .sessions
            .values()
            .filter(|held| held.grant.app == app)
            .count();
        if live < MAX_SESSIONS_PER_APP {
            return;
        }
        let oldest = self
            .sessions
            .iter()
            .filter(|(_, held)| held.grant.app == app)
            .min_by_key(|(_, held)| held.order)
            .map(|(key, _)| *key);
        let Some(key) = oldest else {
            return;
        };
        self.sessions.remove(&key);
    }
}

/// Live app sessions, keyed by the hash of the token. Nothing is stored on disk.
#[derive(Default)]
pub(crate) struct AppSessions(Mutex<Store>);

impl AppSessions {
    /// A new token for `grant`, or `None` when the node holds too many sessions.
    pub(crate) fn issue(&self, grant: AppGrant) -> Option<String> {
        let mut store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        store.drop_expired(Instant::now());
        // Free this app's oldest slot before the total cap, so one app cannot fill the store.
        store.drop_oldest_if_full(&grant.app);
        if store.sessions.len() >= MAX_APP_SESSIONS {
            return None;
        }
        let mut random = [0u8; 32];
        OsRng.fill_bytes(&mut random);
        let token = fold_db::hex::hex_lower(random);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let order = store.next_order;
        store.next_order = store.next_order.wrapping_add(1);
        store.sessions.insert(
            key,
            Held {
                grant: Arc::new(grant),
                order,
            },
        );
        Some(token)
    }

    pub(crate) fn get(&self, token: &str) -> Option<Arc<AppGrant>> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let held = store.sessions.get(&key)?;
        (held.grant.expires > Instant::now()).then(|| Arc::clone(&held.grant))
    }
}
