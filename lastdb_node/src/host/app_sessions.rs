//! Sessions for apps on the app socket. A session names one installed app and
//! the schemas and extensions it declared.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};

/// An app can run for a long time, so its session outlives a database session.
/// The launcher asks for a new one at each start.
pub(crate) const APP_SESSION_LIFETIME: Duration = Duration::from_secs(24 * 3600);
const MAX_APP_SESSIONS: usize = 1024;

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

/// Live app sessions, keyed by the hash of the token. Nothing is stored on disk.
#[derive(Default)]
pub(crate) struct AppSessions(Mutex<HashMap<[u8; 32], Arc<AppGrant>>>);

impl AppSessions {
    /// A new token for `grant`, or `None` when the node holds too many sessions.
    pub(crate) fn issue(&self, grant: AppGrant) -> Option<String> {
        let mut sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        sessions.retain(|_, held| held.expires > now);
        if sessions.len() >= MAX_APP_SESSIONS {
            return None;
        }
        let mut random = [0u8; 32];
        OsRng.fill_bytes(&mut random);
        let token = fold_db::hex::hex_lower(random);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        sessions.insert(key, Arc::new(grant));
        Some(token)
    }

    pub(crate) fn get(&self, token: &str) -> Option<Arc<AppGrant>> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let grant = sessions.get(&key)?;
        (grant.expires > Instant::now()).then(|| Arc::clone(grant))
    }
}
