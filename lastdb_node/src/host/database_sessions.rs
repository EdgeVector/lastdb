//! In-memory grants issued by the owner for one named database.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use cloud_backup_protocol::Id;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};

pub(crate) const LIFETIME: Duration = Duration::from_secs(3600);
const MAX_SESSIONS: usize = 1024;

#[derive(Default)]
pub(crate) struct DatabaseSessions(Mutex<HashMap<[u8; 32], Grant>>);

struct Grant {
    id: Id,
    write: bool,
    expires: Instant,
}

impl DatabaseSessions {
    pub(crate) fn issue(&self, id: Id, write: bool) -> Option<String> {
        let mut sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        sessions.retain(|_, grant| grant.expires > now);
        if sessions.len() >= MAX_SESSIONS {
            return None;
        }
        let mut random = [0u8; 32];
        OsRng.fill_bytes(&mut random);
        let token = hex::encode(random);
        sessions.insert(
            token_hash(&token),
            Grant {
                id,
                write,
                expires: now + LIFETIME,
            },
        );
        Some(token)
    }

    pub(crate) fn get(&self, token: &str) -> Option<(Id, bool)> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let grant = sessions.get(&token_hash(token))?;
        (grant.expires > Instant::now()).then(|| (grant.id.clone(), grant.write))
    }
}

fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}
