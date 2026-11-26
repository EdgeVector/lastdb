//! What this run created on the relay, recorded as it happens so the cleanup can find it after
//! any step fails.
//!
//! Only databases the run mints are recorded: each id comes from a fresh random database key, and
//! its publisher is a node key the run generated. The cleanup acts on nothing else.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use mutation_log::DatabaseId;

use crate::identity::{Actor, DbBirth};

/// One database this run minted.
pub struct OwnedDatabase {
    /// The publisher: the only node key that `blob/drop` accepts for the database.
    pub publisher: Actor,
    pub database_id: DatabaseId,
    /// Digest (hex) and length of each blob this run committed with `blob.put`.
    pub blobs: Vec<(String, u64)>,
}

/// The databases this run minted, shared by the steps that write to the relay.
#[derive(Clone, Default)]
pub struct Ledger(Arc<Mutex<Vec<OwnedDatabase>>>);

impl Ledger {
    /// Mint a database for `publisher` and record it before its first entry is appended.
    pub fn mint(&self, publisher: &Actor) -> DbBirth {
        let birth = DbBirth::generate(publisher.node_key);
        self.lock().push(OwnedDatabase {
            publisher: publisher.clone(),
            database_id: birth.database_id,
            blobs: Vec::new(),
        });
        birth
    }

    /// Record a blob of the database `database_hex` once `blob.put` committed it.
    pub fn blob(&self, database_hex: &str, sha256: &str, bytes: u64) {
        if let Some(owned) = self
            .lock()
            .iter_mut()
            .find(|owned| owned.database_id.to_hex() == database_hex)
        {
            owned.blobs.push((sha256.to_owned(), bytes));
        }
    }

    /// Everything recorded so far; the ledger is empty afterwards.
    pub fn take(&self) -> Vec<OwnedDatabase> {
        std::mem::take(&mut *self.lock())
    }

    fn lock(&self) -> MutexGuard<'_, Vec<OwnedDatabase>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
