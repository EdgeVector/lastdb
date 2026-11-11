//! Local mutations waiting for an idempotent cloud append.

use std::fs;
use std::path::{Path, PathBuf};

use fold_db::schema::types::Mutation;
use serde::{Deserialize, Serialize};

use super::cutover;
use super::disk;
use super::payload;
use super::registry;

const FILE_LIMIT: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OutboxNote {
    pub namespace: String,
    pub payload: Vec<u8>,
    pub authored_at_ms: u64,
    pub accepted_at_ms: u64,
    pub mutation_id: String,
}

/// Clone log payloads only after Exemem has confirmed signed-only for this home.
pub fn capture_mutations(home: &Path, mutations: &[Mutation]) -> Option<Vec<OutboxNote>> {
    let owner = registry::owner(home)?;
    let commit = cutover::remembered(home)?;
    if commit.database_id != owner.id {
        return None;
    }
    let now = fold_db::clock::unix_millis();
    let notes = mutations
        .iter()
        .filter_map(|mutation| note_of(mutation, now))
        .collect();
    Some(notes)
}

pub fn store_outbox(home: &Path, notes: Option<Vec<OutboxNote>>) {
    let Some(notes) = notes else {
        return;
    };
    let Some(owner) = registry::owner(home) else {
        return;
    };
    for note in notes {
        let path = outbox_dir(home, &owner.id).join(safe_name(&note.mutation_id));
        match serde_json::to_vec(&note) {
            Ok(bytes) if bytes.len() <= FILE_LIMIT => {
                if let Err(error) = disk::write_private(&path, &bytes) {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        error = %error,
                        "signed-only mutation was committed locally and not queued"
                    );
                }
            }
            _ => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    "signed-only mutation was committed locally and not queued"
                );
            }
        }
    }
}

pub(crate) fn pending(
    home: &Path,
    database_id: &str,
) -> Result<Vec<(PathBuf, OutboxNote)>, String> {
    let directory = outbox_dir(home, database_id);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("cannot read the log outbox".into()),
    };
    let mut notes = Vec::new();
    for entry in entries {
        let path = entry.map_err(|_| "cannot read the log outbox")?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(bytes) =
            disk::read_limited(&path, FILE_LIMIT).map_err(|_| "cannot read outbox")?
        else {
            continue;
        };
        let note: OutboxNote =
            serde_json::from_slice(&bytes).map_err(|_| "outbox item is invalid")?;
        notes.push((path, note));
    }
    notes.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(notes)
}

pub(crate) fn remove(path: &Path) -> Result<(), String> {
    disk::remove_file(path).map_err(|_| "cannot remove a queued mutation".to_owned())
}

fn note_of(mutation: &Mutation, now: u64) -> Option<OutboxNote> {
    let (namespace, payload) = payload::from_mutation(mutation)?;
    Some(OutboxNote {
        namespace: namespace.as_str().to_owned(),
        payload,
        authored_at_ms: now,
        accepted_at_ms: now,
        mutation_id: mutation.uuid.clone(),
    })
}

fn outbox_dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("outbox")
}

fn safe_name(mutation_id: &str) -> String {
    let cleaned: String = mutation_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    format!("{cleaned}.json")
}
