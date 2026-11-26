//! The count of what the sweeps of this node released, from what the node can observe itself.
//!
//! One `blob.drop` is one step on the relay. When its answer names no absent digest, the relay
//! dropped every digest of the call in that step, and the answer is the count. Otherwise the
//! answer cannot say who dropped the absent ones. The client asks again by itself after an
//! answer that was lost or said to try again, and when the first try had already dropped the
//! files, the second finds them absent and reports nothing. So a call is written down before it
//! is sent (`reclaim-named.json`: each digest with the length the listing gave it, beside the
//! running totals of an unfinished multi-turn sweep). A digest is taken off again when the relay
//! answered that it dropped all of them, or that it dropped nothing because a stream was not as
//! named. What is left is settled by the next `blob.list`: a digest this node named that the
//! relay no longer lists was released, with the length the earlier listing gave it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::snapshot_state::file_of;
use super::Ctx;
use crate::log_sync::blob_transfer::Dropped;
use crate::log_sync::relay::RelayClient;
use crate::profile::private_fs as disk;

const NAMED: &str = "reclaim-named.json";
const LIMIT: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedCount {
    named: BTreeMap<String, u64>,
    #[serde(default)]
    files: u64,
    #[serde(default)]
    bytes: u64,
}

#[derive(Serialize)]
struct SavedCountRef<'a> {
    named: &'a BTreeMap<String, u64>,
    files: u64,
    bytes: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredCount {
    Current(SavedCount),
    Legacy(BTreeMap<String, u64>),
}

fn decode_stored(bytes: &[u8]) -> Result<(BTreeMap<String, u64>, u64, u64), String> {
    let parsed: StoredCount = serde_json::from_slice(bytes)
        .map_err(|_| "the record of named drops is invalid".to_owned())?;
    Ok(match parsed {
        StoredCount::Current(saved) => (saved.named, saved.files, saved.bytes),
        StoredCount::Legacy(named) => (named, 0, 0),
    })
}

/// The digests whose release is not counted yet, and what this sweep counted so far.
pub(super) struct Counter<'a> {
    pub(super) client: &'a RelayClient,
    database_id: &'a str,
    path: PathBuf,
    /// Digests a drop named, with their listed length, until their release is settled.
    named: BTreeMap<String, u64>,
    /// Files this sweep counted as released across its turns.
    pub(super) files: u64,
    /// Their length.
    pub(super) bytes: u64,
}

impl<'a> Counter<'a> {
    pub(super) fn open(ctx: &'a Ctx<'_>, client: &'a RelayClient) -> Result<Self, String> {
        let path = file_of(&ctx.host.home, &ctx.database_id, NAMED);
        let (named, files, bytes) = match disk::read_limited(&path, LIMIT) {
            Ok(Some(raw)) => decode_stored(&raw)?,
            Ok(None) => (BTreeMap::new(), 0, 0),
            Err(_) => return Err("cannot read the record of named drops".into()),
        };
        Ok(Self {
            client,
            database_id: &ctx.database_id,
            path,
            named,
            files,
            bytes,
        })
    }

    /// True when no named digests wait to be settled against `blob.list`.
    pub(super) fn is_empty(&self) -> bool {
        self.named.is_empty()
    }

    /// A new listing of the relay: every digest that waits and is no longer listed was released.
    /// First, a blob whose commit this node never saw end and that the relay lists was committed,
    /// so it counts as put before this sweep can drop it.
    pub(super) fn settle(&mut self, listed: &BTreeMap<String, u64>) -> Result<(), String> {
        if let Some(usage) = &self.client.usage {
            usage.confirm_listed(self.database_id, listed)?;
        }
        if self.named.is_empty() {
            return Ok(());
        }
        let (gone_files, gone_bytes) = self
            .named
            .iter()
            .filter(|(digest, _)| !listed.contains_key(digest.as_str()))
            .fold((0u64, 0u64), |(files, total), (_, bytes)| {
                (files.saturating_add(1), total.saturating_add(*bytes))
            });
        self.count(gone_files, gone_bytes)?;
        self.named.clear();
        self.save()
    }

    /// Write down the digests of a drop before it is sent, returning whether any digest in `batch`
    /// was already pending from an earlier unfinished attempt on this node.
    pub(super) fn naming(
        &mut self,
        batch: &[String],
        listed: &BTreeMap<String, u64>,
    ) -> Result<bool, String> {
        let had_prior = batch.iter().any(|digest| self.named.contains_key(digest));
        for digest in batch {
            let bytes = listed.get(digest).copied().unwrap_or(0);
            self.named.insert(digest.clone(), bytes);
        }
        self.save()?;
        Ok(had_prior)
    }

    /// The relay answered the drop of `batch`: `None` when it refused it and dropped nothing.
    pub(super) fn answered(
        &mut self,
        batch: &[String],
        answer: Option<(Dropped, bool)>,
        had_prior: bool,
    ) -> Result<(), String> {
        match answer {
            // A retry or an earlier unfinished turn of this node may have dropped the absent ones;
            // wait for the next `blob.list` to settle what went.
            Some((dropped, retried)) if dropped.absent > 0 && (retried || had_prior) => {
                return Ok(());
            }
            Some((dropped, _)) => self.count(dropped.dropped, dropped.released_bytes)?,
            None => {}
        }
        for digest in batch {
            self.named.remove(digest);
        }
        self.save()
    }

    /// Clear cumulative sweep totals once the sweep finishes (`Clean` or `NotDue`).
    pub(super) fn finish(&mut self) -> Result<(), String> {
        self.files = 0;
        self.bytes = 0;
        self.save()
    }

    /// Clear cumulative sweep totals when a sweep stops, keeping any unsettled `named` digests.
    pub(super) fn reset_totals(home: &Path, database_id: &str) {
        let path = file_of(home, database_id, NAMED);
        let Ok(Some(raw)) = disk::read_limited(&path, LIMIT) else {
            return;
        };
        let Ok((named, _, _)) = decode_stored(&raw) else {
            return;
        };
        if named.is_empty() {
            let _ = disk::remove_private(&path);
        } else if let Ok(bytes) = serde_json::to_vec(&SavedCountRef {
            named: &named,
            files: 0,
            bytes: 0,
        }) {
            let _ = disk::write_private(&path, &bytes);
        }
    }

    fn count(&mut self, files: u64, bytes: u64) -> Result<(), String> {
        self.files = self.files.saturating_add(files);
        self.bytes = self.bytes.saturating_add(bytes);
        match &self.client.usage {
            Some(usage) => usage.blobs_released(self.database_id, files, bytes),
            None => Ok(()),
        }
    }

    fn save(&self) -> Result<(), String> {
        let saved = if self.named.is_empty() && self.files == 0 && self.bytes == 0 {
            disk::remove_private(&self.path)
        } else {
            let bytes = serde_json::to_vec(&SavedCountRef {
                named: &self.named,
                files: self.files,
                bytes: self.bytes,
            })
            .map_err(|_| "cannot encode the record of named drops".to_owned())?;
            disk::write_private(&self.path, &bytes)
        };
        saved.map_err(|_| "cannot save the record of named drops".to_owned())
    }
}
