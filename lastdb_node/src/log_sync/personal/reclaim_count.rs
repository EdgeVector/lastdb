//! The count of what the sweeps of this node released, from what the node can observe itself.
//!
//! One `blob.drop` is one step on the relay. When its answer names no absent digest, the relay
//! dropped every digest of the call in that step, and the answer is the count. Otherwise the
//! answer cannot say who dropped the absent ones. The client asks again by itself after an
//! answer that was lost or said to try again, and when the first try had already dropped the
//! files, the second finds them absent and reports nothing. So a call is written down before it
//! is sent (`reclaim-named.json`: each digest with the length the listing gave it). It is taken
//! off again when the relay answered that it dropped all of them, or that it dropped nothing
//! because a stream was not as named. What is left is settled by the next `blob.list`: a digest
//! this node named that the relay no longer lists was released, with the length the earlier
//! listing gave it. That also covers a call with no answer at all, and a kill in the middle.

use std::collections::BTreeMap;
use std::path::PathBuf;

use super::snapshot_state::file_of;
use super::Ctx;
use crate::log_sync::blob_transfer::Dropped;
use crate::log_sync::relay::RelayClient;
use crate::profile::private_fs as disk;

const NAMED: &str = "reclaim-named.json";
const LIMIT: usize = 1024 * 1024;

/// The digests whose release is not counted yet, and what this sweep counted so far.
pub(super) struct Counter<'a> {
    pub(super) client: &'a RelayClient,
    database_id: &'a str,
    path: PathBuf,
    /// Digests a drop named, with their listed length, until their release is settled.
    named: BTreeMap<String, u64>,
    /// Files this sweep counted as released.
    pub(super) files: u64,
    /// Their length.
    pub(super) bytes: u64,
}

impl<'a> Counter<'a> {
    pub(super) fn open(ctx: &'a Ctx<'_>, client: &'a RelayClient) -> Result<Self, String> {
        let path = file_of(&ctx.host.home, &ctx.database_id, NAMED);
        let named = match disk::read_limited(&path, LIMIT) {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes)
                .map_err(|_| "the record of named drops is invalid".to_owned())?,
            Ok(None) => BTreeMap::new(),
            Err(_) => return Err("cannot read the record of named drops".into()),
        };
        Ok(Self {
            client,
            database_id: &ctx.database_id,
            path,
            named,
            files: 0,
            bytes: 0,
        })
    }

    /// A new listing of the relay: every digest that waits and is no longer listed was released.
    pub(super) fn settle(&mut self, listed: &BTreeMap<String, u64>) -> Result<(), String> {
        if self.named.is_empty() {
            return Ok(());
        }
        let gone: Vec<u64> = self
            .named
            .iter()
            .filter(|(digest, _)| !listed.contains_key(digest.as_str()))
            .map(|(_, bytes)| *bytes)
            .collect();
        self.count(gone.len() as u64, gone.iter().sum())?;
        self.named.clear();
        self.save()
    }

    /// Write down the digests of a drop before it is sent.
    pub(super) fn naming(
        &mut self,
        batch: &[String],
        listed: &BTreeMap<String, u64>,
    ) -> Result<(), String> {
        for digest in batch {
            let bytes = listed.get(digest).copied().unwrap_or(0);
            self.named.insert(digest.clone(), bytes);
        }
        self.save()
    }

    /// The relay answered the drop of `batch`: `None` when it refused it and dropped nothing.
    pub(super) fn answered(
        &mut self,
        batch: &[String],
        answer: Option<Dropped>,
    ) -> Result<(), String> {
        match answer {
            // Some were gone before this try. The next listing says what was released.
            Some(dropped) if dropped.absent > 0 => return Ok(()),
            Some(dropped) => self.count(dropped.dropped, dropped.released_bytes)?,
            None => {}
        }
        for digest in batch {
            self.named.remove(digest);
        }
        self.save()
    }

    fn count(&mut self, files: u64, bytes: u64) -> Result<(), String> {
        self.files += files;
        self.bytes += bytes;
        match &self.client.usage {
            Some(usage) => usage.blobs_released(self.database_id, files, bytes),
            None => Ok(()),
        }
    }

    fn save(&self) -> Result<(), String> {
        let saved = if self.named.is_empty() {
            disk::remove_private(&self.path)
        } else {
            let bytes = serde_json::to_vec(&self.named)
                .map_err(|_| "cannot encode the record of named drops".to_owned())?;
            disk::write_private(&self.path, &bytes)
        };
        saved.map_err(|_| "cannot save the record of named drops".to_owned())
    }
}
