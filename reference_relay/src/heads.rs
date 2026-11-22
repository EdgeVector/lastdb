//! In-memory contiguous head of every stored device stream.
//!
//! `log/streams` must answer without reading entry files, so the relay keeps
//! one small record per `(database, writer, device)` stream. The map is built
//! once when the store opens (one pass over the stored entries) and then kept
//! current by every append that stores a new entry. Nothing here is persisted:
//! the entry files are the only durable state.
//!
//! The relay does not require appends to arrive gapless, so a stream can hold
//! seq 1, 2 and 5. The head is the highest seq such that every seq from 1 up to
//! it is stored (here 2). Entries beyond a gap wait in `pending` and join the
//! head when the gap is filled. A reader that reads from seq 1 can never get
//! past the gap, so the head is also the last entry such a read returns.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mutation_log::{entry_hash, SignedEntry};
use tokio::fs;

use crate::error::{ApiError, ApiResult};

/// One stream's contiguous head. `seq` 0 means seq 1 is not stored yet.
#[derive(Default)]
struct StreamHead {
    seq: u64,
    hash: String,
    /// Stored entries beyond `seq + 1`, keyed by seq, holding their hash.
    pending: BTreeMap<u64, String>,
}

impl StreamHead {
    fn record(&mut self, seq: u64, hash: String) {
        if seq <= self.seq {
            return;
        }
        self.pending.insert(seq, hash);
        while let Some(next) = self.pending.remove(&(self.seq + 1)) {
            self.seq += 1;
            self.hash = next;
        }
    }
}

/// One line of the `log/streams` answer.
pub(crate) struct ListedStream {
    pub(crate) device_id: String,
    pub(crate) head_seq: u64,
    pub(crate) head_hash: String,
}

type WriterKey = (String, String);
type Devices = BTreeMap<String, StreamHead>;

/// Shared, cheaply cloned handle on the head map.
#[derive(Clone, Default)]
pub(crate) struct Heads {
    streams: Arc<Mutex<BTreeMap<WriterKey, Devices>>>,
}

impl Heads {
    /// Fold one newly stored entry into the head of its stream.
    pub(crate) fn record(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: &str,
        seq: u64,
        hash: String,
    ) {
        let mut streams = self.streams.lock().expect("heads lock");
        streams
            .entry((database_id.to_owned(), writer_key.to_owned()))
            .or_default()
            .entry(device_id.to_owned())
            .or_default()
            .record(seq, hash);
    }

    /// The streams of one writer that hold seq 1, sorted by device id.
    pub(crate) fn list(&self, database_id: &str, writer_key: &str) -> Vec<ListedStream> {
        let streams = self.streams.lock().expect("heads lock");
        let key = (database_id.to_owned(), writer_key.to_owned());
        streams.get(&key).map_or_else(Vec::new, |devices| {
            devices
                .iter()
                .filter(|(_, head)| head.seq >= 1)
                .map(|(device_id, head)| ListedStream {
                    device_id: device_id.clone(),
                    head_seq: head.seq,
                    head_hash: head.hash.clone(),
                })
                .collect()
        })
    }

    /// Build the map from every entry file under `<root>/logs`.
    pub(crate) async fn scan(logs: &Path) -> Self {
        let heads = Self::default();
        for db_dir in subdirs(logs).await {
            for writer_dir in subdirs(&db_dir).await {
                for device_dir in subdirs(&writer_dir).await {
                    heads.scan_device(&db_dir, &writer_dir, &device_dir).await;
                }
            }
        }
        heads
    }

    async fn scan_device(&self, db_dir: &Path, writer_dir: &Path, device_dir: &Path) {
        let (Some(db), Some(writer), Some(device)) =
            (name(db_dir), name(writer_dir), name(device_dir))
        else {
            return;
        };
        let Ok(mut files) = fs::read_dir(device_dir).await else {
            return;
        };
        while let Ok(Some(file)) = files.next_entry().await {
            let path = file.path();
            let seq = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".json"))
                .and_then(|n| n.parse::<u64>().ok());
            let Some(seq) = seq else { continue };
            let Ok(bytes) = fs::read(&path).await else {
                continue;
            };
            let Ok(entry) = serde_json::from_slice::<SignedEntry>(&bytes) else {
                continue;
            };
            if let Ok(hash) = entry_hash(&entry) {
                self.record(&db, &writer, &device, seq, hash.to_hex());
            }
        }
    }
}

/// The entry hash of an opaque stored entry, as 64 lowercase hex characters.
pub(crate) fn hash_of(entry: &serde_json::Value) -> ApiResult<String> {
    let signed: SignedEntry =
        serde_json::from_value(entry.clone()).map_err(|_| ApiError::invalid("entries"))?;
    entry_hash(&signed)
        .map(|hash| hash.to_hex())
        .map_err(|_| ApiError::invalid("entries"))
}

fn name(path: &Path) -> Option<String> {
    path.file_name()?.to_str().map(str::to_owned)
}

async fn subdirs(path: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(mut rd) = fs::read_dir(path).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            if ent.file_type().await.is_ok_and(|t| t.is_dir()) {
                out.push(ent.path());
            }
        }
    }
    out
}
