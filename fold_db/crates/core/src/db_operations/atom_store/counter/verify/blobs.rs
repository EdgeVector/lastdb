//! The blob checks of the verifier.
//!
//! The blob counters are global: one counter for each blob, whatever the scope
//! of the atoms that list it. A run reads them in three places. All of them read
//! at one committed root for each page, so the invariants of a batch hold and no
//! check here has a false positive.
//!
//! - **Listed blobs.** The atoms phase and the tips phase read the blob counter of
//!   every blob that the page's atom counters or tips name. A counter that is
//!   absent, or below the number of holders in the page, is a fault: the true
//!   count can only be higher (holders in other pages and other scopes add to
//!   it). A count that is too high is not found here, because the totals of one
//!   scope say nothing about the other scopes. `lastdb db recount` computes it.
//! - **The counters** (phase `blobs`, personal run only). Each counter is
//!   checked alone: `suspect`, a count of zero with no dwell, a dwell with
//!   references, a dwell with no queue entry.
//! - **The queue** (phase `blob_queue`, personal run only). Each entry must have
//!   a counter at zero with the same `zero_since`.
//!
//! Nothing here writes.

use super::check::Finding;
use super::pages::Request;
use super::report::Kind;
use super::state::Run;
use crate::crypto::count_unsealed_discards;
use crate::db_operations::atom_store::blob_counter::{
    blob_counter_key, blob_counter_key_of_id, blob_id, blob_queue_key, blobs_of_tip_row,
    parse_blob_queue_key, BlobCounter,
};
use crate::db_operations::atom_store::counter::scan::{CounterRead, PairRead};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// What a read found for the counter record of a blob.
pub(super) enum BlobRead {
    Absent,
    /// The record exists and does not decode.
    Unreadable,
    Present(BlobCounter),
}

impl BlobRead {
    fn parse(bytes: Option<&[u8]>) -> Self {
        match bytes {
            None => Self::Absent,
            Some(bytes) => serde_json::from_slice(bytes).map_or(Self::Unreadable, Self::Present),
        }
    }
}

/// A blob that holders of one page name, with the counter read at the same root.
pub(super) struct BlobListed {
    pub blob: String,
    /// Counters or tips of the page that name the blob.
    pub listed: u32,
    pub counter: BlobRead,
}

/// One counter of the blobs phase.
pub(super) struct BlobEntry {
    pub id: String,
    pub counter: BlobRead,
    /// The queue holds the entry at the `zero_since` of the counter.
    pub queued: bool,
}

/// A page of the blobs phase.
pub(super) struct BlobsPage {
    pub rows: usize,
    pub last_key: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub point_unsealed: u64,
    pub entries: Vec<BlobEntry>,
}

/// One entry of the queue, with the counter it names.
pub(super) struct QueueEntry {
    pub zero_since: u64,
    pub id: String,
    pub counter: BlobRead,
}

/// A page of the queue phase.
pub(super) struct QueuePage {
    pub rows: usize,
    pub last_key: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub point_unsealed: u64,
    pub entries: Vec<QueueEntry>,
}

fn finding(kind: Kind, detail: String) -> Finding {
    Finding { kind, detail }
}

/// Check a blob that `listed` holders of one page name. `holder` says which
/// kind of holder the page has.
pub(super) fn judge_listed(listed: u32, counter: &BlobRead, holder: &str) -> Vec<Finding> {
    match counter {
        BlobRead::Absent => vec![finding(
            Kind::BlobCounterMissing,
            format!("{listed} {holder} of the page name the blob and it has no counter"),
        )],
        BlobRead::Unreadable => vec![finding(
            Kind::UnreadableBlobCounter,
            "blob counter does not decode".into(),
        )],
        BlobRead::Present(counter) if !counter.suspect && counter.refs < u64::from(listed) => {
            vec![finding(
                Kind::BlobCounterBelowRefs,
                format!(
                    "counter holds {} and the page shows {listed} {holder}",
                    counter.refs
                ),
            )]
        }
        BlobRead::Present(_) => Vec::new(),
    }
}

/// Check one counter alone. `queued` says that the queue holds its entry.
pub(super) fn judge_counter(counter: &BlobRead, queued: bool) -> Vec<Finding> {
    let counter = match counter {
        BlobRead::Absent => return Vec::new(),
        BlobRead::Unreadable => {
            return vec![finding(
                Kind::UnreadableBlobCounter,
                "blob counter does not decode".into(),
            )]
        }
        BlobRead::Present(counter) => counter,
    };
    let mut out = Vec::new();
    if counter.suspect {
        out.push(finding(
            Kind::BlobSuspect,
            format!("counter holds {} and is marked suspect", counter.refs),
        ));
    } else if counter.refs == 0 && counter.zero_since.is_none() {
        out.push(finding(
            Kind::BlobZeroWithoutDwell,
            "counter holds zero and has no zero_since".into(),
        ));
    }
    if counter.refs > 0 && counter.zero_since.is_some() {
        out.push(finding(
            Kind::BlobDwellWithRefs,
            format!("counter holds {} and has a zero_since", counter.refs),
        ));
    }
    if counter.zero_since.is_some() && !queued {
        out.push(finding(
            Kind::BlobUnqueued,
            "counter has a zero_since and the queue has no entry for it".into(),
        ));
    }
    out
}

/// Check one queue entry against the counter it names.
pub(super) fn judge_queue_entry(zero_since: u64, counter: &BlobRead) -> Option<Finding> {
    let detail = match counter {
        BlobRead::Absent => "the entry names a blob with no counter".to_string(),
        BlobRead::Unreadable => "the entry names a counter that does not decode".to_string(),
        BlobRead::Present(counter)
            if counter.zero_since == Some(zero_since) && counter.refs == 0 && !counter.suspect =>
        {
            return None
        }
        BlobRead::Present(counter) => format!(
            "the entry is at {zero_since} and the counter holds {} with zero_since {:?}{}",
            counter.refs,
            counter.zero_since,
            if counter.suspect { " (suspect)" } else { "" }
        ),
    };
    Some(finding(Kind::BlobStaleQueueEntry, detail))
}

/// The blobs that the atom counters of a page list, with the number of counters
/// that list each.
pub(super) fn blobs_of_pairs(pairs: &[PairRead]) -> BTreeMap<String, u32> {
    let mut out = BTreeMap::new();
    for pair in pairs {
        if let CounterRead::Present(counter) = &pair.counter {
            for blob in &counter.blobs {
                *out.entry(blob.clone()).or_insert(0u32) += 1;
            }
        }
    }
    out
}

/// The blobs that the tips of a page name in their metadata, with the number of
/// tips that name each. A row that does not decode is reported by the tip read.
pub(super) fn blobs_of_tips(rows: &[(Vec<u8>, Vec<u8>)]) -> BTreeMap<String, u32> {
    let mut out = BTreeMap::new();
    for (_, value) in rows {
        for blob in blobs_of_tip_row(value).unwrap_or_default() {
            *out.entry(blob).or_insert(0u32) += 1;
        }
    }
    out
}

/// The counter id and the counter key of a blob counter row, when the key has the
/// current shape.
fn id_of_counter_key(key: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(key).ok()?;
    let id = text.strip_prefix("bref:v3:c:")?.strip_suffix("\0c")?;
    (id.len() == 64).then(|| id.to_string())
}

impl AtomStore {
    async fn read_blob_counter_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.raw()
            .inner()
            .get_many(keys)
            .await
            .map_err(SchemaError::from)
    }

    /// Read the counters of the blobs that the holders of a page name. Call it
    /// inside the read scope of the page.
    pub(super) async fn read_blob_listed(
        &self,
        lists: BTreeMap<String, u32>,
    ) -> Result<Vec<BlobListed>, SchemaError> {
        let keys = lists
            .keys()
            .map(|blob| blob_counter_key(blob).into_bytes())
            .collect();
        let counters = self.read_blob_counter_bytes(keys).await?;
        Ok(lists
            .into_iter()
            .zip(counters)
            .map(|((blob, listed), bytes)| BlobListed {
                blob,
                listed,
                counter: BlobRead::parse(bytes.as_deref()),
            })
            .collect())
    }

    /// One page of blob counters, and whether the queue holds the entry of each.
    pub(super) async fn read_blobs_page(
        &self,
        (start, end, asked): Request,
    ) -> Result<BlobsPage, SchemaError> {
        self.atom_read_scope(async {
            let (rows, scan_unsealed) =
                count_unsealed_discards(self.raw().inner().scan_range_paged(&start, &end, asked))
                    .await;
            let rows = rows.map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let counters: Vec<BlobRead> = rows
                .iter()
                .map(|(_, value)| BlobRead::parse(Some(value)))
                .collect();
            let queue_keys: Vec<Vec<u8>> = counters
                .iter()
                .filter_map(|counter| match counter {
                    BlobRead::Present(counter) => counter
                        .zero_since
                        .map(|at| blob_queue_key(at, &counter.blob).into_bytes()),
                    _ => None,
                })
                .collect();
            let (found, point_unsealed) = count_unsealed_discards(async {
                if queue_keys.is_empty() {
                    return Ok(Vec::new());
                }
                self.raw()
                    .inner()
                    .exists_many(queue_keys)
                    .await
                    .map_err(SchemaError::from)
            })
            .await;
            let mut found = found?.into_iter();
            let mut entries = Vec::with_capacity(rows.len());
            for ((key, _), counter) in rows.iter().zip(counters) {
                let queued = match &counter {
                    BlobRead::Present(c) if c.zero_since.is_some() => found.next().unwrap_or(false),
                    _ => false,
                };
                entries.push(BlobEntry {
                    id: id_of_counter_key(key).unwrap_or_default(),
                    counter,
                    queued,
                });
            }
            Ok(BlobsPage {
                rows: rows.len(),
                last_key,
                scan_unsealed,
                point_unsealed,
                entries,
            })
        })
        .await
    }

    /// One page of queue entries, and the counter each names.
    pub(super) async fn read_queue_page(
        &self,
        (start, end, asked): Request,
    ) -> Result<QueuePage, SchemaError> {
        self.atom_read_scope(async {
            let (rows, scan_unsealed) =
                count_unsealed_discards(self.raw().inner().scan_range_paged(&start, &end, asked))
                    .await;
            let rows = rows.map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let parsed: Vec<(u64, String)> = rows
                .iter()
                .filter_map(|(key, _)| parse_blob_queue_key(key))
                .collect();
            let keys = parsed
                .iter()
                .map(|(_, id)| blob_counter_key_of_id(id).into_bytes())
                .collect();
            let (bytes, point_unsealed) =
                count_unsealed_discards(self.read_blob_counter_bytes(keys)).await;
            let bytes = bytes?;
            let entries = parsed
                .into_iter()
                .zip(bytes)
                .map(|((zero_since, id), bytes)| QueueEntry {
                    zero_since,
                    id,
                    counter: BlobRead::parse(bytes.as_deref()),
                })
                .collect();
            Ok(QueuePage {
                rows: rows.len(),
                last_key,
                scan_unsealed,
                point_unsealed,
                entries,
            })
        })
        .await
    }
}

/// Judge the blobs that a page's holders name.
pub(super) fn apply_listed(run: &mut Run, listed: Vec<BlobListed>, holder: &str) {
    for blob in listed {
        for finding in judge_listed(blob.listed, &blob.counter, holder) {
            run.note_fault(finding.kind, &blob.blob, finding.detail);
        }
    }
}

/// Judge a page of blob counters.
pub(super) fn apply_blobs(run: &mut Run, page: BlobsPage) {
    run.report.pages += 1;
    for entry in page.entries {
        if !matches!(entry.counter, BlobRead::Absent) {
            run.report.blob_counters_read += 1;
        }
        if let BlobRead::Present(counter) = &entry.counter {
            if blob_id(&counter.blob) != entry.id {
                run.note_fault(
                    Kind::UnreadableBlobCounter,
                    &entry.id,
                    "the counter names a blob that does not match its key".into(),
                );
            }
        }
        let label = match &entry.counter {
            BlobRead::Present(counter) => counter.blob.clone(),
            _ => entry.id.clone(),
        };
        for finding in judge_counter(&entry.counter, entry.queued) {
            run.note_fault(finding.kind, &label, finding.detail);
        }
    }
}

/// Judge a page of queue entries.
pub(super) fn apply_queue(run: &mut Run, page: QueuePage) {
    run.report.pages += 1;
    run.report.blob_queue_read += page.entries.len() as u64;
    for entry in page.entries {
        if let Some(finding) = judge_queue_entry(entry.zero_since, &entry.counter) {
            run.note_fault(finding.kind, &entry.id, finding.detail);
        }
    }
}
