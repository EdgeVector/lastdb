//! `blob.list` and `blob.drop`: what the relay holds of a database, and the drop of the stored
//! files that nothing names.

use std::collections::BTreeMap;

use relay_protocol::{
    BlobDrop, BlobDropRequest, BlobList, BlobListAnswer, BlobListRequest, BlobListed, ErrorCode,
};

use crate::log_sync::relay::{CallError, RelayClient};

/// The most digests one `blob.drop` names.
pub(in crate::log_sync) const DROP_BATCH: usize = 25;
/// The most stream heads one `blob.drop` names as its conditions.
pub(in crate::log_sync) const DROP_HEADS: usize = 32;

/// A stream and the sequence number of its head, as a `blob.drop` names it.
pub(in crate::log_sync) type HeadAt = relay_protocol::StreamPosition;

/// What one `blob.drop` did: stored files the relay dropped in this call (`dropped`), the bytes
/// it released for them (`released_bytes`), and digests of the call that the relay did not hold,
/// dropped earlier or never there (`absent`).
pub(in crate::log_sync) type Dropped = relay_protocol::BlobDropAnswer;

/// True when `error` is the answer of a relay whose stream heads moved since the caller listed
/// them: HTTP 409 with the code `heads`.
fn heads_moved(error: &CallError) -> bool {
    error
        .refusal(409)
        .is_some_and(|answer| answer.code == ErrorCode::Heads)
}

/// Maximum total blobs accepted across all pages of one `blob.list` scan.
const MAX_LISTED_BLOBS: usize = 1_000_000;

impl RelayClient {
    /// Every committed blob of the database on the relay, all pages: digest to length.
    pub(in crate::log_sync) async fn blob_list(
        &self,
        database_id: &str,
    ) -> Result<BTreeMap<String, u64>, String> {
        let mut found = BTreeMap::new();
        let mut after: Option<String> = None;
        loop {
            let request = BlobListRequest {
                database_id: database_id.to_owned(),
                after: after.clone(),
            };
            let BlobListAnswer { blobs: page, next } = self.call::<BlobList>(&request).await?;
            validate_blob_page(&page, after.as_deref())?;
            if found.len().saturating_add(page.len()) > MAX_LISTED_BLOBS {
                return Err("relay blob.list exceeded 1000000 blobs".into());
            }
            let last_digest = page.last().map(|blob| blob.sha256.clone());
            for blob in page {
                found.insert(blob.sha256, blob.bytes);
            }
            let Some(next) = next else {
                return Ok(found);
            };
            mutation_log::Hex32::from_hex(&next)
                .map_err(|_| "relay blob.list returned an invalid next cursor".to_owned())?;
            let Some(last) = last_digest else {
                return Err("relay blob.list returned a next cursor for an empty page".into());
            };
            if next != last {
                return Err("relay blob.list next cursor does not match the last digest".into());
            }
            after = Some(last);
        }
    }

    /// Ask the relay to drop the blobs `sha256` (at most [`DROP_BATCH`]) on the condition that
    /// the head of the snapshot stream is `snapshot` and every stream in `heads` still ends where
    /// it is named. `None` when a stream was not as named: the relay dropped nothing, and the
    /// caller computes its list again. The relay refuses the manifest that the snapshot entry
    /// names. The caller keeps the account of what was released (`personal::reclaim_count`): the
    /// client asks again by itself after a lost answer, and the second answer then says `absent`.
    pub(in crate::log_sync) async fn blob_drop(
        &self,
        database_id: &str,
        snapshot: &HeadAt,
        heads: &[HeadAt],
        sha256: &[String],
    ) -> Result<Option<(Dropped, bool)>, String> {
        let request = BlobDropRequest {
            database_id: database_id.to_owned(),
            snapshot: snapshot.clone(),
            heads: heads.to_vec(),
            sha256: sha256.to_vec(),
        };
        match self.call_tracked::<BlobDrop>(&request).await {
            Ok(pair) => Ok(Some(pair)),
            Err(error) if heads_moved(&error) => Ok(None),
            Err(error) => Err(error.message),
        }
    }
}

fn validate_blob_page(page: &[BlobListed], after: Option<&str>) -> Result<(), String> {
    let mut prev = after;
    for blob in page {
        mutation_log::Hex32::from_hex(&blob.sha256)
            .map_err(|_| "relay blob.list returned an invalid blob digest".to_owned())?;
        if prev.is_some_and(|floor| blob.sha256.as_str() <= floor) {
            return Err("relay blob.list returned digests out of ascending order".into());
        }
        prev = Some(&blob.sha256);
    }
    Ok(())
}
