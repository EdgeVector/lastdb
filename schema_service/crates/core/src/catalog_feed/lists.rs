//! Read the registry's interface lists by key and keep the verified ones.
//!
//! Verification is the code the LastDB node uses to install apps
//! (`app_identity_crypto::InterfaceList::load`): the registry signature over
//! the exact bytes, then each publisher signature. A list that fails keeps
//! its previous state and its previous ETag, so the next run reads it again.

use std::ops::RangeInclusive;

use app_identity_crypto::{
    hex_lower, interface_list_path, InterfaceList, VerifyingKey, LIST_CHANNELS,
};
use sha2::{Digest, Sha256};

use super::run::FeedRun;
use super::{
    CatalogFeedState, FeedListEntry, FeedListState, ListFetch, LIST_API_PROBE_AHEAD,
    MIN_LIST_API_VERSION,
};

/// The bytes of one changed list.
struct FetchedList {
    body: Vec<u8>,
    signature: Vec<u8>,
    etag: Option<String>,
}

/// The API versions a run reads: from the floor to a little past the
/// highest list seen, so a new interface version is found without a deploy.
fn probe_versions(state: &CatalogFeedState) -> RangeInclusive<u32> {
    let highest = state
        .lists
        .values()
        .map(|list| list.api_version)
        .max()
        .unwrap_or(MIN_LIST_API_VERSION)
        .max(MIN_LIST_API_VERSION);
    MIN_LIST_API_VERSION..=highest.saturating_add(LIST_API_PROBE_AHEAD)
}

/// Verify one changed list and refuse a sequence rollback.
fn accept_list(
    trust: &VerifyingKey,
    channel: &str,
    api_version: u32,
    previous: Option<&FeedListState>,
    fetched: FetchedList,
    now: &str,
) -> Result<FeedListState, String> {
    let list = InterfaceList::load(
        &fetched.body,
        &fetched.signature,
        trust,
        channel,
        api_version,
    )?;
    let payload_sha256 = hex_lower(&Sha256::digest(&fetched.body));
    if let Some(previous) = previous {
        if list.seq < previous.seq {
            return Err(format!(
                "sequence {} is older than the verified sequence {}",
                list.seq, previous.seq
            ));
        }
        if list.seq == previous.seq && payload_sha256 != previous.payload_sha256 {
            return Err(format!(
                "sequence {} names different bytes than the verified list",
                list.seq
            ));
        }
    }
    let entries = list
        .apps
        .iter()
        .filter(|entry| !entry.is_private())
        .map(|entry| FeedListEntry {
            app_id: entry.app_id.clone(),
            sha: entry.sha.clone(),
            source: entry.source.clone(),
        })
        .collect();
    Ok(FeedListState {
        channel: list.channel,
        api_version: list.api_version,
        etag: fetched.etag,
        seq: list.seq,
        payload_sha256,
        generated_at: list.generated_at,
        fetched_at: now.to_string(),
        entries,
    })
}

impl FeedRun<'_> {
    /// Read every list key once. The keys whose verified content changed
    /// in this run go to `report.lists_changed`.
    pub(super) async fn refresh_lists(&mut self, state: &mut CatalogFeedState) {
        for channel in LIST_CHANNELS {
            for api_version in probe_versions(state) {
                let key = interface_list_path(channel, api_version);
                let refreshed = self.refresh_list(state, channel, api_version, &key).await;
                if let Some(list) = refreshed {
                    state.lists.insert(key.clone(), list);
                    self.report.lists_changed.push(key);
                }
            }
        }
    }

    /// The new state of one key, or `None` when it keeps its previous state.
    async fn refresh_list(
        &mut self,
        state: &CatalogFeedState,
        channel: &str,
        api_version: u32,
        key: &str,
    ) -> Option<FeedListState> {
        let previous = state.lists.get(key);
        let etag = previous.and_then(|list| list.etag.as_deref());
        let fetched = match self.source.fetch_list(channel, api_version, etag).await {
            Ok(ListFetch::NotModified) => {
                self.report.lists_unchanged += 1;
                return None;
            }
            Ok(ListFetch::Absent) => {
                if previous.is_some() {
                    self.list_failed(key, "the list is no longer readable; previous state kept");
                }
                return None;
            }
            Ok(ListFetch::Fetched {
                body,
                signature,
                etag,
            }) => FetchedList {
                body,
                signature,
                etag,
            },
            Err(error) => {
                self.list_failed(key, &error);
                return None;
            }
        };
        match accept_list(
            &self.trust,
            channel,
            api_version,
            previous,
            fetched,
            &self.now,
        ) {
            Ok(list) => Some(list),
            Err(error) => {
                self.list_failed(key, &error);
                None
            }
        }
    }

    fn list_failed(&mut self, key: &str, reason: &str) {
        tracing::warn!(
            target: "schema_service::catalog_feed",
            list = %key,
            reason = %reason,
            "registry list skipped"
        );
        self.report.lists_failed.push(format!("{key}: {reason}"));
    }
}
