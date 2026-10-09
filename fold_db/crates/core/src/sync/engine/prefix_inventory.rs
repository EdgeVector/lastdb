//! Read-only prefix/category size inventory for the connected cloud
//! account (`lastdb cloud prefix-inventory`), across BOTH backing stores.
//!
//! `lastdb cloud status` reports one total (`used_bytes`) and `backup-gc`
//! reports the `backup/chunks/` keep-set footprint; neither names what the
//! rest of the bytes are, so an operator is left inferring
//! `remainder = total - chunks`. This module lists the connected account's
//! full scope through the same authenticated path
//! [`super::SyncEngine::gc_orphan_backup_chunks`] already uses for
//! `backup/chunks/` (`AuthClient::list_objects`, never a direct S3 SDK call
//! and never raw R2/AWS credentials) and buckets every key by storage
//! category, so a reading can attribute growth instead of subtracting totals.
//!
//! List-only: no delete, no lifecycle rule, no object body is fetched.
//!
//! # One list call cannot see the whole scope
//!
//! The storage Lambda routes a list to a bucket **by the prefix it is given**
//! (`exemem_common::storage::s3::select_storage` over
//! `{scope}/{prefix_suffix}`): `log/`, `snapshots/`, `thumbs/`, `inbox/`,
//! `p2p/`, `backup/chunks/`, `backup/manifests/`, `backup/latest`,
//! `backup/v2/`, `latest` and `lock.json` resolve to R2; everything else
//! falls through to B2.
//!
//! The first version of this report issued ONE list with an empty prefix.
//! `{scope}/` matches no R2 rule, so that call listed **B2 only** and could
//! never return an object in seven of the ten categories `classify_key`
//! knows — while still printing a `TOTAL` line and calling itself a
//! full-scope inventory. Measured on the primary 2026-09-07 10:5xZ: this
//! report said 2,943 objects / 505.3 MB with zero `backup/chunks`, against
//! `lastdb cloud backup-gc` listing 32,815 chunks / 53.77 GB **in the same
//! minute, through the same client**. A ~99%-by-bytes under-report from the
//! one tool built to stop operators inferring a remainder.
//!
//! So the scope is listed once per routing class and the results unioned.
//! Keys are de-duplicated because an unconfigured R2 makes `select_storage`
//! fall back to B2, which would otherwise double-count every B2 object under
//! an R2-routed prefix. See
//! `papercut-lastdb-prefix-inventory-lists-b2-only-and-reports-it-as-the-full-scope-20260907`.

use super::*;
use serde::Serialize;

/// One storage category's listed footprint.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PrefixInventoryEntry {
    pub category: &'static str,
    pub object_count: usize,
    pub total_bytes: u64,
}

/// Full-scope prefix inventory: every object the connected account's cloud
/// scope lists, bucketed by storage category.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct PrefixInventoryReport {
    pub entries: Vec<PrefixInventoryEntry>,
    pub total_objects: usize,
    pub total_bytes: u64,
    /// Up to 25 example keys that matched no known category. The whole
    /// point of this report is naming an unattributed remainder instead of
    /// silently folding it into "other" — surface real keys, not a count.
    pub unclassified_sample: Vec<String>,
    /// Every prefix this report actually listed, in call order. A report
    /// that names its own coverage cannot be read as full-scope when it is
    /// not; the empty string is the B2 fall-through list.
    pub listed_prefixes: Vec<&'static str>,
}

const UNCLASSIFIED_SAMPLE_CAP: usize = 25;

/// Scope-relative prefixes that must each be listed to cover the whole
/// account, one per bucket-routing class.
///
/// `""` is the B2 fall-through and picks up `cas/`, `files/`, `.tmp/`,
/// `manifest.json` and anything unclassified. The rest are exactly the
/// prefixes `exemem_common::storage::s3::is_r2_path` routes to R2 when the
/// Lambda joins them onto `{scope}/`.
///
/// `backup/` on its own is NOT usable here: `is_backup_storage_key` matches
/// `/backup/chunks/`, `/backup/manifests/`, a key ending `/backup/latest`,
/// and `/backup/v2/`, so `{scope}/backup/` falls through to B2. The three
/// real v1 backup prefixes are listed individually for that reason. The v2
/// namespace is matched on its root, so one `backup/v2/` list reaches all
/// four of its sub-prefixes.
const SCOPE_LIST_PREFIXES: &[&str] = &[
    "",
    "log/",
    "snapshots/",
    "thumbs/",
    "inbox/",
    "p2p/",
    "backup/chunks/",
    "backup/manifests/",
    "backup/latest",
    backup_keys::BACKUP_V2_PREFIX,
    "latest",
    "lock.json",
];

/// Storage-category classification of a scope-relative object key (the
/// scope/user-hash prefix is already stripped by the Lambda before the
/// client sees it — see `AuthClient::list_objects`).
///
/// Deliberately duplicated rather than imported: the schema's single source
/// of truth (`is_billable_storage_key` and friends) lives in
/// `exemem_common::storage`, which is part of the lambda-only,
/// workspace-excluded `exemem_service/lambdas/*` tree — `fold_db` core must
/// not depend on it. Keep these prefixes in sync with that file by hand.
/// The backup namespaces (v1 and `backup/v2/`) come from
/// [`backup_keys::classify_backup_key`], the one client-side classifier
/// every backup listing parser shares.
fn classify_key(key: &str) -> &'static str {
    if let Some(category) = backup_keys::classify_backup_key(key).category() {
        category
    } else if key == "latest" {
        "photograph/latest"
    } else if key.starts_with("thumbs/loose/") {
        "thumbs/loose"
    } else if key.starts_with("thumbs/") {
        "thumbs/other"
    } else if key.starts_with("snapshots/") {
        "snapshots"
    } else if key.starts_with("log/") {
        "log"
    } else if key.starts_with("cas/") {
        "files/cas"
    } else if key.starts_with("files/") {
        "files/legacy"
    } else if key == "lock.json"
        || key == "manifest.json"
        || key.starts_with(".tmp/")
        || key.starts_with("p2p/")
        || key.starts_with("inbox/")
    {
        "bookkeeping"
    } else {
        "unclassified"
    }
}

impl SyncEngine {
    /// Product path: list every object under the connected account's scope
    /// and bucket bytes by storage category. Read-only — never deletes,
    /// never fetches an object body, never touches the DELETE presign path.
    ///
    /// Issues one list per entry in [`SCOPE_LIST_PREFIXES`] because the
    /// Lambda picks the bucket from the prefix (see the module docs). Fails
    /// closed: if any one of those lists errors, the whole report errors
    /// rather than returning a subtotal that reads like a full scope.
    pub async fn prefix_inventory(&self) -> SyncResult<PrefixInventoryReport> {
        let mut totals: std::collections::BTreeMap<&'static str, (usize, u64)> =
            std::collections::BTreeMap::new();
        let mut unclassified_sample = Vec::new();
        // An unconfigured R2 makes `select_storage` fall back to B2, so the
        // same B2 object can come back under both `""` and an R2-routed
        // prefix. Count each key once.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut total_objects = 0usize;
        let mut total_bytes = 0u64;

        for prefix in SCOPE_LIST_PREFIXES {
            let listed = self.auth.list_objects(prefix).await?;
            for object in listed {
                if !seen.insert(object.key.clone()) {
                    continue;
                }
                let category = classify_key(&object.key);
                let entry = totals.entry(category).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += object.size;
                if category == "unclassified" && unclassified_sample.len() < UNCLASSIFIED_SAMPLE_CAP
                {
                    unclassified_sample.push(object.key.clone());
                }
                total_objects += 1;
                total_bytes += object.size;
            }
        }

        let entries = totals
            .into_iter()
            .map(
                |(category, (object_count, total_bytes))| PrefixInventoryEntry {
                    category,
                    object_count,
                    total_bytes,
                },
            )
            .collect();

        Ok(PrefixInventoryReport {
            entries,
            total_objects,
            total_bytes,
            unclassified_sample,
            listed_prefixes: SCOPE_LIST_PREFIXES.to_vec(),
        })
    }
}
