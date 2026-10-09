use super::super::helpers::attach_target_scope;
use super::super::{AuthClient, ListObjectsResponse, S3ObjectInfo};
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use std::collections::HashSet;

impl AuthClient {
    pub async fn list_objects(&self, prefix: &str) -> SyncResult<Vec<S3ObjectInfo>> {
        let body = serde_json::json!({
            "action": "list_objects",
            "prefix": prefix,
        });

        self.list_objects_all_pages(body, "list").await
    }

    /// Read a complete database prefix without crossing a caller's object cap.
    ///
    /// An explicit db hash and no-claim POST keep rescue preflight read-only.
    /// Stop at the first page above the limit, before any permanent rescue hold.
    pub async fn list_db_objects_at_most(
        &self,
        db_hash: &str,
        prefix: &str,
        maximum: usize,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "db_hash": db_hash,
            "prefix": prefix,
        });
        let mut objects = Vec::new();
        let mut seen_tokens = std::collections::HashSet::new();
        loop {
            if seen_tokens.len() >= 1_000 {
                return Err(SyncError::Storage("bounded list exceeds 1000 pages".into()));
            }
            let resp = self
                .post_no_default_db_hash("/api/sync/list", body.clone())
                .await?;
            let parsed: ListObjectsResponse = serde_json::from_value(resp)?;
            if !parsed.ok {
                return Err(op_failed("bounded list", parsed.error.or(parsed.reason)));
            }
            let next_count = objects
                .len()
                .checked_add(parsed.objects.len())
                .ok_or_else(|| {
                    SyncError::Storage("bounded list object count overflow".to_string())
                })?;
            if next_count > maximum {
                return Err(SyncError::Storage(format!(
                    "bounded list for {prefix} exceeds {maximum} objects"
                )));
            }
            objects.extend(parsed.objects);
            match (parsed.has_more, parsed.continuation_token) {
                (Some(false), None) => return Ok(objects),
                (Some(true), Some(token))
                    if !token.is_empty() && seen_tokens.insert(token.clone()) =>
                {
                    body["continuation_token"] = serde_json::Value::String(token);
                }
                _ => {
                    return Err(SyncError::Storage(
                        "bounded list has incomplete pagination".to_string(),
                    ));
                }
            }
        }
    }

    /// Visit every log key for one database without retaining the full list.
    /// A primary-authoritative resume must inspect its own writer keys too;
    /// the peer-list shortcut deliberately skips those keys.
    pub async fn visit_db_log_objects_at_most<F>(
        &self,
        db_hash: &str,
        maximum: usize,
        mut visit: F,
    ) -> SyncResult<usize>
    where
        F: FnMut(&[S3ObjectInfo]) -> SyncResult<()>,
    {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "db_hash": db_hash,
            "prefix": "log/",
        });
        let mut count = 0usize;
        let mut pages = 0usize;
        let mut seen_tokens = HashSet::new();
        loop {
            let resp = self
                .post_no_default_db_hash("/api/sync/list", body.clone())
                .await?;
            let parsed: ListObjectsResponse = serde_json::from_value(resp)?;
            if !parsed.ok {
                return Err(op_failed(
                    "full log inventory",
                    parsed.error.or(parsed.reason),
                ));
            }
            pages = pages.saturating_add(1);
            if pages > 1_000 {
                return Err(SyncError::Storage(
                    "full log inventory exceeds 1000 pages".into(),
                ));
            }
            count = count
                .checked_add(parsed.objects.len())
                .ok_or_else(|| SyncError::Storage("full log inventory count overflow".into()))?;
            if count > maximum {
                return Err(SyncError::Storage(format!(
                    "full log inventory exceeds {maximum} objects"
                )));
            }
            visit(&parsed.objects)?;
            let Some(token) = parsed.continuation_token else {
                return Ok(count);
            };
            if !seen_tokens.insert(token.clone()) {
                return Err(SyncError::Storage(
                    "full log inventory returned repeated continuation token".into(),
                ));
            }
            body["continuation_token"] = serde_json::Value::String(token);
        }
    }

    /// Visit peer log pages without retaining the full key list.
    ///
    /// The jump matches `list_log_objects_skipping_writer`, while the caller
    /// receives each bounded page before the next request.
    pub async fn visit_db_log_objects_skipping_writer_at_most<F>(
        &self,
        db_hash: &str,
        skip_writer: &str,
        maximum: usize,
        mut visit: F,
    ) -> SyncResult<(usize, LogListStats)>
    where
        F: FnMut(&[S3ObjectInfo]) -> SyncResult<()>,
    {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "db_hash": db_hash,
            "prefix": "log/",
        });
        if skip_writer.is_empty() || skip_writer.contains('/') || skip_writer.contains("..") {
            return Err(SyncError::Storage("invalid local log writer".into()));
        }
        let skip_prefix = format!("log/{skip_writer}/");
        let resume_after = format!("log/{skip_writer}0");
        let mut count = 0usize;
        let mut stats = LogListStats::default();
        let mut seen_tokens = std::collections::HashSet::new();
        let mut jumped = false;
        loop {
            let resp = self
                .post_no_default_db_hash("/api/sync/list", body.clone())
                .await?;
            let parsed: ListObjectsResponse = serde_json::from_value(resp)?;
            if !parsed.ok {
                return Err(op_failed("streamed list", parsed.error.or(parsed.reason)));
            }
            stats.pages = stats.pages.saturating_add(1);
            if stats.pages > 1_000 {
                return Err(SyncError::Storage(
                    "peer log inventory exceeds 1000 pages".into(),
                ));
            }
            let page_ends_in_skip_range = parsed
                .objects
                .last()
                .is_some_and(|object| relative_log_key(&object.key).starts_with(&skip_prefix));
            let mut visible = Vec::new();
            for object in parsed.objects {
                let relative = relative_log_key(&object.key);
                if relative.starts_with(&skip_prefix) {
                    stats.skipped_keys = stats.skipped_keys.saturating_add(1);
                    continue;
                }
                if jumped && relative <= resume_after.as_str() {
                    continue;
                }
                visible.push(object);
            }
            count = count.checked_add(visible.len()).ok_or_else(|| {
                SyncError::Storage("streamed list object count overflow".to_string())
            })?;
            if count > maximum {
                return Err(SyncError::Storage(format!(
                    "peer log inventory exceeds {maximum} objects"
                )));
            }
            visit(&visible)?;
            let Some(token) = parsed.continuation_token else {
                return Ok((count, stats));
            };
            if page_ends_in_skip_range && !jumped {
                jumped = true;
                stats.jumped = true;
                if let Some(map) = body.as_object_mut() {
                    map.remove("continuation_token");
                }
                body["start_after"] = serde_json::Value::String(resume_after.clone());
                continue;
            }
            if !seen_tokens.insert(token.clone()) {
                return Err(SyncError::Storage(
                    "streamed list returned repeated continuation token".to_string(),
                ));
            }
            body["continuation_token"] = serde_json::Value::String(token);
            if let Some(map) = body.as_object_mut() {
                map.remove("start_after");
            }
        }
    }

    /// List objects from the legacy personal root even when this client has a
    /// default `db_hash`. Used only during db_hash migration bootstrap.
    pub async fn list_objects_legacy_personal(
        &self,
        prefix: &str,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let body = serde_json::json!({
            "action": "list_objects",
            "prefix": prefix,
        });

        self.list_objects_all_pages_without_default_db_hash(body, "list legacy personal")
            .await
    }

    /// Read every account-root object under a strict object cap. A missing
    /// page marker cannot make a recovery descriptor look absent.
    pub async fn list_objects_legacy_personal_at_most(
        &self,
        prefix: &str,
        maximum: usize,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let body = serde_json::json!({
            "action": "list_objects",
            "prefix": prefix,
        });
        self.list_objects_all_pages_with_post(
            body,
            "list bounded legacy personal",
            false,
            Some(maximum),
        )
        .await
    }

    /// List log objects for a sync target.
    pub async fn list_log_objects(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        self.list_log_objects_after(target, None).await
    }

    /// List snapshot objects (`snapshots/*.enc`) under a sync target's prefix.
    ///
    /// Used by the cloud-aware reset path to enumerate every snapshot
    /// (including `latest.enc` and any compacted `{seq}.enc`) so each can
    /// be deleted via `presign_snapshot_delete`.
    pub async fn list_snapshot_objects(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "prefix": "snapshots/",
        });
        attach_target_scope(&mut body, target);
        self.list_objects_all_pages(body, "list snapshot objects")
            .await
    }

    /// List log objects for a sync target, optionally starting after a given key.
    ///
    /// **WARNING — lex-ordered `start_after`:** S3 `start_after` filters keys
    /// by **lexicographic** order, not numeric. Do not pass a key built from
    /// an unpadded decimal seq (`log/52.enc`) expecting it to bound keys with
    /// higher numeric seqs — `log/100.enc` lex-sorts *before* `log/52.enc`
    /// and would be silently hidden. This caused alpha BLOCKER 30a7b. Only
    /// use `start_after` against keys whose natural ordering is already
    /// lexicographic (e.g., ISO-8601 timestamps, fixed-width hex). Numeric
    /// log seqs must be filtered client-side after a full prefix list.
    pub async fn list_log_objects_after(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        start_after: Option<&str>,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "prefix": "log/",
        });
        attach_target_scope(&mut body, target);
        if let Some(cursor) = start_after {
            body["start_after"] = serde_json::Value::String(cursor.to_string());
        }
        self.list_objects_all_pages(body, "list log objects").await
    }

    /// List log objects for a sync target, but jump over the `log/{writer}/`
    /// range of one writer instead of paging through it.
    ///
    /// Peer apply discards this node's own writer stream (self-echo), yet a
    /// plain [`Self::list_log_objects`] pages through every one of those keys
    /// first. The own range grows by one object per published segment, so on
    /// the primary (2026-10-05, ~1,200 segments per pass) the full listing took
    /// 7-24 minutes per `do_sync`, and the mutation-log drain waited for it on
    /// every pass. Here the first page that ends inside the skipped range is
    /// followed by a fresh listing with `start_after` = just past that range,
    /// so the cost is one page, not one page per 1,000 own segments. The same
    /// prefix covers classic `log/{writer}/{seq}.enc` keys and schema-folder
    /// `log/{writer}/{schema}/...` keys.
    ///
    /// Flat classic keys (`log/{seq}.enc`) are the other own-file layout. They
    /// sort among digit-leading device ids, so a cursor such as `log/:` would
    /// hide those devices. This call sets `omit_flat_classic`. A current
    /// storage service drops those keys while it scans and returns the other
    /// devices in a few pages. An older service ignores the field and returns
    /// the flat keys; this client drops them and keeps paging, so a device
    /// that sorts between flat keys is still found.
    ///
    /// Keys inside the skipped writer range never reach the caller, even when
    /// a page holds them next to other keys. A server that ignores
    /// `start_after` costs time, not correctness: keys already seen are
    /// dropped, and after one jump the walk falls back to continuation tokens.
    pub async fn list_log_objects_skipping_writer(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        skip_writer: &str,
    ) -> SyncResult<(Vec<S3ObjectInfo>, LogListStats)> {
        let mut body = serde_json::json!({
            "action": "list_objects",
            "prefix": "log/",
            // The storage service drops `log/{seq}.enc` while it scans.
            // An older service ignores this field.
            "omit_flat_classic": true,
        });
        attach_target_scope(&mut body, target);
        let skip_prefix = format!("log/{skip_writer}/");
        // '0' (0x30) is the byte right after '/' (0x2F): every key in
        // `log/{writer}/...` sorts before this, and no other writer key that
        // shares the id as a prefix (`log/{writer}0...`) sorts before it.
        let resume_after = format!("log/{skip_writer}0");
        let in_skip_range = |key: &str| relative_log_key(key).starts_with(&skip_prefix);

        let mut objects = Vec::new();
        let mut stats = LogListStats::default();
        let mut seen_tokens = HashSet::new();
        let mut jumped = false;

        loop {
            let resp = self.post("/api/sync/list", body.clone()).await?;
            let parsed: ListObjectsResponse = serde_json::from_value(resp)?;
            if !parsed.ok {
                return Err(op_failed(
                    "list log objects",
                    parsed.error.or(parsed.reason),
                ));
            }
            stats.pages = stats.pages.saturating_add(1);
            let page_ends_in_skip_range = parsed
                .objects
                .last()
                .is_some_and(|obj| in_skip_range(&obj.key));
            for obj in parsed.objects {
                let relative = relative_log_key(&obj.key);
                if in_skip_range(&obj.key) {
                    stats.skipped_keys = stats.skipped_keys.saturating_add(1);
                    continue;
                }
                // After a jump, anything at or before the resume point was
                // already returned (or is in the skipped range). Only a
                // server that ignored `start_after` sends such keys.
                if jumped && relative <= resume_after.as_str() {
                    continue;
                }
                // Flat classic keys are this device's old layout. Drop them
                // here too, so an older server that still returns them cannot
                // make peer apply download them. Do not resume at `log/:`:
                // that cursor sorts after every digit-leading device id.
                if is_flat_classic_log_key(&obj.key) {
                    stats.skipped_keys = stats.skipped_keys.saturating_add(1);
                    continue;
                }
                objects.push(obj);
            }

            let Some(token) = parsed.continuation_token else {
                break;
            };
            if page_ends_in_skip_range && !jumped {
                jumped = true;
                stats.jumped = true;
                if let Some(map) = body.as_object_mut() {
                    map.remove("continuation_token");
                }
                body["start_after"] = serde_json::Value::String(resume_after.clone());
                continue;
            }
            if !seen_tokens.insert(token.clone()) {
                return Err(SyncError::Storage(
                    "list log objects returned repeated continuation token".to_string(),
                ));
            }
            body["continuation_token"] = serde_json::Value::String(token);
            if let Some(map) = body.as_object_mut() {
                map.remove("start_after");
            }
        }

        Ok((objects, stats))
    }

    async fn list_objects_all_pages(
        &self,
        body: serde_json::Value,
        context: &str,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        self.list_objects_all_pages_with_post(body, context, true, None)
            .await
    }

    async fn list_objects_all_pages_without_default_db_hash(
        &self,
        body: serde_json::Value,
        context: &str,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        self.list_objects_all_pages_with_post(body, context, false, None)
            .await
    }

    async fn list_objects_all_pages_with_post(
        &self,
        mut body: serde_json::Value,
        context: &str,
        apply_default_db_hash: bool,
        maximum: Option<usize>,
    ) -> SyncResult<Vec<S3ObjectInfo>> {
        let mut objects = Vec::new();
        let mut seen_tokens = std::collections::HashSet::new();

        loop {
            let resp = if apply_default_db_hash {
                self.post("/api/sync/list", body.clone()).await?
            } else {
                self.post_no_default_db_hash("/api/sync/list", body.clone())
                    .await?
            };
            let parsed: ListObjectsResponse = serde_json::from_value(resp)?;
            if !parsed.ok {
                return Err(op_failed(context, parsed.error.or(parsed.reason)));
            }

            let count = objects
                .len()
                .checked_add(parsed.objects.len())
                .ok_or_else(|| SyncError::Storage(format!("{context} object count overflow")))?;
            if maximum.is_some_and(|cap| count > cap) {
                return Err(SyncError::Storage(format!(
                    "{context} exceeds {} objects",
                    maximum.expect("bounded list cap")
                )));
            }
            objects.extend(parsed.objects);

            let Some(token) = parsed.continuation_token else {
                if maximum.is_some() && parsed.has_more != Some(false) {
                    return Err(SyncError::Storage(format!(
                        "{context} returned an incomplete final page"
                    )));
                }
                break;
            };
            if maximum.is_some() && (parsed.has_more != Some(true) || token.is_empty()) {
                return Err(SyncError::Storage(format!(
                    "{context} returned an incomplete continuation page"
                )));
            }
            if !seen_tokens.insert(token.clone()) {
                return Err(SyncError::Storage(format!(
                    "{context} returned repeated continuation token"
                )));
            }
            body["continuation_token"] = serde_json::Value::String(token);
            if let Some(map) = body.as_object_mut() {
                map.remove("start_after");
            }
        }

        Ok(objects)
    }
}

/// Page and skip counts from [`AuthClient::list_log_objects_skipping_writer`],
/// so the caller can log what one listing cost.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LogListStats {
    /// List requests sent.
    pub pages: usize,
    /// Keys dropped because they are this writer's files or flat classic files.
    pub skipped_keys: usize,
    /// True when the listing jumped past the skipped range with `start_after`.
    pub jumped: bool,
}

/// The `log/...` part of a listed key. The storage service strips the scope
/// prefix, but a full key is accepted too.
fn relative_log_key(listed: &str) -> &str {
    if listed.starts_with("log/") {
        return listed;
    }
    listed.find("/log/").map_or(listed, |i| &listed[i + 1..])
}

/// Classic `log/{seq}.enc` with no writer folder. Schema-folder and
/// writer-scoped keys do not match.
fn is_flat_classic_log_key(key: &str) -> bool {
    let key = relative_log_key(key);
    let Some(rest) = key.strip_prefix("log/") else {
        return false;
    };
    let Some(seq) = rest.strip_suffix(".enc") else {
        return false;
    };
    !seq.is_empty() && !seq.contains('/') && seq.parse::<u64>().is_ok()
}
