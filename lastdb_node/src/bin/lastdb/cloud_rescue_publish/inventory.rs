//! Cloud writer inventory collection for the S0 publisher. Moved verbatim from `cloud_rescue_publish.rs`.

use super::*;

pub(super) fn writer_and_seq(key: &str) -> Option<(Option<&str>, u64)> {
    let relative = key
        .strip_prefix("log/")
        .or_else(|| key.split_once("/log/").map(|(_, tail)| tail))?;
    let Some((writer, path)) = relative.split_once('/') else {
        return Some((None, relative.strip_suffix(".enc")?.parse().ok()?));
    };
    if writer.is_empty() || writer.contains("..") {
        return None;
    }
    let suffix = path.strip_suffix(".enc")?;
    let seq = if let Some((schemas, file)) = suffix.rsplit_once('/') {
        if schemas
            .split('/')
            .any(|schema| schema.is_empty() || schema.contains(".."))
        {
            return None;
        }
        let (utc_nanos, sequence) = file.rsplit_once('_')?;
        let _: u64 = utc_nanos.parse().ok()?;
        sequence.parse().ok()?
    } else {
        suffix.parse().ok()?
    };
    Some((Some(writer), seq))
}

// lint:fn-size-ok moved verbatim from its original module
pub(super) async fn collect_writer_inventory(
    home: &Path,
    db_hash: &str,
    auth: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
) -> Result<serde_json::Value, String> {
    let scoped = auth
        .clone()
        .with_db_hash(Some(db_hash.to_string()))
        .without_db_auto_claim();
    let (prior_sha, prior) = read_prior_backup_manifest(&scoped, s3, db_hash).await?;
    let local_id = read_small_regular(&home.join("data/.device_id"), 256)?
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .ok_or("writer inspection requires the local device identity")?;
    let mut writers: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    let mut unparsed = 0u64;
    let (visible_count, stats) = auth
        .visit_db_log_objects_skipping_writer_at_most(
            db_hash,
            &local_id,
            MAX_PEER_LOG_OBJECTS,
            |objects| {
                for object in objects {
                    let Some((writer, seq)) = writer_and_seq(&object.key) else {
                        unparsed += 1;
                        continue;
                    };
                    let modified = chrono::DateTime::parse_from_rfc3339(&object.last_modified)
                        .map_err(|_| {
                            fold_db::sync::error::SyncError::Storage(
                                "cloud log object has an invalid modification time".into(),
                            )
                        })?;
                    let modified = u64::try_from(modified.timestamp()).map_err(|_| {
                        fold_db::sync::error::SyncError::Storage(
                            "cloud log object predates Unix time".into(),
                        )
                    })?;
                    let entry = writers.entry(writer.unwrap_or("").to_string()).or_default();
                    entry.0 += 1;
                    entry.1 = entry.1.max(seq);
                    entry.2 = entry.2.max(modified);
                }
                Ok(())
            },
        )
        .await
        .map_err(|error| format!("list cloud peer writers: {error}"))?;
    require_first_backup_writer_inventory(prior.is_some(), visible_count, unparsed)?;
    let mut peer_newer = false;
    let mut unknown_newer = false;
    let summary = writers
        .iter()
        .enumerate()
        .map(|(ordinal, (writer, (count, seq, modified)))| {
            let local = if writer.is_empty() {
                None
            } else {
                Some(local_id.as_str() == writer.as_str())
            };
            if prior
                .as_ref()
                .is_some_and(|prior| *modified > prior.created_at_unix_secs)
            {
                match local {
                    Some(false) => peer_newer = true,
                    None => unknown_newer = true,
                    Some(true) => {}
                }
            }
            serde_json::json!({
                "writer": ordinal + 1,
                "local": local,
                "objects": count,
                "max_seq": seq,
                "latest_modified_unix_secs": modified,
            })
        })
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "ok": true,
        "cloud_write": false,
        "source_scope": "primary_only",
        "prior_manifest_present": prior.is_some(),
        "prior_manifest_sha256": prior_sha,
        "prior_manifest_counter": prior.as_ref().map(|prior| prior.counter),
        "prior_manifest_created_at_unix_secs": prior.as_ref().map(|prior| prior.created_at_unix_secs),
        "visible_writer_count": summary.len(),
        "visible_log_object_count": visible_count,
        "unparsed_log_objects": unparsed,
        "local_writer_skipped": true,
        "local_writer_keys_sampled": stats.skipped_keys,
        "writer_list_pages": stats.pages,
        "writer_list_jumped": stats.jumped,
        "writer_list_complete": true,
        "peer_logs_newer_than_prior_backup": prior.as_ref().map(|_| peer_newer),
        "unknown_logs_newer_than_prior_backup": prior.as_ref().map(|_| unknown_newer),
        "writers": summary,
    });
    Ok(report)
}

pub(super) fn print_writer_inventory(report: &serde_json::Value, json: bool) {
    if json {
        println!("{report}");
    } else {
        println!("Cloud writer inventory: {report}");
    }
}
