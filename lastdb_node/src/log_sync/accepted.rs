//! Publish one page of the durable accepted prefix.
//!
//! The reader is `accepted_writes`. A page is acknowledged locally only after
//! every item is published or withheld. Cloud delivery progress stays in `run`.

mod position;

use std::sync::Arc;

use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use database_policy::SpecBinding;
use fold_db::schema::types::KeyValue;
use fold_db::storage::disk_backup::accepted::UploadPosition;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{
    AppendBatch, AppendOutcome, DatabaseId, Entry, LogError, MutationLog, Namespace,
};
use serde::Deserialize;
use zeroize::Zeroizing;

use crate::accepted_writes::{self, Blocked, Item, Page, Publication};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use position::LocalUpload;

use super::cloud::CloudLog;
use super::registry;
use super::retry::{self, Seal};
use super::{upload, wire};

const PAGE_LIMIT: u32 = 64;

pub(super) async fn publish_owned(host: &Host, log: &CloudLog) -> Result<(), &'static str> {
    let Ok(Some((store, key))) = DatabaseContentKey::local_store(host, log.database_id()) else {
        return Err("local database or content key unavailable");
    };
    let runtime = store.db_ops().native_runtime();
    let Some(policy) = publication(host, log.database_id()) else {
        return Err("unknown database publication policy");
    };
    let private = matches!(&policy, PublishPolicy::Private);
    if private && !runtime.captures_accepted() {
        return Err("local capture disabled until the enrolled node restarts");
    }
    if super::snapshot::holds_first_backup(&host.home, log.database_id()) {
        return Ok(());
    }
    let material = Zeroizing::new(*key.encryption_bytes());
    let reader = Arc::clone(&runtime);
    let joined = tokio::task::spawn_blocking(move || read_page(&reader, &material, &policy)).await;
    if let Ok(Some(page)) = joined {
        if private {
            let pending = legacy_pending(&page).ok_or("old accepted write metadata unreadable")?;
            store
                .mutation_manager()
                .stage_legacy_replay_pending(&pending)
                .await
                .map_err(|_| "old accepted write could not be protected")?;
        }
        if let Err(reason) = send_page(host, log, &runtime, &key, &page).await {
            tracing::warn!(target: "lastdbd::log_sync", database_id = %log.database_id(),
                reason, "pending local writes were retained");
        }
    }
    if private {
        let status = accepted_writes::status(&runtime);
        if status.gap {
            return Err("local capture has a gap");
        }
        if status.snapshot_required {
            return Err("recovery required for writes from before local capture");
        }
        if status.acked < status.legacy_through {
            return Err("old local writes still await cloud acceptance");
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct AcceptedTarget {
    schema: String,
    key: Option<KeyValue>,
    action: String,
    file: Option<serde::de::IgnoredAny>,
    inline_file: Option<serde::de::IgnoredAny>,
}

type LegacyPending<'a> = (String, Option<KeyValue>, &'a str, bool);

fn legacy_pending(page: &Page) -> Option<Vec<LegacyPending<'_>>> {
    page.items
        .iter()
        .filter_map(|item| match item {
            Item::Publish {
                mutation_id,
                kind,
                canonical,
                authored_at_ms: None,
                ..
            } => Some((mutation_id, kind, canonical)),
            _ => None,
        })
        .map(|(id, kind, canonical)| {
            let target: AcceptedTarget = serde_json::from_slice(canonical).ok()?;
            let attachment_affected = target.file.is_some()
                || target.inline_file.is_some()
                || matches!(target.action.as_str(), "delete" | "purge");
            ((kind == "schema") == target.key.is_none()).then_some((
                target.schema,
                target.key,
                id.as_str(),
                attachment_affected,
            ))
        })
        .collect()
}

enum PublishPolicy {
    Private,
    Share(SpecBinding),
}

fn read_page(runtime: &PackRuntime, key: &[u8; 32], policy: &PublishPolicy) -> Option<Page> {
    let status = accepted_writes::status(runtime);
    if status.gap {
        tracing::warn!(target: "lastdbd::log_sync", "accepted prefix has a gap");
        return None;
    }
    if status.snapshot_required {
        return None;
    }
    let publication = match policy {
        PublishPolicy::Private => Publication::Personal,
        PublishPolicy::Share(spec) => Publication::Share(spec),
    };
    match accepted_writes::prefix(runtime, key, publication, status.acked, PAGE_LIMIT) {
        Ok(page) if !page.snapshot_required => Some(page),
        Ok(_) | Err(Blocked::Snapshot | Blocked::Gap) => None,
        Err(blocked) => {
            tracing::warn!(target: "lastdbd::log_sync", reason = ?blocked, "accepted prefix was not read");
            None
        }
    }
}

async fn send_page(
    host: &Host,
    log: &CloudLog,
    runtime: &Arc<PackRuntime>,
    key: &DatabaseContentKey,
    page: &Page,
) -> Result<(), &'static str> {
    let Some(first) = page.items.first() else {
        return Ok(());
    };
    let first = match first {
        Item::Publish { sequence, .. } | Item::Withheld { sequence, .. } => *sequence,
    };
    let after = first.checked_sub(1).ok_or("invalid local spool position")?;
    let Some(upload) = LocalUpload::open(runtime, log, after).await? else {
        return Ok(());
    };
    let mut next = upload.position.writer_seq + 1;
    let mut entries = Vec::with_capacity(page.items.len());
    let mut through = after;
    for item in &page.items {
        through = match item {
            Item::Withheld { sequence, .. } => *sequence,
            Item::Publish { sequence, .. } => {
                let Some(entry) = prepare_item(host, log, key, item, next).await else {
                    break;
                };
                next = next.max(entry.writer_seq + 1);
                entries.push(entry);
                *sequence
            }
        };
    }
    if through == after {
        return Err("the next local write could not be prepared for upload");
    }
    append_entries(log, entries).await?;
    upload
        .confirm(
            runtime,
            UploadPosition {
                writer_seq: next - 1,
                spool_through: through,
            },
        )
        .await
}

async fn prepare_item(
    host: &Host,
    log: &CloudLog,
    key: &DatabaseContentKey,
    item: &Item,
    next: u64,
) -> Option<Entry> {
    let Item::Publish {
        mutation_id,
        kind,
        canonical,
        authored_at_ms,
        file_ciphertext,
        file_reference,
        ..
    } = item
    else {
        return None;
    };
    let namespace = namespace_of(canonical)?;
    let saved = retry::load(&host.home, log.database_id(), mutation_id).ok()?;
    let source = upload::FileSource {
        canonical,
        row: file_ciphertext.as_deref(),
        reference: file_reference.as_deref(),
    };
    let sealed = match saved {
        Some((seal, payload))
            if seal.writer_seq < u64::MAX
                && seal.writer_seq <= next
                && seal.namespace == namespace =>
        {
            Some((seal, payload))
        }
        None if next < u64::MAX => {
            let seal = Seal {
                writer_seq: next,
                authored_at_ms: authored_at_ms.unwrap_or_else(fold_db::clock::unix_millis),
                namespace,
            };
            prepare_payload(host, log, key, mutation_id, kind, source, seal).await
        }
        _ => None,
    };
    let (seal, payload) = sealed?;
    cloud_entry(log, &seal, payload)
}

async fn prepare_payload(
    host: &Host,
    log: &CloudLog,
    key: &DatabaseContentKey,
    mutation_id: &str,
    kind: &str,
    source: upload::FileSource<'_>,
    seal: Seal,
) -> Option<(Seal, Vec<u8>)> {
    let (canonical, seal) = if source.row.is_some() || source.reference.is_some() {
        upload::cloud_body(host, log, key, mutation_id, source, seal).await?
    } else if kind == "file" {
        return None;
    } else {
        (source.canonical.to_vec(), seal)
    };
    let envelope = log_envelope::seal(
        key,
        &LogBinding {
            database_id: log.database_id(),
            writer_key: log.principal().as_str(),
            writer_device: log.device().as_str(),
            writer_seq: seal.writer_seq,
            authored_at_ms: seal.authored_at_ms,
            namespace: &seal.namespace,
            incorporated_from: None,
        },
        &canonical,
    )
    .ok()?;
    retry::store(&host.home, log.database_id(), mutation_id, &seal, &envelope).ok()?;
    Some((seal, envelope))
}

fn cloud_entry(log: &CloudLog, seal: &Seal, payload: Vec<u8>) -> Option<Entry> {
    Some(Entry {
        database_id: DatabaseId::new(log.database_id()).ok()?,
        writer: log.reader(),
        writer_seq: seal.writer_seq,
        authored_at_ms: seal.authored_at_ms,
        namespace: Namespace::new(&seal.namespace).ok()?,
        incorporated_from: None,
        payload,
    })
}

async fn append_entries(log: &CloudLog, entries: Vec<Entry>) -> Result<(), &'static str> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut log = log.clone();
    tokio::task::spawn_blocking(move || {
        // A byte-limited retry must keep its original ciphertext and sequence.
        let mut pending = vec![entries];
        while let Some(mut entries) = pending.pop() {
            let Some(through) = entries.last().map(|entry| entry.writer_seq) else {
                continue;
            };
            let mut batch = AppendBatch {
                writer: log.reader(),
                entries,
                accepted_at_ms: fold_db::clock::unix_millis(),
            };
            let body = wire::append_request(&batch).map_err(|_| "invalid local upload batch")?;
            if body.len() as u64 > CONTROL_JSON_MAX_BYTES {
                entries = std::mem::take(&mut batch.entries);
                if entries.len() == 1 {
                    return Err("signed entry exceeds the request limit");
                }
                pending.push(entries.split_off(entries.len() / 2));
                pending.push(entries);
                continue;
            }
            match MutationLog::append(&mut log, batch) {
                Ok(AppendOutcome::Accepted { delivery_seqs }) if !delivery_seqs.is_empty() => {}
                Ok(AppendOutcome::Duplicate { next_seq }) if next_seq > through => {}
                Err(LogError::Conflict) => {
                    return Err("cloud already has a different edit at this local writer sequence")
                }
                _ => return Err("cloud did not confirm the upload"),
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| "local upload sender stopped")?
}

fn publication(host: &Host, database_id: &str) -> Option<PublishPolicy> {
    let database = registry::databases(&host.home)
        .into_iter()
        .find(|database| database.id == database_id)?;
    match database.kind.as_str() {
        "personal" => Some(PublishPolicy::Private),
        "shareable" => {
            let spec = database.spec?;
            SpecBinding::new(&spec.name, spec.version)
                .ok()
                .map(PublishPolicy::Share)
        }
        _ => None,
    }
}

fn namespace_of(canonical: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let schema = value.get("schema").and_then(|item| item.as_str())?;
    let name = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    (!name.is_empty()).then(|| name.to_owned())
}
