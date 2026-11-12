//! Publish one page of the durable accepted prefix.
//!
//! The reader is `accepted_writes`. A page is acknowledged locally only after
//! every item is published or withheld. Cloud delivery ack stays in `run`.

use std::sync::Arc;

use database_policy::SpecBinding;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{AppendBatch, AppendOutcome, DatabaseId, Entry, MutationLog, Namespace};
use zeroize::Zeroizing;

use crate::accepted_writes::{self, Blocked, Item, Page, Publication};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use super::cloud::CloudLog;
use super::registry;
use super::retry::{self, Seal};
use super::upload;

const PAGE_LIMIT: u32 = 8;

pub(super) async fn publish_owned(host: &Host, log: &CloudLog) {
    let Ok(Some((store, key))) = DatabaseContentKey::local_store(host, log.database_id()) else {
        return;
    };
    let runtime = store.db_ops().native_runtime();
    let Some(policy) = publication(host, log.database_id()) else {
        return;
    };
    let material = Zeroizing::new(*key.encryption_bytes());
    let reader = Arc::clone(&runtime);
    let joined = tokio::task::spawn_blocking(move || read_page(&reader, &material, &policy)).await;
    let Ok(Some(prepared)) = joined else {
        return;
    };
    let _ = send_page(host, log, &runtime, &key, prepared).await;
}

struct Prepared {
    page: Page,
}

enum PublishPolicy {
    Private,
    Share(SpecBinding),
}

fn read_page(runtime: &PackRuntime, key: &[u8; 32], policy: &PublishPolicy) -> Option<Prepared> {
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
        Ok(page) if !page.snapshot_required => Some(Prepared { page }),
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
    prepared: Prepared,
) -> bool {
    if prepared.page.items.is_empty() {
        return false;
    }
    let Some(tip) = writer_tip(log).await else {
        return false;
    };
    let mut next = tip.saturating_add(1);
    for item in &prepared.page.items {
        if !send_item(host, log, key, item, &mut next).await {
            return false;
        }
    }
    let through = prepared.page.through;
    let runtime = Arc::clone(runtime);
    let acked =
        tokio::task::spawn_blocking(move || accepted_writes::acknowledge(&runtime, through)).await;
    if let Ok(Ok(())) = acked {
        true
    } else {
        tracing::warn!(target: "lastdbd::log_sync", "accepted prefix was not acknowledged");
        false
    }
}

async fn send_item(
    host: &Host,
    log: &CloudLog,
    key: &DatabaseContentKey,
    item: &Item,
    next: &mut u64,
) -> bool {
    let Item::Publish {
        mutation_id,
        kind,
        canonical,
        file_ciphertext,
        file_reference,
        ..
    } = item
    else {
        return true;
    };
    let Some(namespace) = namespace_of(canonical) else {
        return false;
    };
    let Ok(saved) = retry::load(&host.home, log.database_id(), mutation_id) else {
        return false;
    };
    let source = upload::FileSource {
        canonical,
        row: file_ciphertext.as_deref(),
        reference: file_reference.as_deref(),
    };
    let sealed = match saved {
        Some((seal, payload)) if seal.writer_seq <= *next && seal.namespace == namespace => {
            Some((seal, payload))
        }
        Some(_) => None,
        None => {
            let seal = Seal {
                writer_seq: *next,
                authored_at_ms: fold_db::clock::unix_millis(),
                namespace,
            };
            prepare_payload(host, log, key, mutation_id, kind, source, seal).await
        }
    };
    let Some((seal, payload)) = sealed else {
        return false;
    };
    let Some(entry) = cloud_entry(log, &seal, payload) else {
        return false;
    };
    if !append_entry(log, entry).await {
        return false;
    }
    *next = (*next).max(seal.writer_seq.saturating_add(1));
    true
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

async fn append_entry(log: &CloudLog, entry: Entry) -> bool {
    let seq = entry.writer_seq;
    let mut log = log.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let writer = log.reader();
        MutationLog::append(
            &mut log,
            AppendBatch {
                writer,
                entries: vec![entry],
                accepted_at_ms: fold_db::clock::unix_millis(),
            },
        )
    })
    .await;
    match joined {
        Ok(Ok(AppendOutcome::Accepted { .. })) => true,
        Ok(Ok(AppendOutcome::Duplicate { next_seq })) => next_seq > seq,
        _ => false,
    }
}

async fn writer_tip(log: &CloudLog) -> Option<u64> {
    let log = log.clone();
    let joined = tokio::task::spawn_blocking(move || {
        MutationLog::writer_tip(&log, log.principal(), &log.reader())
    })
    .await;
    joined.ok()?.ok()
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
