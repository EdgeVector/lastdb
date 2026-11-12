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
use crate::host::owned::Resolved;
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};
use crate::named_snapshot_restore;

use super::cloud::CloudLog;
use super::registry;
use super::retry::{self, Seal};
use super::upload;

const PAGE_LIMIT: u32 = 8;

pub(super) async fn publish_owned(host: &Host, log: &CloudLog) {
    let Some(runtime) = owned_runtime(host, log.database_id()) else {
        return;
    };
    let Some(spec) = spec_binding(host, log.database_id()) else {
        return;
    };
    let Some(key) = content_key(host, log.database_id()) else {
        return;
    };
    let material = Zeroizing::new(*key.encryption_bytes());
    let Some(prepared) = prepare(
        runtime.clone(),
        material,
        spec,
        log.database_id(),
        &host.home,
    )
    .await
    else {
        return;
    };
    let _ = send_page(host, log, &runtime, prepared).await;
}

struct Prepared {
    page: Page,
}

async fn prepare(
    runtime: Arc<PackRuntime>,
    key: Zeroizing<[u8; 32]>,
    spec: SpecBinding,
    database_id: &str,
    home: &std::path::Path,
) -> Option<Prepared> {
    let database_id = database_id.to_owned();
    let home = home.to_owned();
    let joined =
        tokio::task::spawn_blocking(move || read_page(runtime, &*key, &spec, &database_id, &home))
            .await;
    joined.ok().flatten()
}

fn read_page(
    runtime: Arc<PackRuntime>,
    key: &[u8; 32],
    spec: &SpecBinding,
    database_id: &str,
    home: &std::path::Path,
) -> Option<Prepared> {
    let status = accepted_writes::status(&runtime);
    if status.gap {
        tracing::warn!(target: "lastdbd::log_sync", "accepted prefix has a gap");
        return None;
    }
    if status.snapshot_required {
        note_if_installed(&runtime, &home, database_id, status.opened_frontier);
        return None;
    }
    match accepted_writes::prefix(
        &runtime,
        key,
        Publication::Share(spec),
        status.acked,
        PAGE_LIMIT,
    ) {
        Ok(page) if page.snapshot_required => {
            note_if_installed(&runtime, home, database_id, status.opened_frontier);
            None
        }
        Ok(page) => Some(Prepared { page }),
        Err(Blocked::Snapshot | Blocked::Gap) => {
            note_if_installed(&runtime, home, database_id, status.opened_frontier);
            None
        }
        Err(blocked) => {
            tracing::warn!(target: "lastdbd::log_sync", reason = ?blocked, "accepted prefix was not read");
            None
        }
    }
}

fn note_if_installed(
    runtime: &PackRuntime,
    home: &std::path::Path,
    database_id: &str,
    frontier: u64,
) {
    if named_snapshot_restore::prefix_durable(home, database_id) {
        let _ = accepted_writes::note_snapshot(runtime, frontier);
    }
}

async fn send_page(
    host: &Host,
    log: &CloudLog,
    runtime: &Arc<PackRuntime>,
    prepared: Prepared,
) -> bool {
    if prepared.page.items.is_empty() {
        return false;
    }
    let tip = match writer_tip(log).await {
        Some(tip) => tip,
        None => return false,
    };
    let mut next = tip.saturating_add(1);
    for item in &prepared.page.items {
        if !send_item(host, log, item, &mut next).await {
            return false;
        }
    }
    let through = prepared.page.through;
    let runtime = Arc::clone(runtime);
    let acked =
        tokio::task::spawn_blocking(move || accepted_writes::acknowledge(&runtime, through)).await;
    match acked {
        Ok(Ok(())) => true,
        _ => {
            tracing::warn!(target: "lastdbd::log_sync", "accepted prefix was not acknowledged");
            false
        }
    }
}

async fn send_item(host: &Host, log: &CloudLog, item: &Item, next: &mut u64) -> bool {
    let Item::Publish {
        mutation_id,
        canonical,
        file_ciphertext,
        ..
    } = item
    else {
        return true;
    };
    let published = if let Some(ciphertext) = file_ciphertext {
        let Some(body) = upload::cloud_body(host, log, mutation_id, canonical, ciphertext).await
        else {
            return false;
        };
        body
    } else {
        canonical.clone()
    };
    append_canonical(host, log, mutation_id, &published, next).await
}

async fn append_canonical(
    host: &Host,
    log: &CloudLog,
    mutation_id: &str,
    canonical: &[u8],
    next: &mut u64,
) -> bool {
    let namespace = match namespace_of(canonical) {
        Some(namespace) => namespace,
        None => return false,
    };
    let sealed = match sealed_payload(host, log, mutation_id, canonical, &namespace, next) {
        Some(sealed) => sealed,
        None => return false,
    };
    let entry = match cloud_entry(log, &sealed.0, &namespace, sealed.1) {
        Some(entry) => entry,
        None => return false,
    };
    append_entry(log, entry).await
}

fn sealed_payload(
    host: &Host,
    log: &CloudLog,
    mutation_id: &str,
    canonical: &[u8],
    namespace: &str,
    next: &mut u64,
) -> Option<(Seal, Vec<u8>)> {
    if let Some(saved) = retry::load(&host.home, log.database_id(), mutation_id)
        .ok()
        .flatten()
    {
        if saved.0.writer_seq > *next {
            return None;
        }
        *next = (*next).max(saved.0.writer_seq.saturating_add(1));
        return Some(saved);
    }
    let seal = Seal {
        writer_seq: *next,
        authored_at_ms: fold_db::clock::unix_millis(),
        namespace: namespace.to_owned(),
    };
    let key = content_key(host, log.database_id())?;
    let envelope = log_envelope::seal(
        &key,
        &LogBinding {
            database_id: log.database_id(),
            writer_key: log.principal().as_str(),
            writer_device: log.device().as_str(),
            writer_seq: seal.writer_seq,
            authored_at_ms: seal.authored_at_ms,
            namespace,
            incorporated_from: None,
        },
        canonical,
    )
    .ok()?;
    retry::store(&host.home, log.database_id(), mutation_id, &seal, &envelope).ok()?;
    *next = next.saturating_add(1);
    Some((seal, envelope))
}

fn cloud_entry(log: &CloudLog, seal: &Seal, namespace: &str, payload: Vec<u8>) -> Option<Entry> {
    Some(Entry {
        database_id: DatabaseId::new(log.database_id()).ok()?,
        writer: log.reader(),
        writer_seq: seal.writer_seq,
        authored_at_ms: seal.authored_at_ms,
        namespace: Namespace::new(namespace).ok()?,
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
        Ok(Ok(AppendOutcome::Duplicate { next_seq })) => next_seq == seq.saturating_add(1),
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

fn owned_runtime(host: &Host, database_id: &str) -> Option<Arc<PackRuntime>> {
    match host.owned.resolve(database_id) {
        Ok((Resolved::Owned, Some(store))) => Some(store.db_ops().native_runtime()),
        _ => None,
    }
}

fn spec_binding(host: &Host, database_id: &str) -> Option<SpecBinding> {
    let spec = registry::databases(&host.home)
        .into_iter()
        .find(|database| database.id == database_id)?
        .spec?;
    SpecBinding::new(&spec.name, spec.version).ok()
}

fn content_key(host: &Host, database_id: &str) -> Option<DatabaseContentKey> {
    let seed = host.keypair.secret_key_bytes();
    DatabaseContentKey::owned(&seed, database_id).ok()
}

fn namespace_of(canonical: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let schema = value.get("schema").and_then(|item| item.as_str())?;
    let name = schema
        .split_once('/')
        .map(|(prefix, _)| prefix)
        .unwrap_or(schema);
    (!name.is_empty()).then(|| name.to_owned())
}
