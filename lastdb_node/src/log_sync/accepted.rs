//! Publish one page of the durable accepted prefix.
//!
//! The reader is `accepted_writes`. A page is acknowledged locally only after
//! every item is published or withheld. Cloud delivery progress stays in `run`.

mod source;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use fold_db::storage::disk_backup::accepted::UploadPosition;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{AppendBatch, DatabaseId, Entry, Namespace};
use zeroize::Zeroizing;

use crate::accepted_writes::{self, Item, Page};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use source::{capture_block, namespace_of, publication};

pub(super) use source::{read_page, LocalUpload, PublishPolicy};

use super::cloud::CloudLog;
use super::http::paths;
use super::session::retry::{self, Seal};
use super::{upload, wire};

pub(super) fn take_payment_hold(database_id: &str) -> bool {
    payment_holds().remove(database_id)
}

fn mark_payment_hold(database_id: &str) {
    payment_holds().insert(database_id.to_owned());
}

fn payment_holds() -> std::sync::MutexGuard<'static, HashSet<String>> {
    static HOLDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    HOLDS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) async fn publish_owned(host: &Host, log: &CloudLog) -> Result<(), &'static str> {
    let (store, key, policy, received) =
        match DatabaseContentKey::local_store(host, log.database_id()) {
            Ok(Some((store, key))) => {
                let Some(policy) = publication(host, log.database_id()) else {
                    return Err("unknown database publication policy");
                };
                (store, key, policy, false)
            }
            Ok(None) => {
                let (store, key, policy) = super::direct::received_store(host, log.database_id())
                    .await
                    .ok_or("local database or content key unavailable")?;
                (store, key, policy, true)
            }
            Err(_) => return Err("local database or content key unavailable"),
        };
    let runtime = store.db_ops().native_runtime();
    let private = matches!(&policy, PublishPolicy::Private);
    if private && !runtime.captures_accepted() {
        return Err("local capture disabled until the enrolled node restarts");
    }
    let material = Zeroizing::new(*key.encryption_bytes());
    let reader = Arc::clone(&runtime);
    let report = received.then(|| log.database_id().to_owned());
    let joined = tokio::task::spawn_blocking(move || {
        read_page(&reader, &material, &policy, report.as_deref())
    })
    .await;
    let failure = match joined {
        Ok(Ok(Some(page))) => send_page(host, log, &runtime, &key, &page, private)
            .await
            .err(),
        Ok(Ok(None)) => None,
        Ok(Err(reason)) => Some(reason),
        Err(_) => Some("the local upload queue reader stopped"),
    };
    if let Some(reason) = failure {
        tracing::warn!(target: "lastdbd::log_sync", database_id = %log.database_id(),
            reason, "pending local writes were retained");
    }
    let blocked = if private {
        let status = accepted_writes::status(&runtime);
        let blocked = capture_block(&status);
        host.cloud_sync.record_upload(
            log.database_id(),
            status.head,
            status.acked,
            failure.or(blocked),
        );
        blocked
    } else {
        None
    };
    if failure == Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE) {
        return Err(crate::cloud::PAYMENT_REQUIRED_MESSAGE);
    }
    if received {
        if let Some(reason) = failure {
            super::publish_state::note_failure(log.database_id(), reason);
        }
    }
    blocked.map_or(Ok(()), Err)
}

async fn send_page(
    host: &Host,
    log: &CloudLog,
    runtime: &Arc<PackRuntime>,
    key: &DatabaseContentKey,
    page: &Page,
    private: bool,
) -> Result<(), &'static str> {
    let received = matches!(
        DatabaseContentKey::local_store(host, log.database_id()),
        Ok(None)
    );
    let Some(first) = page.items.first() else {
        let status = accepted_writes::status(runtime);
        if received && status.head > status.acked {
            super::publish_state::note_word(log.database_id(), "empty_page");
        }
        return Ok(());
    };
    let first = match first {
        Item::Publish { sequence, .. } | Item::Withheld { sequence } => *sequence,
    };
    let after = first.checked_sub(1).ok_or("invalid local spool position")?;
    let Some(upload) = LocalUpload::open(runtime, log, after, received).await? else {
        return Ok(());
    };
    let mut next = upload.position.writer_seq + 1;
    let mut entries = Vec::with_capacity(page.items.len());
    let mut blobs = HashMap::new();
    let mut through = after;
    for item in &page.items {
        through = match item {
            Item::Withheld { sequence } => *sequence,
            Item::Publish {
                sequence,
                mutation_id,
                canonical,
                file_ciphertext,
                file_reference,
                ..
            } => match prepare_item(host, log, key, item, next).await {
                Ok(entry) => {
                    if private && (file_ciphertext.is_some() || file_reference.is_some()) {
                        if let Ok(path) =
                            super::files::path_for(log.database_id(), mutation_id, canonical)
                        {
                            blobs.insert(entry.writer_seq, path);
                        }
                    }
                    next = next.max(entry.writer_seq + 1);
                    entries.push(entry);
                    *sequence
                }
                Err(err) if err == crate::cloud::PAYMENT_REQUIRED_MESSAGE => {
                    mark_payment_hold(log.database_id());
                    return Err(crate::cloud::PAYMENT_REQUIRED_MESSAGE);
                }
                Err(_) => break,
            },
        };
    }
    if through == after {
        return Err("the next local write could not be prepared for upload");
    }
    if let Err(reason) = append_entries(&host.home, log, entries, &blobs, received).await {
        if reason == super::direct::REFUSED {
            super::direct::undo_page(host, log.database_id(), &page.items, through).await?;
            return upload
                .confirm(
                    runtime,
                    UploadPosition {
                        writer_seq: upload.position.writer_seq,
                        spool_through: through,
                    },
                )
                .await;
        }
        return Err(reason);
    }
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
) -> Result<Entry, &'static str> {
    const FAILED: &str = "the next local write could not be prepared for upload";
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
        return Err(FAILED);
    };
    let namespace = namespace_of(canonical).ok_or(FAILED)?;
    let saved = retry::load(&host.home, log.database_id(), mutation_id).map_err(|_| FAILED)?;
    let source = upload::FileSource {
        canonical,
        row: file_ciphertext.as_deref(),
        reference: file_reference.as_deref(),
    };
    let (seal, payload) = match saved {
        Some((seal, payload))
            if seal.writer_seq < u64::MAX
                && seal.writer_seq <= next
                && seal.namespace == namespace =>
        {
            (seal, payload)
        }
        None if next < u64::MAX => {
            let seal = Seal {
                writer_seq: next,
                authored_at_ms: authored_at_ms.unwrap_or_else(fold_db::clock::unix_millis),
                namespace,
            };
            prepare_payload(host, log, key, mutation_id, kind, source, seal).await?
        }
        _ => return Err(FAILED),
    };
    cloud_entry(log, &seal, payload).ok_or(FAILED)
}

async fn prepare_payload(
    host: &Host,
    log: &CloudLog,
    key: &DatabaseContentKey,
    mutation_id: &str,
    kind: &str,
    source: upload::FileSource<'_>,
    seal: Seal,
) -> Result<(Seal, Vec<u8>), &'static str> {
    const FAILED: &str = "the next local write could not be prepared for upload";
    let (canonical, seal) = if source.row.is_some() || source.reference.is_some() {
        upload::cloud_body(host, log, key, mutation_id, source, seal).await?
    } else if kind == "file" {
        return Err(FAILED);
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
    .map_err(|_| FAILED)?;
    retry::store(&host.home, log.database_id(), mutation_id, &seal, &envelope)
        .map_err(|_| FAILED)?;
    Ok((seal, envelope))
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

async fn append_entries(
    home: &Path,
    log: &CloudLog,
    entries: Vec<Entry>,
    blobs: &HashMap<u64, BlobPath>,
    received: bool,
) -> Result<(), &'static str> {
    if entries.is_empty() {
        return Ok(());
    }
    // A byte-limited retry must keep its original ciphertext and sequence.
    let mut pending = vec![entries];
    let mut committed = false;
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
        let Ok((status, reply)) = log
            .post_status_limit(
                paths::APPEND,
                body,
                super::http::Proof::Data,
                super::http::JSON_LIMIT,
            )
            .await
        else {
            if received {
                super::publish_state::note_unreachable(log.database_id(), home, log.environment());
            }
            return Err("cloud did not confirm the upload");
        };
        match super::publish_state::classify(
            received,
            log.database_id(),
            status,
            &reply,
            &batch,
            through,
            committed,
        ) {
            super::publish_state::Step::Accepted(delivery_seqs) => {
                committed = true;
                if !blobs.is_empty() {
                    let start = batch.entries.len() - delivery_seqs.len();
                    let committed: Vec<_> = batch.entries[start..]
                        .iter()
                        .zip(&delivery_seqs)
                        .filter_map(|(entry, &delivery_seq)| {
                            blobs
                                .get(&entry.writer_seq)
                                .map(|path| (delivery_seq, path.clone()))
                        })
                        .collect();
                    super::blob_retention::record_personal(home, log, &committed)
                        .map_err(|_| "local blob retention ledger could not be stored")?;
                }
            }
            super::publish_state::Step::Duplicate => committed = true,
            super::publish_state::Step::Payment => {
                mark_payment_hold(log.database_id());
                return Err(crate::cloud::PAYMENT_REQUIRED_MESSAGE);
            }
            super::publish_state::Step::Refused => return Err(super::direct::REFUSED),
            super::publish_state::Step::Conflict => {
                return Err("cloud already has a different edit at this local writer sequence");
            }
            super::publish_state::Step::Unconfirmed => {
                return Err("cloud did not confirm the upload");
            }
        }
    }
    Ok(())
}
