//! A received database publishes through the owned append path.
//!
//! The seal key is the content key stored for the registration. The writer is
//! this node's key and device, because `CloudLog` is that principal. A link
//! registration has no member entry and does not publish. A refused append
//! restores the preimage and drops the spool item without moving the writer
//! sequence.

use std::sync::Arc;

use database_policy::SpecBinding;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::KeyValue;
use fold_db::storage::disk_backup::accepted::UploadPosition;
use zeroize::Zeroizing;

use crate::accepted_writes::Item;
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::registered_share;

use super::accepted::{self, read_page, LocalUpload, PublishPolicy};
use super::cloud::CloudLog;
use super::share_mode::{self, WriteMode};

pub(super) const REFUSED: &str = "direct write refused";

pub(super) async fn publish_or_retract(host: &Host, log: &CloudLog) -> Result<(), &'static str> {
    let mode = registered_share::resolve(&host.home, log.database_id())
        .ok()
        .flatten()
        .map_or(WriteMode::Unknown, |share| share_mode::mode(host, &share));
    match mode {
        WriteMode::Direct => {
            super::publish_state::begin(log.database_id());
            let result = accepted::publish_owned(host, log).await;
            if let Err(reason) = result {
                super::publish_state::finish_unnoted(host, log, reason);
            }
            result
        }
        WriteMode::Unknown => {
            super::publish_state::note_access_unknown(log.database_id());
            Ok(())
        }
        WriteMode::Closed => retract(host, log).await,
    }
}

pub(super) async fn received_store(
    host: &Host,
    database_id: &str,
) -> Option<(Arc<FoldDB>, DatabaseContentKey, PublishPolicy)> {
    let share = registered_share::resolve(&host.home, database_id).ok()??;
    // A link has no member entry and never appends, so it has nothing to seal.
    if share.link || !share.signed_log || share.id.as_str() != database_id {
        return None;
    }
    let material = registered_share::content_material(&host.home, &share.id).ok()?;
    let key = DatabaseContentKey::from_stored(database_id, &material).ok()?;
    if key.database_id() != database_id {
        return None;
    }
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .ok()?;
    Some((
        store,
        key,
        PublishPolicy::Share(SpecBinding::whole_database()),
    ))
}

pub(super) async fn undo_page(
    host: &Host,
    database_id: &str,
    items: &[Item],
    through: u64,
) -> Result<(), &'static str> {
    let (store, _, _) = received_store(host, database_id)
        .await
        .ok_or("refused write was not undone")?;
    undo_records(&store, items, through).await
}

async fn retract(host: &Host, log: &CloudLog) -> Result<(), &'static str> {
    super::publish_state::begin(log.database_id());
    super::publish_state::clear_confirmed(log.database_id());
    let Some((store, key, policy)) = received_store(host, log.database_id()).await else {
        return Ok(());
    };
    let material = Zeroizing::new(*key.encryption_bytes());
    let runtime = store.db_ops().native_runtime();
    let reader = Arc::clone(&runtime);
    let reported = log.database_id().to_owned();
    let joined = tokio::task::spawn_blocking(move || {
        read_page(&reader, &material, &policy, Some(&reported))
    })
    .await;
    let page = match joined {
        Ok(Ok(Some(page))) => page,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(reason)) => return note_stop(log, reason),
        Err(_) => return note_stop(log, "the local upload queue reader stopped"),
    };
    let Some(first) = page.items.first() else {
        let status = crate::accepted_writes::status(&runtime);
        if status.head > status.acked {
            super::publish_state::note_word(log.database_id(), "empty_page");
        }
        return Ok(());
    };
    let through = sequence_of(page.items.last().unwrap_or(first));
    let Some(after) = sequence_of(first).checked_sub(1) else {
        return note_stop(log, "invalid local spool position");
    };
    let upload = match LocalUpload::open(&runtime, log, after, true).await {
        Ok(Some(upload)) => upload,
        Ok(None) => return Ok(()),
        Err(reason) => return note_stop(log, reason),
    };
    if let Err(reason) = undo_records(&store, &page.items, through).await {
        return note_stop(log, reason);
    }
    if let Err(reason) = upload
        .confirm(
            &runtime,
            UploadPosition {
                writer_seq: upload.position.writer_seq,
                spool_through: through,
            },
        )
        .await
    {
        return note_stop(log, reason);
    }
    Ok(())
}

fn note_stop(log: &CloudLog, reason: &'static str) -> Result<(), &'static str> {
    super::publish_state::note_failure(log.database_id(), reason);
    Err(reason)
}

async fn undo_records(store: &FoldDB, items: &[Item], through: u64) -> Result<(), &'static str> {
    let manager = store.mutation_manager();
    for item in items.iter().rev() {
        if sequence_of(item) > through {
            continue;
        }
        let Item::Publish {
            mutation_id,
            kind,
            canonical,
            ..
        } = item
        else {
            // A withheld item has no body to restore. Leave the page unconfirmed.
            return Err("refused write was not undone");
        };
        if kind == "schema" || !record_kind(kind) {
            return Err("refused write was not undone");
        }
        let Some((schema, key)) = record_address(canonical) else {
            return Err("refused write was not undone");
        };
        if manager
            .undo_refused_local(&schema, &key, mutation_id)
            .await
            .is_err()
        {
            return Err("refused write was not undone");
        }
    }
    Ok(())
}

fn record_kind(kind: &str) -> bool {
    matches!(kind, "record" | "file" | "inline" | "contribution")
}

fn record_address(canonical: &[u8]) -> Option<(String, KeyValue)> {
    let value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let schema = value.get("schema")?.as_str()?.to_owned();
    let key = serde_json::from_value(value.get("key")?.clone()).ok()?;
    Some((schema, key))
}

fn sequence_of(item: &Item) -> u64 {
    match item {
        Item::Publish { sequence, .. } | Item::Withheld { sequence } => *sequence,
    }
}
