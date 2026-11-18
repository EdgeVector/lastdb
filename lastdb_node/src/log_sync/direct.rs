//! A received database publishes through the owned append path.
//!
//! The seal key is the invitation content key. The writer is this node's key
//! and device, because `CloudLog` is that principal. A refused append restores
//! the preimage and drops the spool item without moving the writer sequence.

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
    match share_mode::write_mode(log).await {
        WriteMode::Direct => accepted::publish_owned(host, log).await,
        WriteMode::Unknown => Ok(()),
        WriteMode::Closed => retract(host, log).await,
    }
}

pub(super) async fn received_store(
    host: &Host,
    database_id: &str,
) -> Option<(Arc<FoldDB>, DatabaseContentKey, PublishPolicy)> {
    let share = registered_share::resolve(&host.home, database_id).ok()??;
    if !share.signed_log || share.id.as_str() != database_id {
        return None;
    }
    let opened = registered_share::verified_invitation(&host.home, &share.id).ok()?;
    let key = DatabaseContentKey::from_invitation(&opened).ok()?;
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
    let Some((store, key, policy)) = received_store(host, log.database_id()).await else {
        return Ok(());
    };
    let material = Zeroizing::new(*key.encryption_bytes());
    let runtime = store.db_ops().native_runtime();
    let reader = Arc::clone(&runtime);
    let joined = tokio::task::spawn_blocking(move || read_page(&reader, &material, &policy)).await;
    let page = match joined {
        Ok(Ok(Some(page))) => page,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(reason)) => return Err(reason),
        Err(_) => return Err("the local upload queue reader stopped"),
    };
    let Some(first) = page.items.first() else {
        return Ok(());
    };
    let through = page
        .items
        .last()
        .map(sequence_of)
        .unwrap_or(sequence_of(first));
    let after = sequence_of(first)
        .checked_sub(1)
        .ok_or("invalid local spool position")?;
    let Some(upload) = LocalUpload::open(&runtime, log, after).await? else {
        return Ok(());
    };
    undo_records(&store, &page.items, through).await?;
    upload
        .confirm(
            &runtime,
            UploadPosition {
                writer_seq: upload.position.writer_seq,
                spool_through: through,
            },
        )
        .await
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
