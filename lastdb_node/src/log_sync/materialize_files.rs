//! Fetch and seal external file Puts while materializing shared history.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::FileWrite;
use mutation_log::{EntryHeader, MaterializedRecord, SealOpener, SignedEntry, VerifiedHistory};
use signed_history_materialize::FileSection;

use super::record_files;
use super::relay::RelayClient;
use crate::personal_seal::Sealer;

/// Inputs needed to fetch and apply one external file Put from history.
pub(super) struct FileApply<'a> {
    pub store: &'a FoldDB,
    pub history: &'a VerifiedHistory,
    pub client: &'a RelayClient,
    pub sealer: &'a Sealer,
    pub home: &'a Path,
    pub database_id: &'a str,
}

/// Apply one history winner, fetching and sealing an external file Put first.
pub(super) async fn apply_with_file(
    ctx: &FileApply<'_>,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
) -> Result<(), String> {
    if winner.value.is_empty() {
        return signed_history_materialize::apply_record_winner(
            ctx.store, namespace, record_key, winner,
        )
        .await;
    }
    // Invalid JSON falls through to apply_record_winner, which drops the
    // winner the same way plan_winner does, instead of stalling materialize.
    let decoded = match signed_history_materialize::decode_record_value(&winner.value) {
        Ok(Some(decoded)) => decoded,
        Ok(None) | Err(_) => {
            return signed_history_materialize::apply_record_winner(
                ctx.store, namespace, record_key, winner,
            )
            .await;
        }
    };
    match &decoded.file {
        FileSection::Put(marker) => {
            let write = fetch_and_seal(ctx, winner, marker).await?;
            signed_history_materialize::apply_winner_with_file(
                ctx.store, namespace, record_key, winner, write,
            )
            .await
        }
        FileSection::Keep | FileSection::Clear => {
            signed_history_materialize::apply_record_winner(
                ctx.store, namespace, record_key, winner,
            )
            .await
        }
    }
}

async fn fetch_and_seal(
    ctx: &FileApply<'_>,
    winner: &MaterializedRecord,
    marker: &FileMarker,
) -> Result<FileWrite, String> {
    let meta = ctx
        .history
        .get_accepted(&winner.entry)
        .ok_or_else(|| "winning entry is missing from accepted history".to_owned())?;
    let SignedEntry::Data { header, body } = &meta.entry else {
        return Err("winning entry is not a data entry".into());
    };
    let file = body
        .file
        .as_ref()
        .ok_or_else(|| "a record sets a file that its entry does not name".to_owned())?;
    let sha256 = file.sha256.to_hex();
    let path = record_files::fetch_incoming(
        ctx.client,
        ctx.home,
        ctx.database_id,
        &sha256,
        marker.size_bytes + record_files::ENVELOPE_OVERHEAD,
    )
    .await?;
    let sealed = std::fs::read(&path)
        .map_err(|error| format!("cannot read fetched record file: {error}"))?;
    let plain = open_blob(ctx.sealer, header, &sealed)?;
    if !record_files::is_file_of(&plain, marker) {
        return Err("the file of a record does not match its entry".into());
    }
    let write = ctx
        .store
        .db_ops()
        .records()
        .seal_record_file(&plain, marker.name.clone(), marker.media_type.clone())
        .map_err(|error| format!("a record file could not be sealed: {error}"))?;
    let _ = std::fs::remove_file(path);
    Ok(write)
}

fn open_blob(sealer: &Sealer, header: &EntryHeader, sealed: &[u8]) -> Result<Vec<u8>, String> {
    SealOpener::open(sealer, header, sealed)
        .ok_or_else(|| "a record file blob does not open".to_owned())
}
