//! Attachment intent on an owned shared-database write mirrored to history.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::FileWrite;
use mutation_log::{DeviceId, Hex32, MutationFileRef, NodeKey};
use signed_history_materialize::FileSection;

use super::history_store::HistoryMap;
use super::record_files;
use crate::personal_seal::Sealer;

/// External file intent for one owned mutation.
#[derive(Clone, Debug)]
pub enum OwnedFile {
    /// Leave the live external file alone.
    Keep,
    /// Clear the external file.
    Clear,
    /// Set the external file; `plain` is the plaintext to seal for the relay.
    Put { marker: FileMarker, plain: Vec<u8> },
}

/// Inline file intent for one owned mutation.
#[derive(Clone, Debug)]
pub enum OwnedInline {
    /// Leave the live inline file alone.
    Keep,
    /// Clear the inline file.
    Clear,
    /// Set the inline file (bytes travel inside the sealed payload).
    Put(InlineFile),
}

impl OwnedFile {
    /// Build from a committed mutation's file write.
    pub fn from_mutation(
        file_write: Option<&FileWrite>,
        marker: Option<&FileMarker>,
    ) -> Result<Self, String> {
        match file_write {
            None => Ok(Self::Keep),
            Some(FileWrite::Clear) => Ok(Self::Clear),
            Some(FileWrite::LocalPath(local)) => {
                let marker = marker.cloned().unwrap_or_else(|| local.marker().clone());
                let plain = std::fs::read(local.path()).map_err(|error| {
                    format!("cannot read linked file for signed history: {error}")
                })?;
                if plain.len() as u64 != marker.size_bytes {
                    return Err("linked file size does not match its marker".into());
                }
                Ok(Self::Put { marker, plain })
            }
            Some(FileWrite::Put(_)) => Err(
                "shared history sealing expects a linked local file path, not a sealed row".into(),
            ),
        }
    }

    /// When a write omits the file, history still needs the retained attachment
    /// so a late joiner materializes the same record. Take the prior history
    /// Put's marker and the live store's plaintext.
    pub async fn resolve_keep(
        self,
        histories: &HistoryMap,
        database_id: &str,
        store: &FoldDB,
        schema: &str,
        record_key: &str,
    ) -> Result<Self, String> {
        if !matches!(self, Self::Keep) {
            return Ok(self);
        }
        let Some(marker) = prior_file_put(histories, database_id, schema, record_key)? else {
            return Ok(Self::Keep);
        };
        let key = signed_history_materialize::key_value(store, schema, record_key)?;
        if let Some(local) = store
            .record_file_local_path(schema, &key)
            .await
            .map_err(|e| e.to_string())?
        {
            if local.intact {
                let plain = std::fs::read(&local.path).map_err(|error| {
                    format!("cannot read retained local file for signed history: {error}")
                })?;
                if record_files::is_file_of(&plain, &marker) {
                    return Ok(Self::Put { marker, plain });
                }
            }
        }
        if let Some((owned_marker, plain)) = store
            .record_file_owned_bytes(schema, &key)
            .await
            .map_err(|e| e.to_string())?
        {
            if owned_marker == marker {
                return Ok(Self::Put { marker, plain });
            }
        }
        Err("retained file bytes are not available to seal into signed history".into())
    }

    /// Seal and stage a Put for relay upload; Keep/Clear name no blob.
    pub(super) fn stage(
        &self,
        home: &Path,
        database_id: &str,
        sealer: &Sealer,
        node_key: NodeKey,
        device_id: DeviceId,
        writer_seq: u64,
    ) -> Result<Option<MutationFileRef>, String> {
        let Self::Put { marker, plain } = self else {
            return Ok(None);
        };
        let staged = record_files::seal_and_stage(
            &record_files::SealStage {
                home,
                database_id,
                sealer,
                node_key,
                device_id,
                writer_seq,
            },
            plain,
            marker.clone(),
        )?;
        if staged.marker != *marker
            || staged.bytes != marker.size_bytes + record_files::ENVELOPE_OVERHEAD
            || !staged.path.is_file()
        {
            return Err("staged sealed blob does not match its marker".into());
        }
        Ok(Some(MutationFileRef {
            file_handle: "file".to_owned(),
            sha256: Hex32::from_hex(&staged.sha256)
                .map_err(|_| "staged blob digest is not hex32")?,
        }))
    }
}

impl OwnedInline {
    pub fn from_mutation(inline_write: Option<&InlineWrite>) -> Self {
        match inline_write {
            None => Self::Keep,
            Some(InlineWrite::Clear) => Self::Clear,
            Some(InlineWrite::Put(file)) => Self::Put(file.clone()),
        }
    }

    pub fn as_write(&self) -> Option<InlineWrite> {
        match self {
            Self::Keep => None,
            Self::Clear => Some(InlineWrite::Clear),
            Self::Put(file) => Some(InlineWrite::Put(file.clone())),
        }
    }

    /// Resolve Keep to the prior history winner's inline Put so the winning
    /// value carries the retained thumbnail for late joiners.
    pub fn resolve_keep(
        self,
        histories: &HistoryMap,
        database_id: &str,
        schema: &str,
        record_key: &str,
    ) -> Result<Self, String> {
        if !matches!(self, Self::Keep) {
            return Ok(self);
        }
        Ok(
            match prior_inline_put(histories, database_id, schema, record_key)? {
                Some(file) => Self::Put(file),
                None => Self::Keep,
            },
        )
    }
}

fn prior_decoded(
    histories: &HistoryMap,
    database_id: &str,
    schema: &str,
    record_key: &str,
) -> Result<Option<signed_history_materialize::DecodedRecord>, String> {
    let history = histories.snapshot(database_id)?;
    let Some(winner) = history
        .records()
        .get(&(schema.to_owned(), record_key.to_owned()))
    else {
        return Ok(None);
    };
    signed_history_materialize::decode_record_value(&winner.value)
}

fn prior_file_put(
    histories: &HistoryMap,
    database_id: &str,
    schema: &str,
    record_key: &str,
) -> Result<Option<FileMarker>, String> {
    Ok(
        match prior_decoded(histories, database_id, schema, record_key)? {
            Some(decoded) => match decoded.file {
                FileSection::Put(marker) => Some(marker),
                FileSection::Keep | FileSection::Clear => None,
            },
            None => None,
        },
    )
}

fn prior_inline_put(
    histories: &HistoryMap,
    database_id: &str,
    schema: &str,
    record_key: &str,
) -> Result<Option<InlineFile>, String> {
    Ok(
        match prior_decoded(histories, database_id, schema, record_key)? {
            Some(decoded) => match decoded.inline {
                Some(InlineWrite::Put(file)) => Some(file),
                Some(InlineWrite::Clear) | None => None,
            },
            None => None,
        },
    )
}
