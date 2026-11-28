//! Attachment intent on an owned shared-database write mirrored to history.

use std::path::Path;

use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::FileWrite;
use mutation_log::{DeviceId, Hex32, MutationFileRef, NodeKey};
use signed_history_materialize::FileSection;

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

    pub fn section(&self) -> FileSection {
        match self {
            Self::Keep => FileSection::Keep,
            Self::Clear => FileSection::Clear,
            Self::Put { marker, .. } => FileSection::Put(marker.clone()),
        }
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
}
