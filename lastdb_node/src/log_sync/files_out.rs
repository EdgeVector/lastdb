//! The file of a record, sealed and kept for upload, when this device signs it.
//!
//! The history value of a record names its file by the digest of a sealed blob
//! (see `RecordValue`). The blob is the file's bytes sealed with the database
//! read key: `Sealer::seal_file` explains why its bound data is the database
//! and the plaintext digest and not an entry position. It is kept under
//! `log-sync/<database>/blobs`, and the upload sends it to the relay before the
//! entry that names it. Nothing about the file travels in clear: its name, type
//! and size are inside the sealed value of the record, and the relay sees only
//! the length of the ciphertext.
//!
//! A record that keeps its file through a later write names the same blob, so a
//! change to one field of a record does not seal and upload its file again.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::KeyValue;
use mutation_log::{Hex32, BUNDLE_BLOB_SIZE_MAX};
use signed_history_materialize::{key_value, FileRef, RecordValue};

use super::blobs;
use super::history_store::HistoryMap;
use super::sealed::Sealer;

/// The bytes that the sealing envelope adds to a payload: version, nonce, tag.
const ENVELOPE_OVERHEAD: u64 = 29;

/// The largest file the personal database takes: the sealed blob must fit the
/// relay's blob limit, and chunking a file is not part of the protocol.
pub(crate) const FILE_SIZE_MAX: u64 = BUNDLE_BLOB_SIZE_MAX - ENVELOPE_OVERHEAD;

/// What a device needs to seal and keep the files of one database.
pub(super) struct FileKeeper<'a> {
    pub(super) sealer: &'a Sealer,
    pub(super) home: &'a Path,
    pub(super) database_id: &'a str,
    pub(super) histories: &'a HistoryMap,
}

impl FileKeeper<'_> {
    /// The reference that the value of `schema/record_key` carries for the file
    /// that `marker` describes.
    pub(super) async fn file_ref(
        &self,
        store: &FoldDB,
        schema: &str,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<FileRef, String> {
        let sha256 = Hex32::from_hex(&marker.sha256).map_err(|e| e.to_string())?;
        let blob = match self.held_blob(schema, record_key, &sha256)? {
            Some(blob) => blob,
            None => {
                self.keep(store, schema, record_key, marker, &sha256)
                    .await?
            }
        };
        Ok(FileRef {
            blob,
            sha256,
            size_bytes: marker.size_bytes,
            name: marker.name.clone(),
            media_type: marker.media_type.clone(),
        })
    }

    /// The blob that the record's current winner already names for this file.
    fn held_blob(
        &self,
        schema: &str,
        record_key: &str,
        sha256: &Hex32,
    ) -> Result<Option<Hex32>, String> {
        let key = (schema.to_owned(), record_key.to_owned());
        let value = self.histories.with_mut(self.database_id, |history| {
            history
                .records()
                .get(&key)
                .map(|winner| winner.value.clone())
        })?;
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let held = RecordValue::decode(&value)?.file;
        Ok(held
            .filter(|file| file.sha256 == *sha256)
            .map(|file| file.blob))
    }

    /// Read the file from the store, seal it and keep the blob.
    async fn keep(
        &self,
        store: &FoldDB,
        schema: &str,
        record_key: &str,
        marker: &FileMarker,
        sha256: &Hex32,
    ) -> Result<Hex32, String> {
        let at: KeyValue = key_value(store, schema, record_key)?;
        let found = store
            .record_file_bytes(schema, &at)
            .await
            .map_err(|e| format!("read the file of {schema}/{record_key}: {e}"))?;
        let Some((held, bytes)) = found else {
            return Err(format!(
                "the file of {schema}/{record_key} is not on this node"
            ));
        };
        if held != *marker || blobs::digest(&bytes) != *sha256 {
            return Err(format!(
                "the file of {schema}/{record_key} changed while it was signed"
            ));
        }
        let sealed = self.sealer.seal_file(&bytes, sha256.as_bytes())?;
        if !u64::try_from(sealed.len()).is_ok_and(|size| size <= BUNDLE_BLOB_SIZE_MAX) {
            return Err("a file is too large for one relay blob".into());
        }
        let blob = blobs::digest(&sealed);
        blobs::save(self.home, self.database_id, &blob, &sealed)?;
        Ok(blob)
    }
}
