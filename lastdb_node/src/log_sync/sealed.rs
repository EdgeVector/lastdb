//! Seal and open the payloads of the personal database.
//!
//! The relay stores what a node signs, and it must learn nothing about the
//! records in it. Every payload of the personal database is sealed with the
//! database read key before the entry is signed: the record key and value of a
//! `data` entry, the definition of a `schema` entry, and the blob of a `bundle`.
//! There is one mechanism for all three: the AES-256-GCM envelope that LastDB
//! already uses for stored data, `0x01 ‖ nonce(12) ‖ ciphertext ‖ tag(16)`,
//! with a fresh random nonce.
//!
//! The bound data of the envelope is the database id, the node key, the device
//! id and the `writer_seq` of the entry (32 + 32 + 32 + 8 bytes). A ciphertext
//! that was moved to another position, another database or another stream does
//! not open.

use fold_db::crypto::envelope::{decrypt_envelope_with_aad, encrypt_envelope_with_aad};
use mutation_log::{DatabaseId, DeviceId, EntryHeader, NodeKey, SealOpener};
use zeroize::Zeroizing;

use crate::personal_identity;

/// Starts the bound data of a file blob, so it can never equal the 104 bytes
/// that bind an entry payload.
const FILE_LABEL: &[u8] = b"lastdb:file-blob:v1:";

/// The key and database that seal and open payloads.
pub(crate) struct Sealer {
    key: Zeroizing<[u8; 32]>,
    database: DatabaseId,
}

impl Sealer {
    /// The sealer of the personal database of the node with this seed.
    pub(crate) fn for_node_seed(node_seed: &[u8; 32]) -> Result<Self, String> {
        let id = personal_identity::database_id(node_seed);
        Ok(Self {
            key: personal_identity::read_key(node_seed, &id),
            database: DatabaseId::from_hex(&id).map_err(|_| "personal database id is not hex32")?,
        })
    }

    /// The database this sealer belongs to.
    pub(crate) fn database(&self) -> DatabaseId {
        self.database
    }

    /// Seal `plain` for the entry that this node will sign at this position.
    pub(crate) fn seal(
        &self,
        node_key: NodeKey,
        device_id: DeviceId,
        writer_seq: u64,
        plain: &[u8],
    ) -> Result<Vec<u8>, String> {
        let aad = self.bound_data(&node_key, &device_id, writer_seq);
        encrypt_envelope_with_aad(&self.key, &aad, plain).map_err(|e| e.to_string())
    }

    /// Seal the bytes of a file for the blob that carries them.
    ///
    /// The bound data is the database id and the SHA-256 of the plaintext, not
    /// an entry position. A record that keeps its file in a later write names
    /// the same blob, and a new device opens it knowing only the record's file
    /// reference, so the blob cannot belong to one position. The database id
    /// keeps it from opening under another database, and the digest ties the
    /// ciphertext to the one plaintext that the record names.
    pub(crate) fn seal_file(&self, plain: &[u8], sha256: &[u8; 32]) -> Result<Vec<u8>, String> {
        encrypt_envelope_with_aad(&self.key, &self.file_bound_data(sha256), plain)
            .map_err(|e| e.to_string())
    }

    /// The plaintext of a file blob made by [`Self::seal_file`], or `None` when
    /// it does not open under this database and this digest.
    pub(crate) fn open_file(&self, sealed: &[u8], sha256: &[u8; 32]) -> Option<Vec<u8>> {
        decrypt_envelope_with_aad(&self.key, &self.file_bound_data(sha256), sealed).ok()
    }

    fn file_bound_data(&self, sha256: &[u8; 32]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(FILE_LABEL.len() + 64);
        aad.extend_from_slice(FILE_LABEL);
        aad.extend_from_slice(self.database.as_bytes());
        aad.extend_from_slice(sha256);
        aad
    }

    fn bound_data(&self, node_key: &NodeKey, device_id: &DeviceId, writer_seq: u64) -> Vec<u8> {
        let mut aad = Vec::with_capacity(104);
        aad.extend_from_slice(self.database.as_bytes());
        aad.extend_from_slice(node_key.as_bytes());
        aad.extend_from_slice(device_id.as_bytes());
        aad.extend_from_slice(&writer_seq.to_be_bytes());
        aad
    }
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Sealer")
    }
}

impl SealOpener for Sealer {
    fn open(&self, header: &EntryHeader, sealed: &[u8]) -> Option<Vec<u8>> {
        let aad = self.bound_data(&header.node_key, &header.device_id, header.writer_seq);
        decrypt_envelope_with_aad(&self.key, &aad, sealed).ok()
    }
}
