//! Seal and open the payloads of the personal database.
//!
//! The relay stores what a node signs, and it must learn nothing about the
//! records in it. Every payload of the personal database is sealed with the
//! database read key before the entry is signed: the accepted body of a `data`
//! entry (a record write with its alternate keys, a schema put or drop) and the
//! blob of a file that a record names. There is one mechanism for both: the
//! AES-256-GCM envelope that LastDB already uses for stored data,
//! `0x01 ‖ nonce(12) ‖ ciphertext ‖ tag(16)`, with a fresh random nonce.
//!
//! The bound data of the envelope is the database id, the node key, the device
//! id and the `writer_seq` of the entry (32 + 32 + 32 + 8 bytes). A ciphertext
//! that was moved to another position, another database or another stream does
//! not open.

use fold_db::crypto::envelope::{decrypt_envelope_with_aad, encrypt_envelope_with_aad};
use mutation_log::{DatabaseId, DeviceId, EntryHeader, NodeKey, SealOpener};
use zeroize::Zeroizing;

use crate::personal_identity;

/// The key and database that seal and open payloads.
pub(crate) struct Sealer {
    key: Zeroizing<[u8; 32]>,
    database: DatabaseId,
}

impl Sealer {
    /// A sealer for a database that already has its shared read key.
    pub(crate) fn for_read_key(database: DatabaseId, key: Zeroizing<[u8; 32]>) -> Self {
        Self { key, database }
    }
    /// The sealer of the personal database of the node with this seed.
    pub(crate) fn for_node_seed(node_seed: &[u8; 32]) -> Result<Self, String> {
        let id = personal_identity::database_id(node_seed);
        Ok(Self {
            key: personal_identity::read_key(node_seed, &id),
            database: DatabaseId::from_hex(&id).map_err(|_| "personal database id is not hex32")?,
        })
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
