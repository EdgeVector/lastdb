//! Seal a member's proposal so that only the owner of the shared database can open it.
//!
//! A proposal is moderated: no other member may read it before the owner decides.
//! The shared read key cannot give that, because every member holds it. So a member
//! seals a proposal to the owner's node key instead.
//!
//! The owner's Ed25519 node key, in its Montgomery form, is an X25519 public key
//! (the RFC 7748 birational map). Every member already knows that key from the
//! birth entry, so nothing new is published. For each proposal the member makes a
//! fresh X25519 key, and HKDF-SHA256 turns the shared secret into an AES-256-GCM
//! key bound to the database id, the owner key and the fresh public key. The
//! ciphertext is LastDB's standard envelope, with the same position-bound
//! additional data as every other sealed payload
//! ([`crate::personal_seal::position_aad`]). Every device of the owner holds the
//! node key, so every device of the owner can open it, and no member can.
//!
//! Wire form of the payload: `fresh_public(32) ‖ envelope`. The namespace on the
//! wire is [`SEALED_NAMESPACE`], and the real namespace travels inside, packed by
//! [`pack_hiding_namespace`].

use app_identity_crypto::{SigningKey, VerifyingKey};
use fold_db::crypto::envelope::{decrypt_envelope_with_aad, encrypt_envelope_with_aad};
use hkdf::Hkdf;
use mutation_log::{
    decode_record_payload, pack_hiding_namespace, unpack_hiding_namespace, DataBody, DatabaseId,
    DeviceId, NodeKey, RecordPayload, SignedEntry, SEALED_NAMESPACE,
};
use rand::RngCore;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use crate::personal_seal::position_aad;

const KEY_DOMAIN: &[u8] = b"lastdb:proposal-owner-seal:v1";
const PUBLIC_BYTES: usize = 32;

/// Where a proposal is signed: the database, the owner it is sealed to, and the
/// position of the entry on the member's stream.
pub(crate) struct ProposalSlot {
    pub(crate) database: DatabaseId,
    pub(crate) owner: NodeKey,
    pub(crate) node_key: NodeKey,
    pub(crate) device_id: DeviceId,
    pub(crate) writer_seq: u64,
}

/// Seal a proposal body so that only the owner can open it.
pub(crate) fn seal_proposal(slot: &ProposalSlot, body: DataBody) -> Result<DataBody, String> {
    let owner = owner_public(&slot.owner)?;
    let mut seed = Zeroizing::new([0u8; 32]);
    rand::rngs::OsRng.fill_bytes(&mut *seed);
    let fresh = StaticSecret::from(*seed);
    let fresh_public = PublicKey::from(&fresh);
    let shared = fresh.diffie_hellman(&owner);
    if !shared.was_contributory() {
        return Err("the owner key is not a usable encryption key".to_owned());
    }
    let key = derive(
        &slot.database,
        &slot.owner,
        &fresh_public,
        shared.as_bytes(),
    )?;
    let plain = Zeroizing::new(
        pack_hiding_namespace(&body.namespace, &body.payload)
            .map_err(std::borrow::ToOwned::to_owned)?,
    );
    let aad = position_aad(
        &slot.database,
        &slot.node_key,
        &slot.device_id,
        slot.writer_seq,
    );
    let envelope = encrypt_envelope_with_aad(&key, &aad, &plain).map_err(|e| e.to_string())?;
    let mut payload = Vec::with_capacity(PUBLIC_BYTES + envelope.len());
    payload.extend_from_slice(fresh_public.as_bytes());
    payload.extend_from_slice(&envelope);
    Ok(DataBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        payload,
        ..body
    })
}

/// Opens proposals on a device of the owner.
pub(crate) struct OwnerOpener {
    secret: StaticSecret,
    owner: NodeKey,
    database: DatabaseId,
}

impl OwnerOpener {
    /// The opener of `database_id` for the owner whose node key is `signing_key`.
    pub(crate) fn new(signing_key: &SigningKey, database_id: &str) -> Result<Self, String> {
        Ok(Self {
            // The unreduced scalar of the Ed25519 key; X25519 clamps it. Its public
            // point is the Montgomery form of the node key.
            secret: StaticSecret::from(signing_key.to_scalar_bytes()),
            owner: NodeKey::from_bytes(signing_key.verifying_key().to_bytes()),
            database: DatabaseId::from_hex(database_id)
                .map_err(|_| "database id is not hex32".to_owned())?,
        })
    }

    /// The namespace and record payload of a proposal, or `None` when it was not
    /// sealed to this owner at its position. A proposal sealed with the shared read
    /// key does not open here, so the owner declines it.
    pub(crate) fn decode(&self, entry: &SignedEntry) -> Option<(String, RecordPayload)> {
        let SignedEntry::Data { header, body } = entry else {
            return None;
        };
        if body.namespace != SEALED_NAMESPACE {
            return None;
        }
        let (fresh, envelope) = body.payload.split_first_chunk::<PUBLIC_BYTES>()?;
        let fresh = PublicKey::from(*fresh);
        let shared = self.secret.diffie_hellman(&fresh);
        if !shared.was_contributory() {
            return None;
        }
        let key = derive(&self.database, &self.owner, &fresh, shared.as_bytes()).ok()?;
        let aad = position_aad(
            &self.database,
            &header.node_key,
            &header.device_id,
            header.writer_seq,
        );
        let plain = Zeroizing::new(decrypt_envelope_with_aad(&key, &aad, envelope).ok()?);
        let (namespace, payload) = unpack_hiding_namespace(&plain)?;
        let decoded = decode_record_payload(&payload).ok()?;
        Some((namespace, decoded))
    }
}

fn owner_public(owner: &NodeKey) -> Result<PublicKey, String> {
    let key = VerifyingKey::from_bytes(owner.as_bytes())
        .map_err(|_| "the owner key is not an Ed25519 key".to_owned())?;
    Ok(PublicKey::from(key.to_montgomery().to_bytes()))
}

fn derive(
    database: &DatabaseId,
    owner: &NodeKey,
    fresh: &PublicKey,
    shared: &[u8; 32],
) -> Result<Zeroizing<[u8; 32]>, String> {
    let mut info = Vec::with_capacity(32 * 3);
    info.extend_from_slice(database.as_bytes());
    info.extend_from_slice(owner.as_bytes());
    info.extend_from_slice(fresh.as_bytes());
    let mut key = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(KEY_DOMAIN), shared)
        .expand(&info, &mut *key)
        .map_err(|_| "cannot derive the proposal key".to_owned())?;
    Ok(key)
}
