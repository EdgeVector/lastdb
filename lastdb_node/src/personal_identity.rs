//! Keys of the personal database, derived from the node seed.
//!
//! The node seed is the 32 bytes behind the recovery phrase. Every device of
//! one owner has the same seed, so everything here is the same on every device
//! and is known from the phrase alone:
//!
//! - the personal database key, whose verifying key is the database id;
//! - the salt of the database birth, so every device signs the same birth;
//! - the device id of the birth stream, a stream that holds only the birth;
//! - the device id of the snapshot stream, a stream that holds only `snapshot` entries;
//! - the read key that seals the payloads of one database.
//!
//! The device id of a running node is not here. It is random, made once per
//! installation, and names the stream that installation signs.

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

/// The seed is uniformly random, so HKDF takes no salt. Each key has its own
/// `info`, which keeps every derived key apart.
const DATABASE_KEY_INFO: &[u8] = b"lastdb:personal-database:v1:database-key";
const BIRTH_SALT_INFO: &[u8] = b"lastdb:personal-database:v1:birth-salt";
const BIRTH_DEVICE_INFO: &[u8] = b"lastdb:personal-database:v1:birth-device";
const SNAPSHOT_DEVICE_INFO: &[u8] = b"lastdb:personal-database:v1:snapshot-device";
/// The `info` of a read key is this prefix and the 64 hex characters of the database id.
const READ_KEY_INFO_PREFIX: &[u8] = b"lastdb:database-read-key:v1:";

fn derive(seed: &[u8; 32], info: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(None, seed)
        .expand(info, out.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    out
}

/// The seed of the personal database key. Its verifying key is the database id.
pub(crate) fn database_key_seed(node_seed: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    derive(node_seed, DATABASE_KEY_INFO)
}

/// The 64-hex id of the personal database.
pub(crate) fn database_id(node_seed: &[u8; 32]) -> String {
    let key = app_identity_crypto::SigningKey::from_bytes(&database_key_seed(node_seed));
    app_identity_crypto::hex_lower(&key.verifying_key().to_bytes())
}

/// The salt of the personal database birth.
pub(crate) fn birth_salt(node_seed: &[u8; 32]) -> [u8; 32] {
    *derive(node_seed, BIRTH_SALT_INFO)
}

/// The device id of the stream that holds the personal database birth.
pub(crate) fn birth_device(node_seed: &[u8; 32]) -> [u8; 32] {
    *derive(node_seed, BIRTH_DEVICE_INFO)
}

/// The device id of the stream that holds the `snapshot` entries of the personal
/// database. Every device of the owner signs its snapshots on this one stream.
pub(crate) fn snapshot_device(node_seed: &[u8; 32]) -> [u8; 32] {
    *derive(node_seed, SNAPSHOT_DEVICE_INFO)
}

/// The key that seals the payloads of one database of this owner.
pub(crate) fn read_key(node_seed: &[u8; 32], database_id: &str) -> Zeroizing<[u8; 32]> {
    derive(
        node_seed,
        &[READ_KEY_INFO_PREFIX, database_id.as_bytes()].concat(),
    )
}
