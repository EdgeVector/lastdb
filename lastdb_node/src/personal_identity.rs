//! Keys of the personal database, derived from the node seed.
//!
//! The node seed is the 32 bytes behind the recovery phrase. Every device of
//! one owner has the same seed, so everything here is the same on every device
//! and is known from the phrase alone:
//!
//! - the personal database key, whose verifying key is the database id;
//! - the salt of the database birth, so every device signs the same birth;
//! - the device id of the birth stream, a stream that holds only the birth;
//! - the read key that seals the payloads of one database.
//!
//! The device id of a running node is not here. It is random, made once per
//! installation, and names the stream that installation signs.

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

/// HKDF salt that separates these keys from every other use of the node seed.
const PERSONAL_SALT: &[u8] = b"lastdb:personal-database:v1";
const DATABASE_KEY: &[u8] = b"database-key";
const BIRTH_SALT: &[u8] = b"birth-salt";
const BIRTH_DEVICE: &[u8] = b"birth-device";
/// HKDF salt of the read keys. The database id is the HKDF info.
const READ_KEY_SALT: &[u8] = b"lastdb:database-read-key:v1";

fn derive(salt: &[u8], seed: &[u8; 32], info: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(salt), seed)
        .expand(info, out.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    out
}

/// The seed of the personal database key. Its verifying key is the database id.
pub(crate) fn database_key_seed(node_seed: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    derive(PERSONAL_SALT, node_seed, DATABASE_KEY)
}

/// The 64-hex id of the personal database.
pub(crate) fn database_id(node_seed: &[u8; 32]) -> String {
    let key = app_identity_crypto::SigningKey::from_bytes(&database_key_seed(node_seed));
    app_identity_crypto::hex_lower(&key.verifying_key().to_bytes())
}

/// The salt of the personal database birth.
pub(crate) fn birth_salt(node_seed: &[u8; 32]) -> [u8; 32] {
    *derive(PERSONAL_SALT, node_seed, BIRTH_SALT)
}

/// The device id of the stream that holds the personal database birth.
pub(crate) fn birth_device(node_seed: &[u8; 32]) -> [u8; 32] {
    *derive(PERSONAL_SALT, node_seed, BIRTH_DEVICE)
}

/// The key that seals the payloads of one database of this owner.
pub(crate) fn read_key(node_seed: &[u8; 32], database_id: &str) -> Zeroizing<[u8; 32]> {
    derive(READ_KEY_SALT, node_seed, database_id.as_bytes())
}
