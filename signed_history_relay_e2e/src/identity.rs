//! Writer identities, database birth material, clocks and random ids.

use app_identity_crypto::{hex_lower, sign, SigningKey};
use mutation_log::{
    birth_proof_bytes, encode_signature_b64url, DatabaseId, DeviceId, Hex32, NodeKey,
};
use rand::RngCore;

/// One Ed25519 node identity.
pub struct Actor {
    pub signing_key: SigningKey,
    pub node_key: NodeKey,
}

impl Actor {
    pub fn generate() -> Self {
        let signing_key = SigningKey::from_bytes(&random_32());
        let node_key = NodeKey::from_bytes(signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            node_key,
        }
    }

    pub fn node_key_hex(&self) -> String {
        self.node_key.to_hex()
    }
}

/// A device id is 32 random bytes shown as 64 lowercase hex characters.
pub fn new_device_id() -> DeviceId {
    DeviceId::from_bytes(random_32())
}

/// Self-certifying database id plus the birth proof and the bundle secret.
pub struct DbBirth {
    pub database_id: DatabaseId,
    pub db_salt: Hex32,
    pub birth_signature: String,
    /// Seed of the database key; bundle blobs are encrypted under keys derived from it.
    pub secret: [u8; 32],
}

impl DbBirth {
    pub fn generate(owner: NodeKey) -> Self {
        let secret = random_32();
        let db_key = SigningKey::from_bytes(&secret);
        let database_id = DatabaseId::from_bytes(db_key.verifying_key().to_bytes());
        let db_salt = Hex32::from_bytes(random_32());
        let proof = birth_proof_bytes(owner, db_salt);
        let birth_signature = encode_signature_b64url(&sign(&db_key, &proof));
        Self {
            database_id,
            db_salt,
            birth_signature,
            secret,
        }
    }
}

pub fn random_32() -> [u8; 32] {
    let mut out = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut out);
    out
}

pub fn nonce_hex() -> String {
    hex_lower(&random_32())
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
