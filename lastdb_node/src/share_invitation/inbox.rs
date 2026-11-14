//! Private contribution payloads for the publisher's per-database inbox key.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use fold_db::canonical::CanonicalWriter;
use fold_db::security::Ed25519KeyPair;
use hkdf::Hkdf;
use rand::{rngs::OsRng, RngCore};
use sha2::Sha256;
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};
use zeroize::Zeroizing;

use super::{Result, ShareInvitationError};

const DOMAIN: &[u8] = b"lastdb:whole-database:contribution:v1";
const INVALID: ShareInvitationError = ShareInvitationError("invalid contribution envelope");

fn secret(publisher: &Ed25519KeyPair, database: &str) -> Result<StaticSecret> {
    let seed = Zeroizing::new(publisher.secret_key_bytes());
    let mut key = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(DOMAIN), &*seed)
        .expand(database.as_bytes(), &mut *key)
        .map_err(|_| INVALID)?;
    Ok(StaticSecret::from(*key))
}

pub(super) fn public_key(publisher: &Ed25519KeyPair, database: &str) -> Result<PublicKey> {
    Ok(PublicKey::from(&secret(publisher, database)?))
}

pub(crate) fn seal(recipient: &PublicKey, aad: &[u8], body: &[u8]) -> Result<Vec<u8>> {
    let ephemeral = EphemeralSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&ephemeral);
    let shared = ephemeral.diffie_hellman(recipient);
    let key = crate::seal::sealed_box_key(&shared, DOMAIN).map_err(|_| INVALID)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| INVALID)?;
    let mut nonce = [0; 12];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: body, aad })
        .map_err(|_| INVALID)?;
    let mut sealed = Vec::with_capacity(44 + ciphertext.len());
    sealed.extend_from_slice(public.as_bytes());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

pub(crate) fn open(
    publisher: &Ed25519KeyPair,
    database: &str,
    aad: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>> {
    let (public, rest) = sealed.split_first_chunk::<32>().ok_or(INVALID)?;
    let (nonce, ciphertext) = rest.split_first_chunk::<12>().ok_or(INVALID)?;
    let shared = secret(publisher, database)?.diffie_hellman(&PublicKey::from(*public));
    let key = crate::seal::sealed_box_key(&shared, DOMAIN).map_err(|_| INVALID)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| INVALID)?;
    cipher
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| INVALID)
}

pub(crate) fn context(
    database: &str,
    writer: &str,
    request: &str,
    namespace: &str,
    time: u64,
) -> Vec<u8> {
    CanonicalWriter::new()
        .field(DOMAIN)
        .field(database.as_bytes())
        .field(writer.as_bytes())
        .field(request.as_bytes())
        .field(namespace.as_bytes())
        .field(&time.to_be_bytes())
        .finish()
}
