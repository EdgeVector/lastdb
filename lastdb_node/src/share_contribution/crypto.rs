use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use cloud_backup_protocol::{Digest, Id};
use fold_db::canonical::CanonicalWriter;
use fold_db::security::Ed25519KeyPair;
use hkdf::Hkdf;
use rand::{rngs::OsRng, RngCore};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};
use zeroize::Zeroizing;

pub(super) const GRANT: &[u8] = b"lastdb:share-contributor-grant:v1";
pub(super) const MUTATION: &[u8] = b"lastdb:share-contributor-mutation:v1";

pub(crate) fn inbox_key(owner: &[u8; 32], publication: &Id) -> Result<StaticSecret, String> {
    let mut key = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(b"lastdb:share-inbox-x25519:v1"), owner)
        .expand(publication.as_str().as_bytes(), &mut *key)
        .map_err(|_| "cannot derive share inbox key")?;
    Ok(StaticSecret::from(*key))
}

pub(super) fn seal<T: Serialize>(
    value: &T,
    recipient: &PublicKey,
    signer: &Ed25519KeyPair,
    domain: &[u8],
) -> Result<Vec<u8>, String> {
    let mut body = Zeroizing::new(serde_json::to_vec(value).map_err(|_| "invalid share payload")?);
    let signature = signer.sign(&signed_bytes(domain, &body)).to_bytes();
    body.extend_from_slice(&signature);
    let ephemeral = EphemeralSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&ephemeral);
    let key = crate::seal::sealed_box_key(&ephemeral.diffie_hellman(recipient), domain)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| "invalid share encryption key")?;
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    cipher
        .encrypt_in_place(Nonce::from_slice(&nonce), domain, &mut *body)
        .map_err(|_| "cannot encrypt share payload")?;
    let mut wire = Vec::with_capacity(32 + 12 + body.len());
    wire.extend_from_slice(public.as_bytes());
    wire.extend_from_slice(&nonce);
    wire.extend_from_slice(&body);
    Ok(wire)
}

pub(super) fn open<T: DeserializeOwned>(
    bytes: &[u8],
    recipient: &StaticSecret,
    signer: &Digest,
    domain: &[u8],
) -> Result<T, String> {
    let (public, rest) = bytes
        .split_first_chunk::<32>()
        .ok_or("invalid share payload")?;
    let (nonce, ciphertext) = rest
        .split_first_chunk::<12>()
        .ok_or("invalid share payload")?;
    let key =
        crate::seal::sealed_box_key(&recipient.diffie_hellman(&PublicKey::from(*public)), domain)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| "invalid share encryption key")?;
    let mut plain = Zeroizing::new(ciphertext.to_vec());
    cipher
        .decrypt_in_place(Nonce::from_slice(nonce), domain, &mut *plain)
        .map_err(|_| "invalid encrypted share payload")?;
    let (body, signature) = plain
        .split_last_chunk::<64>()
        .ok_or("invalid share payload")?;
    let mut raw_key = [0; 32];
    hex::decode_to_slice(signer.as_str(), &mut raw_key).map_err(|_| "invalid share signer")?;
    let verifying = app_identity_crypto::VerifyingKey::from_bytes(&raw_key)
        .map_err(|_| "invalid share signer")?;
    app_identity_crypto::verify(&verifying, signature, &signed_bytes(domain, body))
        .map_err(|_| "invalid share signature")?;
    serde_json::from_slice(body).map_err(|_| "invalid signed share payload".into())
}

fn signed_bytes(domain: &[u8], body: &[u8]) -> Vec<u8> {
    CanonicalWriter::new()
        .field(domain)
        .field(&Sha256::digest(body))
        .finish()
}
