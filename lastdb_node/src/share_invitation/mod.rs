//! Recipient-encrypted invitations for ordinary Backup V2 share streams.
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use cloud_backup_client::byte_backup::GuestReadPermission;
use cloud_backup_protocol::Digest;
use fold_db::canonical::CanonicalWriter;
use fold_db::security::Ed25519KeyPair;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest as _, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use x25519_dalek::{EphemeralSecret, PublicKey};
use zeroize::Zeroizing;

mod contact;
pub use contact::{ShareContact, ShareRecipient};

const INVITATION_V1: &[u8] = b"lastdb-share-invite-v1\0";
const INVITATION_SIGNATURE: &[u8] = b"lastdb:share-invitation-signature:v1";
const INVITATION_KEY: &[u8] = b"lastdb:share-invitation-aes:v1";
const SIGNATURE_BYTES: usize = 64;
const TAG_BYTES: usize = 16;

/// Limit sealed invitation input before allocation or decryption.
pub const MAX_SEALED_INVITATION_BYTES: usize = 4096;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ShareInvitationError(&'static str);

pub type Result<T> = std::result::Result<T, ShareInvitationError>;

/// The independently encrypted publication key and its bound read permission.
pub struct OpenedShareInvitation {
    pub permission: GuestReadPermission,
    pub publication_key: Zeroizing<[u8; 32]>,
}

/// Seal a signed stream permission and publication key to a verified contact.
///
/// The caller checks the contact identity before selecting it for this share.
pub fn seal_invitation(
    contact: &ShareContact,
    permission: &GuestReadPermission,
    publication_key: &[u8; 32],
    publisher: &Ed25519KeyPair,
) -> Result<Vec<u8>> {
    if fold_db::hex::hex_lower(publisher.public_key_bytes())
        != permission.publisher_public_key().as_str()
    {
        return Err(ShareInvitationError("share permission publisher differs"));
    }
    require_unexpired(permission)?;
    let ephemeral = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = PublicKey::from(&ephemeral);
    let shared = ephemeral.diffie_hellman(&contact.encryption);
    let aes_key =
        crate::seal::sealed_box_key(&shared, INVITATION_KEY).map_err(ShareInvitationError)?;
    let cipher = Aes256Gcm::new_from_slice(&*aes_key)
        .map_err(|_| ShareInvitationError("invalid share invitation key"))?;
    let authorization = permission.authorization().as_bytes();
    let mut body = Zeroizing::new(Vec::with_capacity(
        32 + 32 + 32 + authorization.len() + SIGNATURE_BYTES + TAG_BYTES,
    ));
    body.extend_from_slice(&contact.identity);
    body.extend_from_slice(contact.encryption.as_bytes());
    body.extend_from_slice(publication_key);
    body.extend_from_slice(authorization);
    let signature = publisher.sign(&signing_bytes(&body)).to_bytes();
    body.extend_from_slice(&signature);

    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let aad = invitation_context(contact, &ephemeral_public);
    cipher
        .encrypt_in_place(Nonce::from_slice(&nonce), &aad, &mut *body)
        .map_err(|_| ShareInvitationError("cannot seal share invitation"))?;

    let mut sealed = Vec::with_capacity(INVITATION_V1.len() + 32 + nonce.len() + body.len());
    sealed.extend_from_slice(INVITATION_V1);
    sealed.extend_from_slice(ephemeral_public.as_bytes());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&body);
    Ok(sealed)
}

impl ShareRecipient {
    /// Open the invitation for this identity and an independently pinned publisher.
    pub fn open(
        &self,
        sealed: &[u8],
        expected_publisher: &Digest,
    ) -> Result<OpenedShareInvitation> {
        let invalid = || ShareInvitationError("invalid sealed share invitation");
        let wire = sealed
            .strip_prefix(INVITATION_V1)
            .filter(|_| sealed.len() <= MAX_SEALED_INVITATION_BYTES)
            .ok_or_else(invalid)?;
        let (ephemeral, rest) = wire.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (nonce, ciphertext) = rest.split_first_chunk::<12>().ok_or_else(invalid)?;
        let ephemeral = PublicKey::from(*ephemeral);
        let shared = self.secret.diffie_hellman(&ephemeral);
        let aes_key =
            crate::seal::sealed_box_key(&shared, INVITATION_KEY).map_err(ShareInvitationError)?;
        let cipher = Aes256Gcm::new_from_slice(&*aes_key)
            .map_err(|_| ShareInvitationError("invalid share invitation key"))?;
        let mut plaintext = Zeroizing::new(ciphertext.to_vec());
        let aad = invitation_context(&self.contact, &ephemeral);
        cipher
            .decrypt_in_place(Nonce::from_slice(nonce), &aad, &mut *plaintext)
            .map_err(|_| invalid())?;
        self.verify_plaintext(&plaintext, expected_publisher)
    }

    fn verify_plaintext(
        &self,
        plaintext: &[u8],
        expected_publisher: &Digest,
    ) -> Result<OpenedShareInvitation> {
        let invalid = || ShareInvitationError("invalid share invitation body");
        let (body, signature) = plaintext
            .split_last_chunk::<SIGNATURE_BYTES>()
            .ok_or_else(invalid)?;
        let (identity, rest) = body.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (encryption, rest) = rest.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (publication_key, authorization) =
            rest.split_first_chunk::<32>().ok_or_else(invalid)?;
        if identity != &self.contact.identity || encryption != self.contact.encryption.as_bytes() {
            return Err(ShareInvitationError("share invitation recipient differs"));
        }
        let authorization = std::str::from_utf8(authorization).map_err(|_| invalid())?;
        let permission = GuestReadPermission::parse(authorization)
            .map_err(|_| ShareInvitationError("invalid share invitation permission"))?;
        if permission.publisher_public_key() != expected_publisher {
            return Err(ShareInvitationError("share invitation publisher differs"));
        }
        let publisher: [u8; 32] = hex::decode(expected_publisher.as_str())
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(ShareInvitationError("invalid share invitation publisher"))?;
        verify_signature(&publisher, signature, &signing_bytes(body))?;
        require_unexpired(&permission)?;
        Ok(OpenedShareInvitation {
            permission,
            publication_key: Zeroizing::new(*publication_key),
        })
    }
}

fn signing_bytes(body: &[u8]) -> Vec<u8> {
    CanonicalWriter::new()
        .field(INVITATION_SIGNATURE)
        .field(&Sha256::digest(body))
        .finish()
}

fn invitation_context(contact: &ShareContact, ephemeral: &PublicKey) -> Vec<u8> {
    CanonicalWriter::new()
        .field(INVITATION_V1)
        .field(contact.encryption.as_bytes())
        .field(ephemeral.as_bytes())
        .finish()
}

fn require_unexpired(permission: &GuestReadPermission) -> Result<()> {
    let clock = || ShareInvitationError("invalid clock for share invitation");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| clock())?;
    let now = u64::try_from(now.as_millis()).map_err(|_| clock())?;
    if now >= permission.expires_ms() {
        return Err(ShareInvitationError("share invitation expired"));
    }
    Ok(())
}

fn verify_signature(key: &[u8; 32], signature: &[u8; 64], message: &[u8]) -> Result<()> {
    let invalid = || ShareInvitationError("invalid share invitation signature");
    let key = app_identity_crypto::VerifyingKey::from_bytes(key).map_err(|_| invalid())?;
    app_identity_crypto::verify(&key, signature, message).map_err(|_| invalid())
}
