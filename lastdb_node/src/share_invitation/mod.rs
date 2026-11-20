//! Recipient-encrypted invitations for ordinary Backup V2 share streams.
use crate::cloud_backup_client::GuestReadPermission;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use cloud_backup_protocol::Digest;
use fold_db::canonical::CanonicalWriter;
use fold_db::security::Ed25519KeyPair;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest as _, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use x25519_dalek::{EphemeralSecret, PublicKey};
use zeroize::Zeroizing;

mod contact;
pub(crate) use contact::inbox;
pub use contact::{ShareContact, ShareRecipient};

const INVITATION_V1: &[u8] = b"lastdb-share-invite-v1\0";
const INVITATION_V2: &[u8] = b"lastdb-share-invite-v2\0";
const INVITATION_V3: &[u8] = b"lastdb-share-invite-v3\0";
const INVITATION_SIGNATURE: &[u8] = b"lastdb:share-invitation-signature:v1";
const INVITATION_KEY: &[u8] = b"lastdb:share-invitation-aes:v1";
const SIGNATURE_BYTES: usize = 64;
const TAG_BYTES: usize = 16;

/// Limit sealed invitation input before allocation or decryption.
pub const MAX_SEALED_INVITATION_BYTES: usize = 4096;

#[derive(Debug)]
pub struct ShareInvitationError(&'static str);

impl std::fmt::Display for ShareInvitationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ShareInvitationError {}

pub type Result<T> = std::result::Result<T, ShareInvitationError>;

/// The independently encrypted publication key and its bound read permission.
pub(crate) struct OpenedShareInvitation {
    pub(crate) permission: GuestReadPermission,
    pub(crate) publication_key: Zeroizing<[u8; 32]>,
    pub(crate) spec: Option<crate::profile::PublicationSpec>,
}

/// Load the recipient decryption and contact authority from a LastDB identity home.
pub fn recipient(home: &std::path::Path) -> std::result::Result<ShareRecipient, String> {
    let seed = crate::offline_home::Directory::open(home)
        .map_err(|e| e.to_string())?
        .identity_seed()?
        .ok_or("share requires an initialized LastDB identity")?;
    ShareRecipient::from_identity_seed(&seed).map_err(|e| e.to_string())
}

pub(crate) fn seal(
    contact: &ShareContact,
    permission: &GuestReadPermission,
    publication_key: &[u8; 32],
    publisher: &Ed25519KeyPair,
    spec: Option<&crate::profile::PublicationSpec>,
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
    let aes_key = inbox::sealed_box_key(&shared, INVITATION_KEY).map_err(ShareInvitationError)?;
    let cipher = Aes256Gcm::new_from_slice(&*aes_key)
        .map_err(|_| ShareInvitationError("invalid share invitation key"))?;
    let authorization = permission.authorization().as_bytes();
    let mut body = Zeroizing::new(Vec::with_capacity(
        32 + 32 + 32 + authorization.len() + SIGNATURE_BYTES + TAG_BYTES,
    ));
    body.extend_from_slice(&contact.identity);
    body.extend_from_slice(contact.encryption.as_bytes());
    body.extend_from_slice(publication_key);
    let version = if let Some(spec) = spec {
        if spec.whole_database() {
            body.extend_from_slice(
                inbox::public_key(publisher, permission.stream().stream_id.as_str())?.as_bytes(),
            );
            encode_spec(&mut body, spec, authorization)?;
            INVITATION_V3
        } else {
            encode_spec(&mut body, spec, authorization)?;
            INVITATION_V2
        }
    } else {
        body.extend_from_slice(authorization);
        INVITATION_V1
    };
    let signature = publisher.sign(&signing_bytes(version, &body)).to_bytes();
    body.extend_from_slice(&signature);

    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let aad = invitation_context(version, contact, &ephemeral_public);
    cipher
        .encrypt_in_place(Nonce::from_slice(&nonce), &aad, &mut *body)
        .map_err(|_| ShareInvitationError("cannot seal share invitation"))?;

    let mut sealed = Vec::with_capacity(version.len() + 32 + nonce.len() + body.len());
    sealed.extend_from_slice(version);
    sealed.extend_from_slice(ephemeral_public.as_bytes());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&body);
    Ok(sealed)
}

impl ShareRecipient {
    /// Open the invitation for this identity and an independently pinned publisher.
    pub(crate) fn open(
        &self,
        sealed: &[u8],
        expected_publisher: &Digest,
    ) -> Result<OpenedShareInvitation> {
        self.open_inner(sealed, expected_publisher, true)
    }

    /// Same verification as [`Self::open`]. A stored signed log stays readable
    /// after the invitation bearer expires.
    pub(crate) fn open_stored(
        &self,
        sealed: &[u8],
        expected_publisher: &Digest,
    ) -> Result<OpenedShareInvitation> {
        self.open_inner(sealed, expected_publisher, false)
    }

    fn open_inner(
        &self,
        sealed: &[u8],
        expected_publisher: &Digest,
        check_expiry: bool,
    ) -> Result<OpenedShareInvitation> {
        let invalid = || ShareInvitationError("invalid sealed share invitation");
        let version = [INVITATION_V1, INVITATION_V2, INVITATION_V3]
            .into_iter()
            .find(|prefix| sealed.starts_with(prefix))
            .filter(|_| sealed.len() <= MAX_SEALED_INVITATION_BYTES)
            .ok_or_else(invalid)?;
        let wire = sealed.get(version.len()..).ok_or_else(invalid)?;
        let (ephemeral, rest) = wire.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (nonce, ciphertext) = rest.split_first_chunk::<12>().ok_or_else(invalid)?;
        let ephemeral = PublicKey::from(*ephemeral);
        let shared = self.secret.diffie_hellman(&ephemeral);
        let aes_key =
            inbox::sealed_box_key(&shared, INVITATION_KEY).map_err(ShareInvitationError)?;
        let cipher = Aes256Gcm::new_from_slice(&*aes_key)
            .map_err(|_| ShareInvitationError("invalid share invitation key"))?;
        let mut plaintext = Zeroizing::new(ciphertext.to_vec());
        let aad = invitation_context(version, &self.contact, &ephemeral);
        cipher
            .decrypt_in_place(Nonce::from_slice(nonce), &aad, &mut *plaintext)
            .map_err(|_| invalid())?;
        self.verify_plaintext(version, &plaintext, expected_publisher, check_expiry)
    }

    fn verify_plaintext(
        &self,
        version: &[u8],
        plaintext: &[u8],
        expected_publisher: &Digest,
        check_expiry: bool,
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
        let (inbox_key, authorization) = if version == INVITATION_V3 {
            let (key, rest) = authorization
                .split_first_chunk::<32>()
                .ok_or_else(invalid)?;
            (Some(PublicKey::from(*key)), rest)
        } else {
            (None, authorization)
        };
        let (spec, authorization) = if version == INVITATION_V1 {
            (None, authorization)
        } else {
            let (spec, authorization) = decode_spec(authorization)?;
            if spec.whole_database() != inbox_key.is_some() {
                return Err(invalid());
            }
            (Some(spec), authorization)
        };
        let authorization = std::str::from_utf8(authorization).map_err(|_| invalid())?;
        let permission = GuestReadPermission::parse(authorization)
            .map_err(|_| ShareInvitationError("invalid share invitation permission"))?;
        if permission.publisher_public_key() != expected_publisher {
            return Err(ShareInvitationError("share invitation publisher differs"));
        }
        let publisher: [u8; 32] = fold_db::hex::hex_decode_array(expected_publisher.as_str())
            .ok_or(ShareInvitationError("invalid share invitation publisher"))?;
        verify_signature(&publisher, signature, &signing_bytes(version, body))?;
        if check_expiry {
            require_unexpired(&permission)?;
        }
        Ok(OpenedShareInvitation {
            permission,
            publication_key: Zeroizing::new(*publication_key),
            spec,
        })
    }
}

fn signing_bytes(version: &[u8], body: &[u8]) -> Vec<u8> {
    let domain = if version == INVITATION_V3 {
        &b"lastdb:share-invitation-signature:v3"[..]
    } else if version == INVITATION_V2 {
        &b"lastdb:share-invitation-signature:v2"[..]
    } else {
        INVITATION_SIGNATURE
    };
    CanonicalWriter::new()
        .field(domain)
        .field(&Sha256::digest(body))
        .finish()
}

fn invitation_context(version: &[u8], contact: &ShareContact, ephemeral: &PublicKey) -> Vec<u8> {
    CanonicalWriter::new()
        .field(version)
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

const INVALID_SPEC: ShareInvitationError = ShareInvitationError("invalid share invitation spec");

fn encode_spec(
    body: &mut Vec<u8>,
    spec: &crate::profile::PublicationSpec,
    permission: &[u8],
) -> Result<()> {
    database_policy::SpecBinding::new(&spec.name, spec.version).map_err(|_| INVALID_SPEC)?;
    let length = u8::try_from(spec.name.len()).map_err(|_| INVALID_SPEC)?;
    body.push(length);
    body.extend_from_slice(spec.name.as_bytes());
    body.extend_from_slice(&spec.version.to_be_bytes());
    body.extend_from_slice(permission);
    Ok(())
}

fn decode_spec(payload: &[u8]) -> Result<(crate::profile::PublicationSpec, &[u8])> {
    let (&length, payload) = payload.split_first().ok_or(INVALID_SPEC)?;
    let (name, payload) = payload
        .split_at_checked(usize::from(length))
        .ok_or(INVALID_SPEC)?;
    let (version, permission) = payload.split_first_chunk::<4>().ok_or(INVALID_SPEC)?;
    let name = std::str::from_utf8(name).map_err(|_| INVALID_SPEC)?;
    let version = u32::from_be_bytes(*version);
    database_policy::SpecBinding::new(name, version).map_err(|_| INVALID_SPEC)?;
    Ok((
        crate::profile::PublicationSpec {
            name: name.to_owned(),
            version,
        },
        permission,
    ))
}
