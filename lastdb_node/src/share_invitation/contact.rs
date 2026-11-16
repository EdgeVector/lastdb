use super::{verify_signature, Result, ShareInvitationError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::Digest;
use fold_db::canonical::CanonicalWriter;
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

const CONTACT_PREFIX: &str = "lastdb-share-contact-v1.";
const CONTACT_DOMAIN: &[u8] = b"lastdb:share-contact:v1";
const RECIPIENT_KEY_INFO: &[u8] = b"lastdb:share-recipient-x25519:v1";
const CACHE_KEY_DOMAIN: &[u8] = b"lastdb:share-recipient-cache:v1";
const CONTACT_BODY_BYTES: usize = 32 + 32 + 64;
const CONTACT_ENCODED_BYTES: usize = (CONTACT_BODY_BYTES * 4).div_ceil(3);

/// A public recipient contact with a verified identity-to-encryption-key binding.
#[derive(Clone)]
pub struct ShareContact {
    identity_public_key: Digest,
    pub(super) identity: [u8; 32],
    pub(super) encryption: PublicKey,
    signature: [u8; 64],
}

/// The recipient's local decryption authority. Only the contact is exported.
pub struct ShareRecipient {
    pub(super) secret: StaticSecret,
    pub(super) contact: ShareContact,
}

impl ShareRecipient {
    /// Derive a separate X25519 key from the recipient's local identity seed.
    pub fn from_identity_seed(seed: &[u8; 32]) -> Result<Self> {
        let identity = app_identity_crypto::SigningKey::from_bytes(seed);
        let mut key = Zeroizing::new([0u8; 32]);
        Hkdf::<Sha256>::new(None, seed)
            .expand(RECIPIENT_KEY_INFO, &mut *key)
            .map_err(|_| ShareInvitationError("cannot derive local share recipient key"))?;
        let secret = StaticSecret::from(*key);
        let encryption = PublicKey::from(&secret);
        let identity_key = identity.verifying_key().to_bytes();
        let signature =
            app_identity_crypto::sign(&identity, &binding_bytes(&identity_key, &encryption));
        let contact = ShareContact::verified(identity_key, encryption, signature)?;
        Ok(Self { secret, contact })
    }

    pub fn contact(&self) -> ShareContact {
        self.contact.clone()
    }

    pub(crate) fn cache_key(
        &self,
        handle: &cloud_backup_protocol::Id,
    ) -> Result<Zeroizing<[u8; 32]>> {
        let mut key = Zeroizing::new([0u8; 32]);
        Hkdf::<Sha256>::new(Some(CACHE_KEY_DOMAIN), self.secret.as_bytes())
            .expand(handle.as_str().as_bytes(), &mut *key)
            .map_err(|_| ShareInvitationError("cannot derive local share cache key"))?;
        Ok(key)
    }
}

impl ShareContact {
    /// Verify the contact format and identity proof.
    pub fn parse(value: &str) -> Result<Self> {
        let invalid = || ShareInvitationError("invalid LastDB share contact");
        let encoded = value
            .strip_prefix(CONTACT_PREFIX)
            .filter(|encoded| encoded.len() == CONTACT_ENCODED_BYTES)
            .ok_or_else(invalid)?;
        let decoded = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
        let (identity, rest) = decoded.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (encryption, signature) = rest.split_first_chunk::<32>().ok_or_else(invalid)?;
        let signature = signature.try_into().map_err(|_| invalid())?;
        Self::verified(*identity, PublicKey::from(*encryption), signature)
    }

    pub fn encode(&self) -> String {
        let mut body = Vec::with_capacity(CONTACT_BODY_BYTES);
        body.extend_from_slice(&self.identity);
        body.extend_from_slice(self.encryption.as_bytes());
        body.extend_from_slice(&self.signature);
        let encoded = URL_SAFE_NO_PAD.encode(body);
        format!("{CONTACT_PREFIX}{encoded}")
    }

    /// The verified raw Ed25519 identity encoded as lowercase hex.
    pub fn identity_public_key(&self) -> &Digest {
        &self.identity_public_key
    }

    fn verified(identity: [u8; 32], encryption: PublicKey, signature: [u8; 64]) -> Result<Self> {
        if !StaticSecret::from([0u8; 32])
            .diffie_hellman(&encryption)
            .was_contributory()
        {
            return Err(ShareInvitationError("non-contributory share contact key"));
        }
        verify_signature(
            &identity,
            &signature,
            &binding_bytes(&identity, &encryption),
        )
        .map_err(|_| ShareInvitationError("invalid share contact proof"))?;
        let identity_public_key = Digest::try_from(fold_db::hex::hex_lower(identity))
            .map_err(|_| ShareInvitationError("invalid share contact identity"))?;
        Ok(Self {
            identity_public_key,
            identity,
            encryption,
            signature,
        })
    }
}

fn binding_bytes(identity: &[u8; 32], encryption: &PublicKey) -> Vec<u8> {
    CanonicalWriter::new()
        .field(CONTACT_DOMAIN)
        .field(identity)
        .field(encryption.as_bytes())
        .finish()
}

pub(crate) mod inbox {
    use aes_gcm::aead::{Aead, AeadCore, KeyInit, Payload};
    use aes_gcm::{Aes256Gcm, Nonce};
    use fold_db::canonical::CanonicalWriter;
    use fold_db::security::Ed25519KeyPair;
    use hkdf::Hkdf;
    use rand::rngs::OsRng;
    use sha2::Sha256;
    use x25519_dalek::{EphemeralSecret, PublicKey, SharedSecret, StaticSecret};
    use zeroize::Zeroizing;

    use crate::share_invitation::{Result, ShareInvitationError};

    const DOMAIN: &[u8] = b"lastdb:whole-database:contribution:v1";
    const INVALID: ShareInvitationError = ShareInvitationError("invalid contribution envelope");

    pub(in crate::share_invitation) fn sealed_box_key(
        shared: &SharedSecret,
        context: &[u8],
    ) -> std::result::Result<Zeroizing<[u8; 32]>, &'static str> {
        if !shared.was_contributory() {
            return Err("non-contributory X25519 key");
        }
        let hk = Hkdf::<Sha256>::new(None, shared.as_bytes());
        let mut key = Zeroizing::new([0u8; 32]);
        hk.expand(context, &mut *key)
            .map_err(|_| "sealed box key derivation failed")?;
        Ok(key)
    }

    fn secret(publisher: &Ed25519KeyPair, database: &str) -> Result<StaticSecret> {
        let seed = Zeroizing::new(publisher.secret_key_bytes());
        let mut key = Zeroizing::new([0u8; 32]);
        Hkdf::<Sha256>::new(Some(DOMAIN), &*seed)
            .expand(database.as_bytes(), &mut *key)
            .map_err(|_| INVALID)?;
        Ok(StaticSecret::from(*key))
    }

    pub(in crate::share_invitation) fn public_key(
        publisher: &Ed25519KeyPair,
        database: &str,
    ) -> Result<PublicKey> {
        Ok(PublicKey::from(&secret(publisher, database)?))
    }

    pub(crate) fn seal(recipient: &PublicKey, aad: &[u8], body: &[u8]) -> Result<Vec<u8>> {
        let ephemeral = EphemeralSecret::random_from_rng(OsRng);
        let public = PublicKey::from(&ephemeral);
        let shared = ephemeral.diffie_hellman(recipient);
        let key = sealed_box_key(&shared, DOMAIN).map_err(|_| INVALID)?;
        let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| INVALID)?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, Payload { msg: body, aad })
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
        let key = sealed_box_key(&shared, DOMAIN).map_err(|_| INVALID)?;
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
}
