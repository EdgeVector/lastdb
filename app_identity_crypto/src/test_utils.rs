//! Test-only minting helpers, gated behind the `test-utils` feature.
//!
//! These produce the exact signed artifacts a real exemem root + a real
//! developer would: ES256-signed [`DevCert`]s and Ed25519 signature
//! envelopes. Co-located with the verifier so the signing side cannot
//! drift from the verification side — the canonicalization here is the
//! same [`canonicalize`] the verifier uses. Downstream test suites
//! (schema_service, fold_db_node) enable `app_identity_crypto/test-utils`
//! as a dev-dependency feature.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey as EdSigningKey;
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature as P256Signature, SigningKey as P256SigningKey};
use p256::pkcs8::EncodePublicKey;
use rand_core::OsRng;
use serde_json::Value;

use crate::{
    canonicalize, compute_payload_hash, key_id, root_key_id, sign_envelope, DevCert, Env, Purpose,
    SignatureEnvelope, ALG_ED25519, ALG_ES256, DEV_CERT_VERSION, ENVELOPE_VERSION,
    PURPOSE_DEV_CERT,
};

/// A throwaway exemem root signer plus its SubjectPublicKeyInfo DER (the
/// value that goes into schema_service's trusted-root config).
pub struct TestRoot {
    signing_key: P256SigningKey,
    /// SPKI DER of the root public key — base64 this for
    /// `APP_IDENTITY_ROOT_PUBKEYS`.
    pub spki_der: Vec<u8>,
}

impl TestRoot {
    /// Generate a fresh P-256 root.
    pub fn generate() -> Self {
        let signing_key = P256SigningKey::random(&mut OsRng);
        let spki_der = signing_key
            .verifying_key()
            .to_public_key_der()
            .expect("encode P-256 SPKI")
            .as_bytes()
            .to_vec();
        Self {
            signing_key,
            spki_der,
        }
    }

    /// The `key_id` (hex sha256 of SPKI DER) this root's certs carry.
    pub fn key_id(&self) -> String {
        root_key_id(&self.spki_der)
    }

    /// Mint a DevCert exactly as exemem auth_service does: sign
    /// `JCS(cert_without_sig)` with the root key (ES256), DER-encode,
    /// base64 into `sig`.
    pub fn mint_dev_cert(
        &self,
        dev_pubkey_b64: &str,
        user_hash: &str,
        env: &str,
        issued_at: &str,
        expires_at: &str,
    ) -> DevCert {
        // Default to an authorized cert — most tests exercise the
        // happy/authorized path. Use `mint_dev_cert_with_authorized` to
        // forge an unauthorized cert (the squat-attempt case).
        self.mint_dev_cert_with_authorized(
            dev_pubkey_b64,
            user_hash,
            env,
            issued_at,
            expires_at,
            true,
        )
    }

    /// Same as [`mint_dev_cert`] but with explicit `authorized_publisher`.
    #[allow(clippy::too_many_arguments)]
    pub fn mint_dev_cert_with_authorized(
        &self,
        dev_pubkey_b64: &str,
        user_hash: &str,
        env: &str,
        issued_at: &str,
        expires_at: &str,
        authorized_publisher: bool,
    ) -> DevCert {
        let mut cert = DevCert {
            version: DEV_CERT_VERSION,
            purpose: PURPOSE_DEV_CERT.to_string(),
            alg: ALG_ES256.to_string(),
            key_id: self.key_id(),
            dev_pubkey: dev_pubkey_b64.to_string(),
            user_hash: user_hash.to_string(),
            issued_at: issued_at.to_string(),
            expires_at: expires_at.to_string(),
            env: env.to_string(),
            authorized_publisher,
            sig: String::new(),
        };
        let bytes = dev_cert_signing_bytes(&cert);
        let signature: P256Signature = self.signing_key.sign(&bytes);
        cert.sig = BASE64.encode(signature.to_der().as_bytes());
        cert
    }
}

/// A throwaway developer Ed25519 keypair.
pub struct TestDev {
    signing_key: EdSigningKey,
    /// Base64 of the 32-byte Ed25519 public key — the `dev_pubkey` a cert
    /// vouches for.
    pub pubkey_b64: String,
}

impl TestDev {
    pub fn generate() -> Self {
        let signing_key = EdSigningKey::generate(&mut OsRng);
        let pubkey_b64 = BASE64.encode(signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            pubkey_b64,
        }
    }

    /// Sign `payload` as a base64 [`SignatureEnvelope`] of `purpose` (the
    /// value for an `X-Signature` header).
    pub fn sign_envelope_b64(
        &self,
        purpose: Purpose,
        env: Env,
        payload: &Value,
        expires_at: Option<DateTime<Utc>>,
    ) -> String {
        self.sign_envelope_b64_at(purpose, env, payload, Utc::now(), expires_at)
    }

    /// Sign with an explicit fixture clock. Production verification still
    /// enforces the signed not-before and expiry bounds.
    pub fn sign_envelope_b64_at(
        &self,
        purpose: Purpose,
        env: Env,
        payload: &Value,
        issued_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> String {
        let unsigned = SignatureEnvelope {
            version: ENVELOPE_VERSION,
            purpose,
            alg: ALG_ED25519.to_string(),
            key_id: key_id(&self.signing_key.verifying_key()),
            issued_at,
            expires_at,
            env,
            payload_hash: compute_payload_hash(payload).expect("payload hash"),
            sig: None,
        };
        let signed = sign_envelope(&self.signing_key, unsigned).expect("sign envelope");
        BASE64.encode(serde_json::to_vec(&signed).expect("serialize envelope"))
    }
}

#[test]
fn explicit_fixture_clock_keeps_signature_and_not_before_checks() {
    let dev = TestDev::generate();
    let issued: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
    let payload = serde_json::json!({"synthetic": "field proof"});
    let encoded = dev.sign_envelope_b64_at(Purpose::SchemaClaim, Env::Dev, &payload, issued, None);
    let envelope: SignatureEnvelope =
        serde_json::from_slice(&BASE64.decode(encoded).unwrap()).unwrap();
    assert_eq!(envelope.issued_at, issued);
    assert_eq!(
        envelope.payload_hash,
        compute_payload_hash(&payload).unwrap()
    );
    let key = dev.signing_key.verifying_key();
    assert!(crate::envelope::verify_envelope_at(&key, &envelope, issued).is_ok());
    assert!(matches!(
        crate::envelope::verify_envelope_at(&key, &envelope, issued - chrono::Duration::seconds(1)),
        Err(crate::VerifyError::NotYetValid)
    ));
}

/// Base64-encode a DevCert for the `X-Exemem-Dev-Cert` header.
pub fn cert_header(cert: &DevCert) -> String {
    BASE64.encode(serde_json::to_vec(cert).expect("serialize cert"))
}

fn dev_cert_signing_bytes(cert: &DevCert) -> Vec<u8> {
    let mut value = serde_json::to_value(cert).expect("serialize cert");
    if let Value::Object(map) = &mut value {
        map.remove("sig");
    }
    canonicalize(&value).expect("canonicalize cert")
}
