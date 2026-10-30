//! Fixed app, release, and channel writes.
//!
//! Signing seeds are public fixtures. They are not printed.

use std::collections::HashMap;

use app_identity_crypto::{
    canonicalize, compute_payload_hash, key_id, root_key_id, sign_envelope, DevCert, Env, Purpose,
    SignatureEnvelope, SigningKey, ALG_ED25519, ALG_ES256, DEV_CERT_VERSION, ENVELOPE_VERSION,
    PURPOSE_DEV_CERT,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use p256::ecdsa::signature::Signer;
use p256::pkcs8::EncodePublicKey;
use schema_service_core::app_release::{
    sha256_hex, ReleaseError, ReleaseManifest, ReleasePublishOutcome,
};
use schema_service_core::SchemaServiceState;
use serde_json::{json, Value};

use crate::state::Prepared;
use crate::{fail, normalize};

pub(crate) const APP_ID: &str = "baseline";
const APP_UUID: &str = "11111111-1111-4111-8111-111111111111";
const ISSUED_AT: &str = "2020-01-01T00:00:00Z";
const EXPIRES_AT: &str = "2099-01-01T00:00:00Z";
const USER_HASH: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DEV_SEED: [u8; 32] = [0x11; 32];
const ROOT_SEED: [u8; 32] = [0x42; 32];

pub(crate) struct Fixture {
    dev: SigningKey,
    root_spki: Vec<u8>,
    pub(crate) cert_header: String,
}

impl Fixture {
    pub(crate) fn load() -> Self {
        let dev = SigningKey::from_bytes(&DEV_SEED);
        let dev_pubkey = BASE64.encode(dev.verifying_key().to_bytes());
        let root = p256::ecdsa::SigningKey::from_slice(&ROOT_SEED)
            .unwrap_or_else(|error| fail("root seed", error));
        let root_spki = root
            .verifying_key()
            .to_public_key_der()
            .unwrap_or_else(|error| fail("root spki", error))
            .as_bytes()
            .to_vec();
        let cert_header = b64_json(&sign_dev_cert(&root, &dev_pubkey, &root_spki));
        Self {
            dev,
            root_spki,
            cert_header,
        }
    }

    pub(crate) fn trusted_roots(&self) -> HashMap<String, Vec<u8>> {
        let mut roots = HashMap::new();
        roots.insert(root_key_id(&self.root_spki), self.root_spki.clone());
        roots
    }

    pub(crate) fn sign(&self, purpose: Purpose, body: &Value) -> String {
        let payload_hash =
            compute_payload_hash(body).unwrap_or_else(|error| fail("payload", error));
        let unsigned = SignatureEnvelope {
            version: ENVELOPE_VERSION,
            purpose,
            alg: ALG_ED25519.to_string(),
            key_id: key_id(&self.dev.verifying_key()),
            issued_at: fixed_time(ISSUED_AT),
            expires_at: Some(fixed_time(EXPIRES_AT)),
            env: Env::Dev,
            payload_hash,
            sig: None,
        };
        let signed =
            sign_envelope(&self.dev, unsigned).unwrap_or_else(|error| fail("envelope", error));
        b64_json(&signed)
    }
}

pub(crate) async fn write_releases(
    fixture: &Fixture,
    prepared: &mut Prepared,
) -> (Vec<Value>, Vec<Value>) {
    let profiles = required_hash(&prepared.schema_hashes, "Family Profiles");
    let photos = required_hash(&prepared.schema_hashes, "Concert Photos");
    let both = manifest(
        "1111111111111111111111111111111111111111",
        &[("Concert Photos", photos), ("Family Profiles", profiles)],
    );
    let profiles_only = manifest(
        "2222222222222222222222222222222222222222",
        &[("Family Profiles", profiles)],
    );
    let both_id = publish(
        fixture,
        &prepared.state,
        &both,
        "created",
        &mut prepared.operations,
    )
    .await;
    publish(
        fixture,
        &prepared.state,
        &both,
        "idempotent",
        &mut prepared.operations,
    )
    .await;
    let profiles_id = publish(
        fixture,
        &prepared.state,
        &profiles_only,
        "created",
        &mut prepared.operations,
    )
    .await;
    set_channel(
        fixture,
        &prepared.state,
        "stable",
        &profiles_id,
        0,
        &mut prepared.operations,
    )
    .await;
    set_channel(
        fixture,
        &prepared.state,
        "stable",
        &both_id,
        1,
        &mut prepared.operations,
    )
    .await;
    set_channel(
        fixture,
        &prepared.state,
        "beta",
        &profiles_id,
        0,
        &mut prepared.operations,
    )
    .await;
    expect_conflict(
        fixture,
        &prepared.state,
        "stable",
        &profiles_id,
        0,
        &mut prepared.operations,
    )
    .await;
    revoke(
        fixture,
        &prepared.state,
        &profiles_id,
        &mut prepared.operations,
    )
    .await;
    (
        release_rows(&prepared.state, &[&both_id, &profiles_id]).await,
        channel_rows(&prepared.state).await,
    )
}

fn manifest(source_commit: &str, schemas: &[(&str, &str)]) -> Value {
    let mut locked = std::collections::BTreeMap::new();
    for (name, identity) in schemas {
        locked.insert((*name).to_string(), (*identity).to_string());
    }
    let manifest = ReleaseManifest {
        app_id: APP_ID.to_string(),
        app_uuid: APP_UUID.to_string(),
        schemas: locked,
        source_commit: source_commit.to_string(),
        artifact_digest: sha256_hex(source_commit.as_bytes()),
        artifact_url: format!("artifact://baseline/{source_commit}"),
        artifact_signature: "baseline-artifact-signature".to_string(),
        contract_range: None,
    };
    json!({ "manifest": manifest })
}

async fn publish(
    fixture: &Fixture,
    state: &SchemaServiceState,
    body: &Value,
    expect: &str,
    operations: &mut Vec<String>,
) -> String {
    let signature = fixture.sign(Purpose::AppReleasePublish, body);
    let outcome = state
        .publish_release(APP_ID, &fixture.cert_header, &signature, body)
        .await
        .unwrap_or_else(|error| fail("publish_release", format!("{error:?}")));
    let (label, record) = match outcome {
        ReleasePublishOutcome::Created(record) => ("created", record),
        ReleasePublishOutcome::Idempotent(record) => ("idempotent", record),
    };
    if label != expect {
        fail("publish_release", format!("expected {expect}, got {label}"));
    }
    operations.push(format!("publish:{label}:{}", record.release_id));
    record.release_id
}

async fn set_channel(
    fixture: &Fixture,
    state: &SchemaServiceState,
    channel: &str,
    release_id: &str,
    generation: u64,
    operations: &mut Vec<String>,
) {
    let record = channel_write(fixture, state, channel, release_id, generation)
        .await
        .unwrap_or_else(|error| fail("set_channel", format!("{error:?}")));
    operations.push(format!("channel:{channel}:{}", record.generation));
}

async fn expect_conflict(
    fixture: &Fixture,
    state: &SchemaServiceState,
    channel: &str,
    release_id: &str,
    generation: u64,
    operations: &mut Vec<String>,
) {
    match channel_write(fixture, state, channel, release_id, generation).await {
        Err(ReleaseError::GenerationConflict { expected, supplied }) => {
            operations.push(format!("channel:{channel}:conflict:{expected}:{supplied}"));
        }
        Ok(_) => fail("set_channel", "stale generation was accepted"),
        Err(error) => fail("set_channel", format!("expected conflict, got {error:?}")),
    }
}

async fn channel_write(
    fixture: &Fixture,
    state: &SchemaServiceState,
    channel: &str,
    release_id: &str,
    generation: u64,
) -> Result<schema_service_core::app_release::ChannelRecord, ReleaseError> {
    let body = json!({
        "app_id": APP_ID,
        "channel": channel,
        "generation": generation,
        "release_id": release_id,
    });
    let signature = fixture.sign(Purpose::AppChannelSet, &body);
    state
        .set_channel(APP_ID, channel, &fixture.cert_header, &signature, &body)
        .await
}

async fn revoke(
    fixture: &Fixture,
    state: &SchemaServiceState,
    release_id: &str,
    operations: &mut Vec<String>,
) {
    let body = json!({
        "app_id": APP_ID,
        "reason": "baseline fixture revocation",
        "release_id": release_id,
    });
    let signature = fixture.sign(Purpose::AppReleaseRevoke, &body);
    state
        .revoke_release(APP_ID, &fixture.cert_header, &signature, &body)
        .await
        .unwrap_or_else(|error| fail("revoke_release", format!("{error:?}")));
    operations.push(format!("revoke:{release_id}"));
}

async fn release_rows(state: &SchemaServiceState, ids: &[&str]) -> Vec<Value> {
    let mut rows = Vec::new();
    for release_id in ids {
        let record = state
            .get_release(release_id)
            .await
            .unwrap_or_else(|error| fail("get_release", error))
            .unwrap_or_else(|| fail("get_release", (*release_id).to_string()));
        rows.push(normalize(json!({
            "app_id": record.manifest.app_id,
            "app_uuid": record.manifest.app_uuid,
            "artifact_digest": record.manifest.artifact_digest,
            "artifact_signature": record.manifest.artifact_signature,
            "artifact_url": record.manifest.artifact_url,
            "publisher_dev_pubkey": record.publisher_dev_pubkey,
            "release_id": record.release_id,
            "revocation_reason": record.revocation_reason,
            "revoked": record.revoked(),
            "schemas": record.manifest.schemas,
            "source_commit": record.manifest.source_commit,
        })));
    }
    rows.sort_by_key(|left| row_str(left, "release_id"));
    rows
}

async fn channel_rows(state: &SchemaServiceState) -> Vec<Value> {
    let mut rows = Vec::new();
    for channel in ["beta", "stable"] {
        let record = state
            .get_channel(APP_ID, channel)
            .await
            .unwrap_or_else(|error| fail("get_channel", error))
            .unwrap_or_else(|| fail("get_channel", channel));
        rows.push(normalize(json!({
            "app_id": record.app_id,
            "channel": record.channel,
            "generation": record.generation,
            "release_id": record.release_id,
        })));
    }
    rows.sort_by_key(|left| row_str(left, "channel"));
    rows
}

fn required_hash<'a>(hashes: &'a HashMap<String, String>, name: &str) -> &'a str {
    hashes
        .get(name)
        .unwrap_or_else(|| fail("schema hash", name))
        .as_str()
}

fn sign_dev_cert(root: &p256::ecdsa::SigningKey, dev_pubkey: &str, root_spki: &[u8]) -> DevCert {
    let mut cert = DevCert {
        version: DEV_CERT_VERSION,
        purpose: PURPOSE_DEV_CERT.to_string(),
        alg: ALG_ES256.to_string(),
        key_id: root_key_id(root_spki),
        dev_pubkey: dev_pubkey.to_string(),
        user_hash: USER_HASH.to_string(),
        issued_at: ISSUED_AT.to_string(),
        expires_at: EXPIRES_AT.to_string(),
        env: "dev".to_string(),
        authorized_publisher: true,
        sig: String::new(),
    };
    let mut value = serde_json::to_value(&cert).unwrap_or_else(|error| fail("cert json", error));
    if let Value::Object(map) = &mut value {
        map.remove("sig");
    }
    let bytes = canonicalize(&value).unwrap_or_else(|error| fail("cert jcs", error));
    let signature: p256::ecdsa::Signature = root.sign(&bytes);
    cert.sig = BASE64.encode(signature.to_der());
    cert
}

fn b64_json<T: serde::Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_else(|error| fail("json", error));
    BASE64.encode(bytes)
}

fn fixed_time(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .unwrap_or_else(|error| fail("clock", error))
        .with_timezone(&Utc)
}

fn row_str(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}
