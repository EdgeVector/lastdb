//! Synthetic records, the bundle blob plaintext framing, and its encryption.
//!
//! Plaintext (big-endian): `u32 count`, then per change `u16 ns_len` · namespace ·
//! `u16 key_len` · record key · `u32 value_len` · value, sorted ascending by
//! `(namespace, record_key)`. The blob is `nonce(12) ‖ AES-256-GCM(ciphertext ‖ tag)`
//! with a fresh random nonce per blob, so equal plaintexts never share a digest.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hkdf::Hkdf;
use mutation_log::{BundleChange, DatabaseId, BUNDLE_CHANGE_COUNT_MAX};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use sha2::Sha256;
use signed_history_materialize::encode_string_field;

/// FoldDB schema (history namespace) every synthetic record lands in.
pub const NAMESPACE: &str = "relay_e2e";
/// Random bytes behind one record; its `v` text is their base64 (48 KiB, under the 64 KiB record cap).
pub const VALUE_RAW_BYTES: usize = 36 * 1024;
/// Text length of one record's `v` field.
pub const VALUE_TEXT_BYTES: u64 = (VALUE_RAW_BYTES / 3 * 4) as u64;
/// Records per bundle: about 49 MB of plaintext, under the 64 MiB blob cap.
pub const BUNDLE_RECORDS: u64 = 1000;

const NONCE_BYTES: usize = 12;
const KEY_INFO: &[u8] = b"lastdb-relay-e2e-bundle-key-v1";
const SEED_DOMAIN: u64 = 0x6c61_7374_6462_0001;
const TRUNCATED: &str = "bundle blob truncated";

pub fn record_key(index: u64) -> String {
    format!("r{index:010}")
}

/// Deterministic incompressible text for record `index`.
pub fn record_text(index: u64) -> String {
    let mut rng = StdRng::seed_from_u64(SEED_DOMAIN ^ index);
    let mut raw = vec![0u8; VALUE_RAW_BYTES];
    rng.fill_bytes(&mut raw);
    URL_SAFE_NO_PAD.encode(raw)
}

/// History value of record `index`: the JSON object `{"v": "<text>"}`.
pub fn record_value(index: u64) -> Vec<u8> {
    encode_string_field(&record_text(index))
}

/// Changes for records `first .. first + count`, already in apply order.
pub fn bundle_changes(first: u64, count: u64) -> Vec<BundleChange> {
    (first..first + count)
        .map(|index| BundleChange {
            namespace: NAMESPACE.to_owned(),
            record_key: record_key(index),
            value: record_value(index),
        })
        .collect()
}

pub fn encode_changes(changes: &[BundleChange]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(changes.len()).map_err(|_| "too many changes in a bundle")?;
    let body: usize = changes
        .iter()
        .map(|c| 8 + c.namespace.len() + c.record_key.len() + c.value.len())
        .sum();
    let mut out = Vec::with_capacity(4 + body);
    out.extend_from_slice(&count.to_be_bytes());
    for change in changes {
        put_text(&mut out, &change.namespace)?;
        put_text(&mut out, &change.record_key)?;
        let len = u32::try_from(change.value.len()).map_err(|_| "record value over 4 GiB")?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&change.value);
    }
    Ok(out)
}

fn put_text(out: &mut Vec<u8>, text: &str) -> Result<(), String> {
    let len = u16::try_from(text.len()).map_err(|_| "text over 65535 bytes")?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

pub fn decode_changes(plain: &[u8]) -> Result<Vec<BundleChange>, String> {
    let mut reader = Reader { buf: plain, off: 0 };
    let count = reader.u32()?;
    if count > BUNDLE_CHANGE_COUNT_MAX {
        return Err("bundle blob holds more changes than the protocol cap".into());
    }
    let mut out = Vec::new();
    for _ in 0..count {
        let namespace = reader.text()?;
        let record_key = reader.text()?;
        let len = reader.u32()?;
        let value = reader.take(len as usize)?.to_vec();
        out.push(BundleChange {
            namespace,
            record_key,
            value,
        });
    }
    if reader.off != plain.len() {
        return Err("bundle blob has trailing bytes".into());
    }
    Ok(out)
}

struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self.off.checked_add(len).ok_or(TRUNCATED)?;
        let slice = self.buf.get(self.off..end).ok_or(TRUNCATED)?;
        self.off = end;
        Ok(slice)
    }

    fn u16(&mut self) -> Result<usize, String> {
        let raw: [u8; 2] = self.take(2)?.try_into().map_err(|_| TRUNCATED)?;
        Ok(usize::from(u16::from_be_bytes(raw)))
    }

    fn u32(&mut self) -> Result<u32, String> {
        let raw: [u8; 4] = self.take(4)?.try_into().map_err(|_| TRUNCATED)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn text(&mut self) -> Result<String, String> {
        let len = self.u16()?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| "bundle blob text is not UTF-8".to_owned())
    }
}

/// AES-256-GCM under a key derived from the database secret, bound to the database id.
pub struct BundleCipher {
    cipher: Aes256Gcm,
    aad: [u8; 32],
}

impl BundleCipher {
    pub fn new(secret: &[u8; 32], database_id: DatabaseId) -> Result<Self, String> {
        let mut key = [0u8; 32];
        Hkdf::<Sha256>::new(Some(database_id.as_bytes().as_slice()), secret)
            .expand(KEY_INFO, &mut key)
            .map_err(|_| "derive bundle key")?;
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| "bundle key length")?;
        Ok(Self {
            cipher,
            aad: database_id.to_bytes(),
        })
    }

    pub fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, String> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plain,
                    aad: &self.aad,
                },
            )
            .map_err(|_| "bundle encrypt failed")?;
        let mut out = Vec::with_capacity(NONCE_BYTES + sealed.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    pub fn open(&self, blob: &[u8]) -> Result<Vec<u8>, String> {
        let (nonce, msg) = blob
            .split_at_checked(NONCE_BYTES)
            .ok_or("bundle blob shorter than its nonce")?;
        self.cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg,
                    aad: &self.aad,
                },
            )
            .map_err(|_| "bundle decrypt failed (key, nonce or tag)".to_owned())
    }
}
