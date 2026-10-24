//! Read-only compatibility for the earlier direct account-v2 CAS form.

use crate::sharing::delivery_wire::FILE_BLOB_CIPHER_SUITE;
use serde::Deserialize;

/// Earlier format-5 CAS records can retain their authenticated file envelope.
pub(crate) fn is_inner_protected_family(namespace: &str, key: &[u8]) -> bool {
    namespace == "cas_blobs"
        && key
            .strip_prefix(b"sha256:")
            .is_some_and(|hash| hash.len() == 64 && hash.iter().all(u8::is_ascii_hexdigit))
}

pub(crate) fn has_inner_protection(namespace: &str, key: &[u8], value: &[u8]) -> bool {
    is_inner_protected_family(namespace, key) && cas_has_inner_seal(key, value)
}

#[derive(Deserialize)]
struct FileContent<'a> {
    #[serde(borrow)]
    blob_ref: &'a str,
    #[serde(borrow)]
    content_sha256: &'a str,
    #[serde(borrow)]
    local_cipher_suite: &'a str,
    #[serde(borrow)]
    bytes_b64: &'a str,
    #[serde(default)]
    access: Option<serde::de::IgnoredAny>,
}

pub(crate) fn cas_has_inner_seal(key: &[u8], value: &[u8]) -> bool {
    serde_json::from_slice::<FileContent<'_>>(value).is_ok_and(|file| {
        file.access.is_none()
            && file.local_cipher_suite == FILE_BLOB_CIPHER_SUITE
            && file.blob_ref.as_bytes() == key
            && key.strip_prefix(b"sha256:") == Some(file.content_sha256.as_bytes())
            && file
                .bytes_b64
                .starts_with(crate::crypto::AT_REST_ENC_PREFIX)
    })
}
