//! Existing CAS and resident key contracts, without file-content decode.

use super::*;
use chrono::DateTime;
use serde::Deserialize;

pub(super) struct Identity {
    pub scope: String,
    pub reference: String,
}

pub(super) fn identity(collection: &str, key: &[u8]) -> Result<Option<Identity>, String> {
    if collection == "cas_blobs" {
        let reference = std::str::from_utf8(key).map_err(err)?;
        super::super::pointers::valid_blob_ref(reference)?;
        return Ok(Some(Identity {
            scope: String::new(),
            reference: reference.into(),
        }));
    }
    let Some(reference) = super::super::pointers::resident_blob_ref(key)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(key).map_err(err)?;
    let scope = if text.starts_with("cas_blob:") {
        ""
    } else {
        text.split_once(":cas_blob:")
            .ok_or("invalid resident blob key")?
            .0
    };
    if scope.contains('\0') || scope.contains("cas_blob:") {
        return Err("ambiguous resident blob storage scope".into());
    }
    Ok(Some(Identity {
        scope: scope.into(),
        reference,
    }))
}

// These fields match ContentAddressedBlob's durable identity envelope.
#[derive(Deserialize)]
struct BlobHeader {
    blob_ref: String,
    content_sha256: String,
    bytes_b64: String,
    size: u64,
    stored_at: Option<String>,
}

#[derive(Deserialize)]
struct ResidentHeader {
    stored_at: Option<String>,
    blob_ref: Option<String>,
    content_sha256: Option<String>,
}

pub(super) fn date(
    collection: &str,
    reference: &str,
    plain: &[u8],
) -> Result<Option<String>, String> {
    if plain.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err("selected blob header is not a JSON object".into());
    }
    let stored_at = if collection == "cas_blobs" {
        let header: BlobHeader = serde_json::from_slice(plain).map_err(err)?;
        if header.blob_ref != reference || format!("sha256:{}", header.content_sha256) != reference
        {
            return Err("selected blob header differs from its physical key".into());
        }
        if header.bytes_b64.is_empty() && header.size != 0 {
            return Err("selected nonempty blob has no stored bytes".into());
        }
        header.stored_at
    } else {
        let header: ResidentHeader = serde_json::from_slice(plain).map_err(err)?;
        if header
            .blob_ref
            .as_deref()
            .is_some_and(|value| value != reference)
            || header
                .content_sha256
                .as_deref()
                .is_some_and(|value| format!("sha256:{value}") != reference)
        {
            return Err("selected resident blob header differs from its physical key".into());
        }
        header.stored_at
    };
    if let Some(date) = &stored_at {
        DateTime::parse_from_rfc3339(date).map_err(err)?;
    }
    Ok(stored_at)
}
