//! Operation Trinity bar — unit-level checks that product crypto defaults and
//! sealed paths hold. Invoked from `#[test]` and from host scripts via the same
//! pure functions where possible.

use super::content_at_rest::{
    atom_content_dual_read_enabled, open_content_value, reseal_atom_json_if_plain,
    seal_content_value,
};
use super::molecule_key_codec::{HashKeyEncoding, MoleculeKeyCodec, RangeKeyEncoding};
use crate::crypto::is_sealed_at_rest;
use serde_json::json;

/// Father: product env-default encodings are blind + OPE.
#[must_use]
pub fn father_encodings_are_strict_defaults() -> bool {
    // Do not mutate process env here — callers must unset plain overrides.
    matches!(
        (
            HashKeyEncoding::from_env_or_default(),
            RangeKeyEncoding::from_env_or_default()
        ),
        (HashKeyEncoding::BlindV1, RangeKeyEncoding::OpeV1)
    )
}

/// Father: storage hash for a greppable slug is not the slug itself under blind.
pub fn father_hash_not_greppable(
    index_key: &[u8; 32],
    molecule_uuid: &str,
    api_hash: &str,
) -> Result<bool, String> {
    let codec = MoleculeKeyCodec::with_encodings(
        HashKeyEncoding::BlindV1,
        RangeKeyEncoding::OpeV1,
        Some(*index_key),
        Some(*index_key),
    );
    let storage = codec
        .storage_hash(molecule_uuid, api_hash)
        .map_err(|e| e.to_string())?;
    Ok(storage != api_hash && !storage.contains(api_hash))
}

/// Son: sealed content round-trips; plain open fails under strict mode.
pub fn son_content_strict_roundtrip(content_key: &[u8; 32]) -> Result<(), String> {
    if atom_content_dual_read_enabled() {
        return Err(
            "dual-read still enabled; set LASTDB_ATOM_CONTENT_STRICT=1 for Trinity strict bar"
                .into(),
        );
    }
    let plain = json!({"title": "trinity-secret", "n": 1});
    let sealed = seal_content_value(content_key, &plain).map_err(|e| e.to_string())?;
    let s = sealed
        .as_str()
        .ok_or_else(|| "sealed content must be string".to_string())?;
    if !is_sealed_at_rest(s.as_bytes()) {
        return Err("sealed content missing ENC: prefix".into());
    }
    if s.contains("trinity-secret") {
        return Err("plaintext leaked into sealed content".into());
    }
    let opened = open_content_value(content_key, sealed).map_err(|e| e.to_string())?;
    if opened != plain {
        return Err("roundtrip mismatch".into());
    }
    match open_content_value(content_key, json!({"title": "still-plain"})) {
        Err(_) => Ok(()),
        Ok(_) => Err("plain content must fail closed under Trinity strict open".into()),
    }
}

/// Son: reseal upgrades plain atom content.
pub fn son_reseal_upgrades_plain(content_key: &[u8; 32]) -> Result<(), String> {
    let mut atom = json!({
        "uuid": "trinity-atom",
        "content": {"title": "was-plain"}
    });
    let changed = reseal_atom_json_if_plain(content_key, &mut atom).map_err(|e| e.to_string())?;
    if !changed {
        return Err("expected reseal to change plain content".into());
    }
    let raw = serde_json::to_string(&atom).map_err(|e| e.to_string())?;
    if raw.contains("was-plain") {
        return Err("plaintext remains after reseal".into());
    }
    Ok(())
}
