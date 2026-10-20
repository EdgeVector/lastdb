use crate::schema::SchemaError;

/// Parse the outer scope before the kind. Never search a key payload for kinds.
pub(super) fn source_parts<'a>(
    key: &'a str,
    scopes: &[String],
) -> Result<(Option<&'a str>, &'a str), SchemaError> {
    for scope in scopes {
        if let Some(bare) = key
            .strip_prefix(scope)
            .and_then(|rest| rest.strip_prefix(':'))
        {
            return Ok((Some(&key[..scope.len()]), bare));
        }
    }
    if let Some(parts) = crate::kind_partition::split_org_storage_prefix(key) {
        return Ok((Some(parts.0), parts.1));
    }
    if let Some(rest) = key.strip_prefix("from:") {
        if let Some((sender, bare)) = rest.split_once(':') {
            if matches!(sender.len(), 32 | 64)
                && sender.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                let bare = if let Some(cloud) = bare.strip_prefix("cloud:") {
                    let (identity, bare) = cloud.split_once(':').ok_or_else(|| {
                        SchemaError::InvalidData("malformed cloud share source scope".into())
                    })?;
                    if identity.len() != 64
                        || !identity.bytes().all(|byte| byte.is_ascii_hexdigit())
                    {
                        return Err(SchemaError::InvalidData(
                            "malformed cloud share identity".into(),
                        ));
                    }
                    bare
                } else {
                    bare
                };
                let prefix_end = key.len() - bare.len() - 1;
                return Ok((Some(&key[..prefix_end]), bare));
            }
        }
    }
    let kind_end = key.find([':', '\0']).unwrap_or(key.len());
    let kind = &key[..kind_end];
    if !outer_kind(kind)
        && [":mk:", ":mgp:v1:", ":atom:", ":atom\0"]
            .iter()
            .any(|token| key.contains(token))
    {
        return Err(SchemaError::InvalidData(format!(
            "blob reference bootstrap cannot certify unsupported source scope in {key:?}"
        )));
    }
    Ok((None, key))
}

pub(super) fn outer_kind(kind: &str) -> bool {
    // These are outer main-store families. Their payloads can contain any
    // user hash/range text, including strings that resemble another kind.
    const OUTER_KINDS: &[&str] = &[
        "mk",
        "mh",
        "mgp",
        "mgr",
        "mgd",
        "atom",
        "aloc",
        "aref",
        "bref",
        "mref",
        "tv",
        "history",
        "conflict",
        "ref",
        "dellog",
        "mord",
        "moc",
        "protein",
        "molprot",
        "fldprot",
        "pfq",
        "mhr",
        "mhk",
        "mhi",
        "schema_atoms",
        "idx",
        "schemaidx",
        "cas_blob",
        "gcatoms-probe-ref",
    ];
    OUTER_KINDS.contains(&kind)
}

/// Unsupported legacy scopes must not masquerade as an unrelated outer kind.
/// Recognize generation key structure before examining a payload: generation
/// records can contain key text and record-shaped values legitimately.
pub(super) fn reject_hidden_source(key: &str, value: &[u8]) -> Result<(), SchemaError> {
    if generation_payload_key(key)
        || ![":mk:", ":mgp:v1:", ":atom:", ":atom\0"]
            .iter()
            .any(|token| key.contains(token))
    {
        return Ok(());
    }
    let json = serde_json::from_slice::<serde_json::Value>(value).ok();
    let source_value = value.starts_with(crate::atom::ATOM_BINARY_ROW_PREFIX)
        || json.as_ref().is_some_and(|row| {
            row.get("entry").is_some()
                || row.get("source_schema_name").is_some()
                || (row.get("generation").is_some() && row.get("activated_at_unix_nanos").is_some())
        });
    if source_value {
        return Err(SchemaError::InvalidData(format!(
            "blob reference bootstrap cannot certify ambiguous legacy source scope in {key:?}"
        )));
    }
    Ok(())
}

fn generation_payload_key(key: &str) -> bool {
    use crate::atom::molecule_key_codec as codec;
    if let Some(rest) = key.strip_prefix("mgr:v1:") {
        let Some((molecule, rest)) = rest.split_once(':') else {
            return false;
        };
        let Some((generation, _)) = rest.split_once(':') else {
            return false;
        };
        if molecule.is_empty() || generation.is_empty() {
            return false;
        }
        return codec::molecule_record_key_from_generation_key(molecule, generation, key)
            .is_some_and(|base| codec::decode_hash_range(&base, molecule).is_some());
    }
    if let Some(rest) = key.strip_prefix("mgd:v1:") {
        let Some((molecule, _)) = rest.split_once(':') else {
            return false;
        };
        if molecule.is_empty() {
            return false;
        }
        return codec::molecule_record_key_from_generation_delete_key(molecule, key)
            .is_some_and(|base| codec::decode_hash_range(&base, molecule).is_some());
    }
    false
}
