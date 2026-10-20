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
