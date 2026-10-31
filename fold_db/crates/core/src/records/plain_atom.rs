//! The plain atom row of a disk format v2 home.

/// The plain row of a disk format v2 home: the atom JSON with sorted keys, no
/// content seal and no molecule key bundle name. The block encryption of each
/// run file is the encryption at rest of this row.
pub(crate) fn encode_plain_atom(
    atom: &crate::atom::Atom,
) -> Result<Vec<u8>, crate::schema::SchemaError> {
    serde_json::to_value(atom)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|e| crate::schema::SchemaError::InvalidData(format!("serialize plain atom: {e}")))
}

pub(crate) fn decode_plain_atom(
    stored: &[u8],
) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
    let value = serde_json::from_slice(stored).map_err(|e| {
        crate::schema::SchemaError::InvalidData(format!("decode plain atom row JSON: {e}"))
    })?;
    serde_json::from_value(plain_atom_value(value)?)
        .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode plain atom: {e}")))
}

/// A plain row names no molecule key bundle: its content has no seal.
fn plain_atom_value(
    value: serde_json::Value,
) -> Result<serde_json::Value, crate::schema::SchemaError> {
    if value.get("molecule_key_bundle").is_some() {
        return Err(crate::schema::SchemaError::InvalidData(
            "a disk format v2 home holds plain atom rows, not sealed rows".into(),
        ));
    }
    Ok(value)
}
