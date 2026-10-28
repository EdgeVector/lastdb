//! Recover API keys from one bounded batch of selected field values.

use super::*;
use crate::atom::MoleculeKeyCodec;
use crate::schema::types::key_value::{scalar_key_coordinate, KeyValue};
use serde_json::Value;

impl AtomStore {
    pub(super) async fn list_key_values(
        &self,
        request: &KeyList<'_>,
        codec: &MoleculeKeyCodec,
        mut rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, String, String)>, SchemaError> {
        rows.retain(|(_, record)| !record.meta.as_ref().is_some_and(|meta| meta.tombstoned));
        let values = self.list_field_values(request, codec, &rows).await?;
        let mut selected = Vec::new();
        let mut keys = Vec::with_capacity(rows.len());
        for ((key, _), value) in rows.iter().zip(&values) {
            let (storage_hash, range) = decoded_key(request, codec, key)?;
            let hash = listed_hash(
                request,
                codec,
                &storage_hash,
                value.as_ref().map(|v| &v.field.value),
            )?;
            let api_key = hash
                .as_ref()
                .map(|hash| KeyValue::new(Some(hash.clone()), Some(range.clone())));
            keys.push(api_key);
        }
        if let Membership::Schema(membership) = request.membership {
            let requested: Vec<_> = keys.iter().filter_map(Clone::clone).collect();
            let expected = requested.len();
            let matches = (membership.check)(requested).await?;
            if matches.len() != expected {
                return Err(SchemaError::StorageRead {
                    detail: "listed schema membership count differs".into(),
                });
            }
            let mut matches = matches.into_iter();
            for key in &mut keys {
                if key.is_some() && !matches.next().expect("listed membership alignment") {
                    *key = None;
                }
            }
            if request.hash.is_none() {
                self.certify_listed_tips(request, &rows, &keys).await?;
            }
            self.admit_listed_values(request, &keys, values)?;
        }
        for ((storage_key, _), key) in rows.iter().zip(keys) {
            if let Some(key) = key {
                selected.push((
                    key.hash.unwrap_or_default(),
                    key.range.unwrap_or_default(),
                    storage_key.clone(),
                ));
            }
        }
        Ok(selected)
    }
}

pub(super) fn decoded_key(
    request: &KeyList<'_>,
    codec: &MoleculeKeyCodec,
    key: &str,
) -> Result<(String, String), SchemaError> {
    let (hash, range) = molecule_key_codec::decode_hash_range(key, request.molecule)
        .ok_or_else(|| SchemaError::InvalidData("invalid listed molecule key".into()))?;
    let range = codec
        .api_range(&range)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    Ok((hash, range))
}

fn listed_hash(
    request: &KeyList<'_>,
    codec: &MoleculeKeyCodec,
    storage_hash: &str,
    value: Option<&Value>,
) -> Result<Option<String>, SchemaError> {
    let scalar = value.and_then(scalar_key_coordinate);
    let recovered = match scalar.as_deref() {
        Some(hash) => (codec
            .storage_hash(request.molecule, hash)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            == storage_hash)
            .then_some(hash),
        None => None,
    };
    let hash = request.hash.or(recovered).unwrap_or(storage_hash);
    if let Membership::Schema(membership) = request.membership {
        let Some(value) = value.filter(|value| !crate::atom::tombstone::is_tombstone_value(value))
        else {
            return Ok(None);
        };
        if membership.named_layout
            && (recovered.is_none()
                || !KeyValue::new(Some(hash.to_owned()), None).hash_matches_scalar(value))
        {
            return Ok(None);
        }
        if request.hash.is_none() && recovered.is_none() {
            return Err(SchemaError::InvalidField(
                "schema key list cannot recover a blinded API hash; provide a hash filter".into(),
            ));
        }
    }
    Ok(Some(hash.to_owned()))
}
