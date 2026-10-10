//! Pure production-codec inspection of durable reverse-reference roots.

use super::*;
use crate::kind_partition::form_twin;

#[derive(Debug)]
pub enum OfflineAtomRefRoot {
    Active {
        atom_uuid: String,
        kind: &'static str,
    },
    Pending {
        atom_uuid: String,
    },
    CatalogTransition {
        atom_uuids: Vec<String>,
        transitions: usize,
    },
    Completion {
        history: bool,
    },
    Metadata,
}

fn invalid(message: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

fn token_bytes(token: &str) -> Result<Vec<u8>, SchemaError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| invalid("invalid compact reference token"))?;
    if bytes.len() != 32 || URL_SAFE_NO_PAD.encode(&bytes) != token {
        return Err(invalid("noncanonical compact reference token"));
    }
    Ok(bytes)
}

/// Decode one physical `aref` row with exact production key validation.
/// The caller preserves scope and walks every physical row; this performs no I/O.
pub fn offline_atom_ref_root(
    bare_key: &str,
    value: &[u8],
) -> Result<OfflineAtomRefRoot, SchemaError> {
    let normalized;
    let bare_key = if bare_key.starts_with("aref\0") {
        normalized =
            form_twin(bare_key).ok_or_else(|| invalid("invalid anchored reference key"))?;
        normalized.as_str()
    } else {
        bare_key
    };
    if let Some(rest) = bare_key.strip_prefix(ATOM_REF_V2_PREFIX) {
        let (atom, source) = rest
            .split_once('\0')
            .ok_or_else(|| invalid("compact reference has no source"))?;
        let atom = token_bytes(atom)?;
        if source == ATOM_LIVE_REFCOUNT_SUFFIX {
            let _: AtomLiveRefCount =
                serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
            return Ok(OfflineAtomRefRoot::Metadata);
        }
        token_bytes(source)?;
        if value != ATOM_REF_V2_ACTIVE_MARKER {
            return Err(invalid("compact reference has an invalid active marker"));
        }
        return Ok(OfflineAtomRefRoot::Active {
            atom_uuid: crate::hex::hex_lower(atom),
            kind: "active_v2",
        });
    }
    if bare_key.starts_with(molecule_key_codec::ATOM_REF_EDGE_PREFIX) {
        let edge: AtomRefEdge =
            serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
        if bare_key != edge.storage_key(None) || edge.atom_uuid.is_empty() {
            return Err(invalid("legacy reference key differs from its value"));
        }
        return Ok(if edge.active {
            OfflineAtomRefRoot::Active {
                atom_uuid: edge.atom_uuid,
                kind: "active_v1",
            }
        } else {
            OfflineAtomRefRoot::Metadata
        });
    }
    if bare_key.starts_with(ATOM_REF_PENDING_PREFIX) {
        let pending: PendingAtomRef =
            serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
        if pending.atom_uuid.is_empty()
            || pending.source.is_empty()
            || bare_key
                != AtomStore::pending_atom_ref_key(&pending.atom_uuid, &pending.source, None)
        {
            return Err(invalid("pending reference key differs from its value"));
        }
        return Ok(OfflineAtomRefRoot::Pending {
            atom_uuid: pending.atom_uuid,
        });
    }
    if bare_key == CATALOG_ATOM_REF_TRANSITIONS_KEY {
        let registry: CatalogAtomRefTransitionRegistry =
            serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
        let transitions = registry.transitions.len();
        let mut atoms = BTreeSet::new();
        for (id, transition) in registry.transitions {
            if id != catalog_atom_ref_transition_id(&transition.db_locator, &transition.schema_name)
                || transition.plan.transition_id != id
            {
                return Err(invalid("catalog transition identity differs"));
            }
            for atom in transition.plan.atom_deltas.into_keys() {
                if atom.is_empty() {
                    return Err(invalid("catalog transition has an empty atom"));
                }
                atoms.insert(atom);
            }
        }
        return Ok(OfflineAtomRefRoot::CatalogTransition {
            atom_uuids: atoms.into_iter().collect(),
            transitions,
        });
    }
    completion_or_metadata(bare_key, value)
}

fn completion_or_metadata(key: &str, value: &[u8]) -> Result<OfflineAtomRefRoot, SchemaError> {
    let key = if key.starts_with("aref\0") {
        form_twin(key).unwrap_or_else(|| key.to_string())
    } else {
        key.to_string()
    };
    if matches!(
        key.as_str(),
        molecule_key_codec::ATOM_REF_V2_COMPLETE_KEY
            | molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY
    ) {
        if value != ATOM_REF_V2_COMPLETE_MARKER {
            return Err(invalid("compact reference completion marker is invalid"));
        }
        return Ok(OfflineAtomRefRoot::Completion {
            history: key == molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
        });
    }
    if key.starts_with("aref:v1:m:") || key.starts_with("aref:v2:m:") {
        let _: Value = serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
        return Ok(OfflineAtomRefRoot::Metadata);
    }
    Err(invalid("unsupported atom reference root key"))
}

/// Every atom retained by a production mutation-history edge builder.
pub fn offline_history_atom_roots(key: &str, event: &MutationEvent) -> Vec<String> {
    mutation_history_edges(key, event)
        .into_iter()
        .map(|edge| edge.atom_uuid)
        .collect()
}
