//! Molecule-map continuity when a schema is (re)loaded: restore this
//! identity's own stored map, or adopt the local predecessor's.

use std::collections::HashMap;

use super::super::locks::read_map;
use super::super::SchemaCore;
use crate::schema::types::{Schema, SchemaError};

fn descriptive_base(s: &str) -> &str {
    let t = s.trim_end();
    if t.ends_with(')') {
        if let Some(open) = t.rfind(" (") {
            return &t[..open];
        }
    }
    t
}

fn norm_owner(s: Option<&str>) -> Option<&str> {
    s.filter(|x| !x.is_empty())
}

fn key_fingerprint(s: &Schema) -> (Option<String>, Option<String>) {
    let norm = |v: Option<&str>| {
        v.map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_string)
    };
    match s.key.as_ref() {
        Some(k) => (
            norm(k.hash_field.as_deref()),
            norm(k.range_field.as_deref()),
        ),
        None => (None, None),
    }
}

/// Same-identity storage takes precedence unless incoming nonempty mappers differ.
/// Some(map) means authoritative, even if every runtime field already matches.
fn stored_molecules<'a>(
    incoming: &Schema,
    stored: Option<&'a Schema>,
) -> Option<&'a HashMap<String, String>> {
    let incoming_mappers = incoming.field_mappers.as_ref().filter(|m| !m.is_empty());
    let stored_mappers = stored
        .and_then(|s| s.field_mappers.as_ref())
        .filter(|m| !m.is_empty());
    let mapper_refresh = incoming_mappers.is_some() && incoming_mappers != stored_mappers;
    stored
        .filter(|_| !mapper_refresh)
        .and_then(|s| s.field_molecule_uuids.as_ref())
        .filter(|m| !m.is_empty())
}

/// Facts needed to recognize a local predecessor, without storage or cache access.
struct PredecessorMatch<'a> {
    incoming: &'a Schema,
    base: &'a str,
    fields: std::collections::HashSet<&'a String>,
    key: (Option<String>, Option<String>),
}

impl<'a> PredecessorMatch<'a> {
    fn new(incoming: &'a Schema) -> Option<Self> {
        let base = descriptive_base(incoming.descriptive_name.as_deref()?);
        let fields: std::collections::HashSet<_> = incoming.fields.as_ref()?.iter().collect();
        if fields.is_empty() {
            return None;
        }
        Some(Self {
            incoming,
            base,
            fields,
            key: key_fingerprint(incoming),
        })
    }

    /// Preserve iterator tie order: a tie must not change molecule ownership.
    fn select<'b>(&self, peers: impl Iterator<Item = &'b Schema>) -> Option<&'b Schema> {
        peers
            .filter(|peer| peer.name != self.incoming.name)
            .filter(|peer| {
                norm_owner(peer.owner_app_id.as_deref())
                    == norm_owner(self.incoming.owner_app_id.as_deref())
            })
            .filter(|peer| {
                peer.descriptive_name
                    .as_deref()
                    .is_some_and(|d| descriptive_base(d) == self.base)
            })
            .filter(|peer| peer.schema_type == self.incoming.schema_type)
            .filter(|peer| key_fingerprint(peer) == self.key)
            .filter(|peer| {
                peer.fields
                    .as_ref()
                    .is_some_and(|fs| !fs.is_empty() && fs.iter().all(|f| self.fields.contains(f)))
            })
            .filter(|peer| {
                peer.field_molecule_uuids
                    .as_ref()
                    .is_some_and(|m| !m.is_empty())
            })
            .max_by_key(|peer| peer.fields.as_ref().map_or(0, Vec::len))
    }
}

/// Update runtime definitions only. Unmatched new fields keep their derived UUIDs.
fn use_molecules(schema: &mut Schema, molecules: &HashMap<String, String>) -> usize {
    let mut changed = 0;
    for (name, field) in &mut schema.runtime_fields {
        if let Some(uuid) = molecules.get(name) {
            if field.common().molecule_uuid() != Some(uuid) {
                field.common_mut().set_molecule_uuid(uuid.clone());
                changed += 1;
            }
        }
    }
    changed
}

impl SchemaCore {
    /// Restore the node's effective molecule map before derived values reach disk.
    /// Write history can re-anchor a molecule, so catalog derivation alone cannot
    /// preserve historical data. Stored identity wins before predecessor selection.
    pub(super) fn adopt_predecessor_molecule_uuids(
        &self,
        schema: &mut Schema,
        own_stored: Option<&Schema>,
    ) -> Result<Option<Schema>, SchemaError> {
        if let Some(molecules) = stored_molecules(schema, own_stored) {
            let restored = use_molecules(schema, molecules);
            if restored > 0 {
                tracing::info!(
                    schema = %schema.name,
                    restored,
                    "restored this identity's stored molecule map over wire-derived values"
                );
            }
            return Ok(None);
        }
        let Some(criteria) = PredecessorMatch::new(schema) else {
            return Ok(None);
        };
        // Clone only the selected map, then release the cache lock before mutation.
        let selected = {
            let schemas = read_map(&self.schemas, "schemas")?;
            criteria
                .select(schemas.values())
                .map(Schema::clone_for_read_shared)
        };
        if let Some(predecessor) = selected {
            let molecules = predecessor
                .field_molecule_uuids
                .as_ref()
                .cloned()
                .unwrap_or_default();
            let adopted = use_molecules(schema, &molecules);
            if adopted > 0 {
                tracing::info!(
                    schema = %schema.name,
                    predecessor = %predecessor.name,
                    adopted,
                    "adopted predecessor molecule map for shared fields (schema expansion \
                     data continuity — reads/writes stay on the predecessor's molecules)"
                );
            }
            return Ok(Some(predecessor));
        }
        Ok(None)
    }
}
