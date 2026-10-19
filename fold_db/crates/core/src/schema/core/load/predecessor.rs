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

/// The local schema whose molecules an expanded schema keeps using.
struct Predecessor {
    name: String,
    field_molecule_uuids: HashMap<String, String>,
}

impl SchemaCore {
    /// Adopt the LOCAL predecessor's effective field→molecule map when loading
    /// an externally-expanded schema, so the expanded identity reads and writes
    /// the SAME molecules the predecessor's rows live in.
    ///
    /// Why derivation is not enough: a field's *effective* molecule is
    /// write-history-dependent — `ensure_molecule` re-anchors a mapped field to
    /// `deterministic(own_name, field)` when the mapped molecule does not exist
    /// on disk at first write — so the predecessor's persisted
    /// `field_molecule_uuids` (its ground truth, synced across its own loads
    /// and writes) can disagree per-field with what mapper derivation computes.
    /// A schema-service expansion re-derives from mappers
    /// (`expand_schema` clears `field_molecule_uuids`), so loading its product
    /// pointed enumeration at empty molecules and every historical row
    /// vanished under the expanded identity (lastgit LastgitCrEvent expansion
    /// on the CiStatus-shared collection; card
    /// fold-schema-expansion-shared-collection-rows-invisible). The node-side
    /// approval path solves this with `apply_field_mappers`; this is the
    /// missing equivalent for the load path, keyed on the predecessor
    /// relationship rather than the (service-side, possibly stale-anchored)
    /// mapper graph.
    ///
    /// A predecessor is a DIFFERENT local identity with the same normalized
    /// `descriptive_name` (the service decorates expansions with a trailing
    /// parenthesized token), the same owner, the same schema_type and key
    /// layout, whose field set is a SUBSET of the incoming schema's — i.e.
    /// exactly the schema this one expands. The subset gate is what makes the
    /// relation asymmetric: loading the OLD schema never adopts from the NEW.
    /// Fields the predecessor has no entry for keep their derived molecule
    /// (fresh fields get fresh molecules on first write, as before).
    pub(super) fn adopt_predecessor_molecule_uuids(
        &self,
        schema: &mut Schema,
        own_stored: Option<&Schema>,
    ) -> Result<(), SchemaError> {
        if Self::restore_own_stored_molecules(schema, own_stored) {
            return Ok(());
        }
        let Some(predecessor) = self.find_predecessor(schema)? else {
            return Ok(());
        };
        Self::adopt_from_predecessor(schema, predecessor);
        Ok(())
    }

    /// Same-identity continuity first: a stored record for THIS identity is
    /// the node's local truth (it carries previously adopted or
    /// write-established molecules). Restoring it here — BEFORE the first
    /// sync — makes reloads order-independent: the wire copy's
    /// service-derived values never overwrite what this node's data
    /// actually lives under, and boot order (expansion before predecessor)
    /// stops mattering once the first load persisted the adopted map.
    ///
    /// EXCEPT when the incoming copy carries a DIFFERENT (non-empty) mapper
    /// set than the stored record: a catalog refresh that delivers new
    /// mappers is a deliberate re-point, and derivation from those mappers
    /// must win (contract:
    /// same_name_catalog_reload_replaces_mapperless_definition_durably).
    ///
    /// Returns true when the stored field map was restored, which ends the
    /// adoption (no predecessor search).
    fn restore_own_stored_molecules(schema: &mut Schema, own_stored: Option<&Schema>) -> bool {
        let mapper_refresh = {
            let incoming = schema.field_mappers.as_ref().filter(|m| !m.is_empty());
            let stored = own_stored
                .and_then(|st| st.field_mappers.as_ref())
                .filter(|m| !m.is_empty());
            match incoming {
                Some(incoming) => stored != Some(incoming),
                None => false,
            }
        };
        let Some(stored_map) = own_stored
            .filter(|_| !mapper_refresh)
            .and_then(|st| st.field_molecule_uuids.as_ref())
            .filter(|m| !m.is_empty())
        else {
            return false;
        };
        let restored = Self::apply_field_molecules(schema, stored_map);
        if restored > 0 {
            tracing::info!(
                schema = %schema.name,
                restored,
                "restored this identity's stored molecule map over wire-derived values"
            );
        }
        true
    }

    /// Point each runtime field at the molecule `uuids` names for it; returns
    /// how many fields changed.
    fn apply_field_molecules(schema: &mut Schema, uuids: &HashMap<String, String>) -> usize {
        let mut changed = 0usize;
        for (field_name, field) in &mut schema.runtime_fields {
            if let Some(uuid) = uuids.get(field_name) {
                if field.common().molecule_uuid() != Some(uuid) {
                    field.common_mut().set_molecule_uuid(uuid.clone());
                    changed += 1;
                }
            }
        }
        changed
    }

    /// The widest local schema that this schema expands, if any.
    fn find_predecessor(&self, schema: &Schema) -> Result<Option<Predecessor>, SchemaError> {
        let Some(desc) = schema.descriptive_name.clone() else {
            return Ok(None);
        };
        let base = descriptive_base(&desc).to_string();
        let incoming_fields: std::collections::HashSet<String> = schema
            .fields
            .as_ref()
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default();
        if incoming_fields.is_empty() {
            return Ok(None);
        }
        let incoming_key = key_fingerprint(schema);

        let schemas = read_map(&self.schemas, "schemas")?;
        Ok(schemas
            .values()
            .filter(|c| c.name != schema.name)
            .filter(|c| {
                norm_owner(c.owner_app_id.as_deref()) == norm_owner(schema.owner_app_id.as_deref())
            })
            .filter(|c| {
                c.descriptive_name
                    .as_deref()
                    .is_some_and(|d| descriptive_base(d) == base)
            })
            .filter(|c| c.schema_type == schema.schema_type)
            .filter(|c| key_fingerprint(c) == incoming_key)
            .filter(|c| {
                c.fields.as_ref().is_some_and(|fs| {
                    !fs.is_empty() && fs.iter().all(|f| incoming_fields.contains(f))
                })
            })
            .filter(|c| {
                c.field_molecule_uuids
                    .as_ref()
                    .is_some_and(|m| !m.is_empty())
            })
            .max_by_key(|c| c.fields.as_ref().map_or(0, Vec::len))
            .map(|c| Predecessor {
                name: c.name.clone(),
                field_molecule_uuids: c.field_molecule_uuids.clone().unwrap_or_default(),
            }))
    }

    fn adopt_from_predecessor(schema: &mut Schema, predecessor: Predecessor) {
        let adopted = Self::apply_field_molecules(schema, &predecessor.field_molecule_uuids);
        if adopted > 0 {
            tracing::info!(
                schema = %schema.name,
                predecessor = %predecessor.name,
                adopted,
                "adopted predecessor molecule map for shared fields (schema expansion \
                 data continuity — reads/writes stay on the predecessor's molecules)"
            );
        }
    }
}
