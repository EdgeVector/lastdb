//! Validate two exact schema definitions and their existing protein membership.

use super::{invalid, FoldDB, KeyValue, SchemaError};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::field_hash_coherence::{shared_field_identities, KeyLayoutFingerprint};
use crate::schema::types::{schema::DeclarativeSchemaType, Schema};
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) struct Binding {
    pub source_field: String,
    pub target_field: String,
    pub source_molecule: String,
    pub target_molecule: String,
}

pub(super) struct Plan {
    pub source: Schema,
    pub target: Schema,
    pub keys: Vec<KeyValue>,
    pub fields: Vec<Binding>,
    pub source_names: Vec<String>,
}

impl Plan {
    pub(super) fn new(
        db: &FoldDB,
        source: &str,
        target: &str,
        keys: &[KeyValue],
    ) -> Result<Self, SchemaError> {
        let source = load_schema(db, source)?;
        let target = load_schema(db, target)?;
        let mut distinct = HashSet::new();
        let keys = keys
            .iter()
            .map(|key| {
                if key.hash.is_none()
                    || (source.schema_type == DeclarativeSchemaType::HashRange)
                        != key.range.is_some()
                {
                    return Err(invalid(
                        "protein backfill source key does not match its schema layout",
                    ));
                }
                let key = db
                    .db_ops()
                    .molecule_keys()
                    .canonical_key(&source, key.clone())?;
                if !distinct.insert(key.clone()) {
                    return Err(invalid("protein backfill source keys must be distinct"));
                }
                Ok(key)
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        let fields = shared_field_identities(&source, &target)
            .into_iter()
            .map(|shared| {
                if source.get_field_type(&shared.a_field) != target.get_field_type(&shared.b_field)
                {
                    return Err(invalid("protein backfill shared field types differ"));
                }
                Ok(Binding {
                    source_molecule: molecule(&source, &shared.a_field)?,
                    target_molecule: molecule(&target, &shared.b_field)?,
                    source_field: shared.a_field,
                    target_field: shared.b_field,
                })
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        let source_names: BTreeSet<_> = fields
            .iter()
            .map(|field| field.source_field.clone())
            .chain(key_names(&source).into_iter().map(str::to_owned))
            .collect();
        if fields.is_empty() || source_names.len().saturating_mul(keys.len()) > 4096 {
            return Err(invalid(
                "protein backfill exceeds its 4096 source-tip budget or has no shared field",
            ));
        }
        for key_name in key_names(&target) {
            if !fields.iter().any(|field| field.target_field == key_name) {
                return Err(invalid(
                    "protein backfill target keys must have shared identities in the source schema",
                ));
            }
        }
        Ok(Self {
            source,
            target,
            keys,
            fields,
            source_names: source_names.into_iter().collect(),
        })
    }

    pub(super) async fn verify_membership(&self, atoms: &AtomStore) -> Result<(), SchemaError> {
        let molecules: Vec<_> = self
            .fields
            .iter()
            .flat_map(|field| [field.source_molecule.clone(), field.target_molecule.clone()])
            .collect();
        let owners = atoms.protein_of_molecules(&molecules).await?;
        if owners.len() != molecules.len() {
            return Err(invalid("protein backfill membership batch is incomplete"));
        }
        let ids: BTreeSet<_> = owners.iter().flatten().cloned().collect();
        let ids: Vec<_> = ids.into_iter().collect();
        let records = atoms.protein_get_many(&ids).await?;
        let records: HashMap<_, _> = ids.into_iter().zip(records).collect();
        let source = KeyLayoutFingerprint::from_schema(&self.source);
        let target = KeyLayoutFingerprint::from_schema(&self.target);
        for (index, field) in self.fields.iter().enumerate() {
            let id = owners[index * 2]
                .as_ref()
                .ok_or_else(|| invalid("source field is not protein-bound"))?;
            if owners[index * 2 + 1].as_ref() != Some(id) {
                return Err(invalid(
                    "source and target fields are not members of one protein",
                ));
            }
            let protein = records
                .get(id)
                .and_then(Option::as_ref)
                .ok_or_else(|| invalid("protein membership record is absent"))?;
            for (molecule, layout) in [
                (&field.source_molecule, &source),
                (&field.target_molecule, &target),
            ] {
                let hash = layout
                    .hash_field
                    .as_deref()
                    .ok_or_else(|| invalid("protein layout lacks a hash field"))?;
                if protein
                    .member_for_layout(molecule, hash, layout.range_field.as_deref())
                    .is_none()
                {
                    return Err(invalid("protein back-reference and member layout disagree"));
                }
            }
        }
        Ok(())
    }
}

pub(super) fn key_names(schema: &Schema) -> Vec<&str> {
    schema
        .key
        .iter()
        .flat_map(|key| {
            key.hash_field
                .as_deref()
                .into_iter()
                .chain(key.range_field.as_deref())
        })
        .collect()
}

pub(super) fn molecule(schema: &Schema, name: &str) -> Result<String, SchemaError> {
    schema
        .runtime_fields
        .get(name)
        .and_then(|field| field.common().molecule_uuid())
        .cloned()
        .ok_or_else(|| invalid("protein backfill field has no molecule binding"))
}

fn load_schema(db: &FoldDB, name: &str) -> Result<Schema, SchemaError> {
    let schema = db
        .schema_manager()
        .get_schema_metadata(name)?
        .ok_or_else(|| invalid("protein backfill schema is not installed"))?;
    if schema.name != name {
        return Err(invalid(
            "protein backfill requires an exact installed schema name",
        ));
    }
    db.schema_manager().require_queryable_schema(name)?;
    crate::schema::require_logical_layout(&schema.schema_type)?;
    let layout = schema
        .key
        .as_ref()
        .ok_or_else(|| invalid("protein backfill schema has no key layout"))?;
    if layout.hash_field.as_deref().is_none_or(str::is_empty)
        || (schema.schema_type == DeclarativeSchemaType::HashRange) != layout.range_field.is_some()
        || layout.range_field.as_deref() == Some("")
    {
        return Err(invalid(
            "protein backfill schema key layout is incomplete or inconsistent",
        ));
    }
    let mut identities = HashSet::new();
    if schema
        .field_hashes
        .values()
        .any(|identity| identity.is_empty() || !identities.insert(identity))
    {
        return Err(invalid(
            "protein backfill requires unambiguous field identities",
        ));
    }
    Ok(schema)
}
