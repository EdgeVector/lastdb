//! The live schema catalog and L, the set of live molecules.
//!
//! L comes from the strict catalog read only. A row that does not decode
//! stops the plan, because a short catalog would make a live molecule look
//! dead.

use std::collections::{BTreeMap, BTreeSet};

use fold_db::atom::deterministic_molecule_uuid;
use fold_db::db_operations::SchemaStore;
use fold_db::kind_partition::logical_row_id;
use fold_db::record_molecule::{is_record_molecule_field, record_molecule_uuid};
use fold_db::schema::Schema;
use fold_db::storage::traits::NamespacedStore;

use super::identities::Identities;
use super::keys::{mol_key, MolKey};
use super::walk::Walker;
use super::ReapError;

/// Counts of the sources that feed L, before the union.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LiveSources {
    pub runtime_fields: u64,
    pub persisted_field_molecule_uuids: u64,
    pub schema_molecule_uuid: u64,
    pub record_molecules: u64,
    pub derived_field_molecules: u64,
}

/// What the strict catalog read found.
#[derive(Debug, Default)]
pub(crate) struct CatalogFacts {
    pub schema_count: usize,
    /// `Schema.name` of every live schema.
    pub live_names: BTreeSet<String>,
    /// Every spelling of every live schema: key, name, canonical name,
    /// descriptive name and identity hash.
    pub spellings: BTreeSet<String>,
    /// L, by digest.
    pub live: BTreeSet<MolKey>,
    /// The first live schemas that use each live molecule.
    pub owners: BTreeMap<MolKey, Vec<String>>,
    pub sources: LiveSources,
}

/// Read the whole catalog strictly and build L.
///
/// The strict read and a physical walk of the `schemas` namespace must agree
/// on the rows. A strict read that misses a row would leave a live molecule
/// out of L.
pub(crate) async fn load(
    store: &dyn NamespacedStore,
    unwrap_key: Option<[u8; 32]>,
) -> Result<CatalogFacts, ReapError> {
    let catalog = SchemaStore::open_for_offline_read(store, unwrap_key)
        .await
        .map_err(|error| ReapError::Failed(format!("open schema catalog: {error}")))?;
    let schemas = catalog
        .get_all_schemas_strict()
        .await
        .map_err(|error| ReapError::abort("CATALOG_UNDECODABLE", error.to_string()))?;
    let raw_keys = list_row_keys(store).await?;
    check_complete(&raw_keys, &schemas)?;
    Ok(facts_of(&schemas))
}

/// Every key of the `schemas` namespace, by a walk of all physical groups.
async fn list_row_keys(store: &dyn NamespacedStore) -> Result<Vec<Vec<u8>>, ReapError> {
    let kv = store
        .open_namespace("schemas")
        .await
        .map_err(|error| ReapError::Failed(format!("open schemas: {error}")))?;
    let mut walker = Walker::new(kv);
    let mut keys = Vec::new();
    while let Some(page) = walker.next_page().await? {
        keys.extend(page.rows.into_iter().map(|(key, _)| key));
    }
    Ok(keys)
}

/// Gate: the strict read holds one schema for each row that the store holds.
pub(crate) fn check_complete(
    raw_keys: &[Vec<u8>],
    schemas: &std::collections::HashMap<String, Schema>,
) -> Result<(), ReapError> {
    let in_store: BTreeSet<String> = raw_keys
        .iter()
        .map(|key| logical_row_id(&String::from_utf8_lossy(key)))
        .collect();
    let read: BTreeSet<String> = schemas
        .values()
        .map(|schema| logical_row_id(&schema.name))
        .collect();
    if in_store == read {
        return Ok(());
    }
    let missing: Vec<String> = in_store.difference(&read).take(5).cloned().collect();
    let extra: Vec<String> = read.difference(&in_store).take(5).cloned().collect();
    Err(ReapError::abort(
        "CATALOG_INCOMPLETE",
        format!(
            "the store holds {} schema row(s), the strict read {}; not read: {missing:?}; \
             read but not in the store: {extra:?}",
            in_store.len(),
            read.len()
        ),
    ))
}

/// Build the facts from decoded schemas. Pure.
pub(crate) fn facts_of(schemas: &std::collections::HashMap<String, Schema>) -> CatalogFacts {
    let mut facts = CatalogFacts {
        schema_count: schemas.len(),
        ..CatalogFacts::default()
    };
    let mut names: Vec<(&String, &Schema)> = schemas.iter().collect();
    names.sort_by(|a, b| a.0.cmp(b.0));
    for (key, schema) in names {
        facts.live_names.insert(schema.name.clone());
        note_spellings(&mut facts.spellings, key, schema);
        add_schema_molecules(&mut facts, schema);
    }
    facts
}

fn note_spellings(out: &mut BTreeSet<String>, key: &str, schema: &Schema) {
    let mut own: Vec<String> = vec![
        key.to_string(),
        schema.name.clone(),
        schema.canonical_name(),
    ];
    own.extend(schema.descriptive_name.iter().cloned());
    own.extend(schema.identity_hash.iter().cloned());
    for spelling in own {
        out.extend(Identities::spellings_of(&spelling));
    }
}

fn add_schema_molecules(facts: &mut CatalogFacts, schema: &Schema) {
    let mut ids: Vec<String> = Vec::new();
    for field in schema.runtime_fields.values() {
        if let Some(id) = field.common().molecule_uuid() {
            facts.sources.runtime_fields += 1;
            ids.push(id.clone());
        }
    }
    for id in schema.field_molecule_uuids.iter().flat_map(|m| m.values()) {
        facts.sources.persisted_field_molecule_uuids += 1;
        ids.push(id.clone());
    }
    if let Some(id) = &schema.molecule_uuid {
        facts.sources.schema_molecule_uuid += 1;
        ids.push(id.clone());
    }
    ids.push(record_molecule_uuid(&schema.name));
    facts.sources.record_molecules += 1;
    let declared = schema
        .fields
        .iter()
        .flatten()
        .cloned()
        .chain(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        )
        .chain(
            schema
                .runtime_fields
                .keys()
                .filter(|name| !is_record_molecule_field(name))
                .cloned(),
        )
        .collect::<BTreeSet<String>>();
    for field in declared {
        facts.sources.derived_field_molecules += 1;
        ids.push(deterministic_molecule_uuid(&schema.name, &field));
    }
    for id in ids {
        let key = mol_key(&id);
        facts.live.insert(key);
        let owners = facts.owners.entry(key).or_default();
        if owners.len() < 3 && !owners.contains(&schema.name) {
            owners.push(schema.name.clone());
        }
    }
}

/// The collection of database catalogs.
const DB_CATALOG: &str = "db_catalog";

/// Gate: the `db_catalog` collection must hold no row.
///
/// A copied schema in a database catalog uses the same molecules as the schema
/// it copies. Such a copy keeps a molecule live, and L does not read it. A home
/// with such a row is outside what this plan can prove.
pub(crate) async fn check_no_database_catalog(base: &dyn NamespacedStore) -> Result<(), ReapError> {
    let present = base
        .list_namespaces()
        .await
        .map_err(|error| ReapError::Failed(format!("list collections: {error}")))?;
    if !present.iter().any(|name| name == DB_CATALOG) {
        return Ok(());
    }
    let kv = base
        .open_namespace(DB_CATALOG)
        .await
        .map_err(|error| ReapError::Failed(format!("open {DB_CATALOG}: {error}")))?;
    let mut walker = Walker::new(kv);
    while let Some(page) = walker.next_page().await? {
        if let Some((key, _)) = page.rows.first() {
            return Err(ReapError::abort(
                "DB_CATALOG_PRESENT",
                format!(
                    "the {DB_CATALOG} collection holds rows, first {:?}. A copied schema there \
                     can keep a dropped molecule live.",
                    String::from_utf8_lossy(key)
                ),
            ));
        }
    }
    Ok(())
}

/// Gate: no spelling of a dropped name may be in the live catalog.
pub(crate) fn check_names_absent(ids: &Identities, facts: &CatalogFacts) -> Result<(), ReapError> {
    let present: Vec<&String> = ids
        .spellings
        .iter()
        .filter(|spelling| facts.spellings.contains(*spelling))
        .collect();
    if present.is_empty() {
        return Ok(());
    }
    Err(ReapError::abort(
        "N_IN_CATALOG",
        format!(
            "{} dropped name(s) are in the live catalog: {}",
            present.len(),
            present
                .iter()
                .take(5)
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ))
}
