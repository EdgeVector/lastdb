//! Current exact atom references from live tips and mutation-history events.
//! Writers store source changes and compact edge/count changes in one batch.
//! Only the proven-empty constructor creates global completeness proofs.

use super::{AtomStore, MoleculeData, PerKeyRecord};
use crate::atom::{molecule_key_codec, AtomEntry, MutationEvent};
use crate::clock::unix_nanos;
use crate::db_operations::DbCatalogStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::{Schema, SchemaError};
use crate::storage::KvMutation;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

mod catalog_refs;
mod counts_pending;
mod edge_keys;
mod transitions;

pub const ATOM_REF_V2_ACTIVE_MARKER: &[u8] = &[0x01];
const ATOM_REF_V2_PREFIX: &str = "aref:v2:e:";
const ATOM_LIVE_REFCOUNT_SUFFIX: &str = "!live-count";
const ATOM_LIVE_REFCOUNT_KEY_END: &[u8] = b"\0!live-count";
const ATOM_REF_PENDING_PREFIX: &str = "aref:pending:v1:";
const CATALOG_ATOM_REF_TRANSITIONS_KEY: &str = "aref:catalog-transitions:v1";
const MOLECULE_CATALOG_REFCOUNT_PREFIX: &str = "mref:v1:c:database-catalog:";
const ATOM_REF_V2_SOURCE_DOMAIN: &[u8] = b"lastdb:atom-ref-edge-source:v2\0";
pub(super) const ATOM_REF_V2_COMPLETE_MARKER: &[u8] = b"true";

/// The durable row type that keeps an atom alive.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum AtomRefEdgeType {
    Tip,
    MutationHistory,
}

impl AtomRefEdgeType {
    fn key_name(self) -> &'static str {
        match self {
            Self::Tip => "tip",
            Self::MutationHistory => "mutation-history",
        }
    }
}

/// One reverse edge. An inactive row is a logical removal that lands in the
/// same put batch as the new authoritative tip/history state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefEdge {
    pub atom_uuid: String,
    pub edge_type: AtomRefEdgeType,
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
    pub version_id: String,
    pub active: bool,
}

impl AtomRefEdge {
    /// Build the compact v2 key for this source relationship.
    pub fn storage_key_v2(&self, storage_prefix: Option<&str>) -> Result<String, SchemaError> {
        let atom_token = atom_content_sha256_token(&self.atom_uuid)?;
        let edge_token = atom_ref_v2_source_token(self);
        Ok(build_storage_key(
            storage_prefix,
            &format!("{ATOM_REF_V2_PREFIX}{atom_token}\0{edge_token}"),
        ))
    }
}

/// Durable number of committed live tip paths that retain one atom.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomLiveRefCount {
    pub live_refs: u64,
    pub exact: bool,
}

impl Default for AtomLiveRefCount {
    fn default() -> Self {
        Self {
            live_refs: 0,
            exact: true,
        }
    }
}

/// A short-lived hold for work that can create a tip but has not committed it.
/// Pending holds never change [`AtomLiveRefCount`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingAtomRef {
    pub atom_uuid: String,
    pub source: String,
    pub created_at_unix_nanos: u64,
}

/// Durable number of database-catalog paths to one molecule.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
struct MoleculeCatalogRefCount {
    live_refs: u64,
}

/// Prepared count changes for one database-catalog schema membership.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CatalogAtomRefPlan {
    storage_prefix: Option<String>,
    molecule_edges: Vec<super::MoleculeRefEdge>,
    atom_deltas: HashMap<String, u64>,
    pending_keys: Vec<String>,
    transition_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CatalogAtomRefTransitionKind {
    Add,
    Remove,
}

/// Recovery record that joins the `db_catalog` and `main` durability domains.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CatalogAtomRefTransition {
    db_locator: String,
    schema_name: String,
    kind: CatalogAtomRefTransitionKind,
    plan: CatalogAtomRefPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
struct CatalogAtomRefTransitionRegistry {
    transitions: HashMap<String, CatalogAtomRefTransition>,
}

fn atom_ref_v2_partition_prefix(
    atom_content_sha256: &str,
    storage_prefix: Option<&str>,
) -> Result<String, SchemaError> {
    let atom_token = atom_content_sha256_token(atom_content_sha256)?;
    Ok(build_storage_key(
        storage_prefix,
        &format!("{ATOM_REF_V2_PREFIX}{atom_token}\0"),
    ))
}

fn atom_content_sha256_token(atom_content_sha256: &str) -> Result<String, SchemaError> {
    if atom_content_sha256.len() != 64
        || !atom_content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SchemaError::InvalidData(
            "atom content identity must be a 64-character SHA-256 hex digest".to_string(),
        ));
    }
    let mut digest = [0_u8; 32];
    for (index, chunk) in atom_content_sha256.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|error| {
            SchemaError::InvalidData(format!("decode atom content SHA-256: {error}"))
        })?;
        digest[index] = u8::from_str_radix(pair, 16).map_err(|error| {
            SchemaError::InvalidData(format!("decode atom content SHA-256: {error}"))
        })?;
    }
    Ok(URL_SAFE_NO_PAD.encode(digest))
}

fn atom_ref_v2_source_token(edge: &AtomRefEdge) -> String {
    let mut digest = Sha256::new();
    digest.update(ATOM_REF_V2_SOURCE_DOMAIN);
    for part in [
        edge.edge_type.key_name().as_bytes(),
        edge.molecule_uuid.as_bytes(),
        edge.disk_hash.as_bytes(),
        edge.disk_range.as_bytes(),
        edge.version_id.as_bytes(),
    ] {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(part);
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
}

fn stable_token(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(bytes))
}

pub(super) fn atom_live_ref_count_key(
    atom_uuid: &str,
    storage_prefix: Option<&str>,
) -> Result<String, SchemaError> {
    Ok(build_storage_key(
        storage_prefix,
        &format!(
            "{ATOM_REF_V2_PREFIX}{}\0{ATOM_LIVE_REFCOUNT_SUFFIX}",
            atom_content_sha256_token(atom_uuid)?
        ),
    ))
}

fn molecule_catalog_ref_count_key(molecule_uuid: &str, storage_prefix: Option<&str>) -> String {
    build_storage_key(
        storage_prefix,
        &format!(
            "{MOLECULE_CATALOG_REFCOUNT_PREFIX}{}",
            stable_token(molecule_uuid.as_bytes())
        ),
    )
}

fn catalog_atom_ref_transition_id(db_locator: &str, schema_name: &str) -> String {
    let mut identity = Vec::with_capacity(db_locator.len() + schema_name.len() + 1);
    identity.extend_from_slice(db_locator.as_bytes());
    identity.push(0);
    identity.extend_from_slice(schema_name.as_bytes());
    stable_token(&identity)
}

fn catalog_atom_ref_transition_registry_mutation(
    registry: &CatalogAtomRefTransitionRegistry,
) -> Result<KvMutation, SchemaError> {
    if registry.transitions.is_empty() {
        return Ok(KvMutation::delete(
            CATALOG_ATOM_REF_TRANSITIONS_KEY.as_bytes().to_vec(),
        ));
    }
    Ok(KvMutation::put(
        CATALOG_ATOM_REF_TRANSITIONS_KEY.as_bytes().to_vec(),
        serde_json::to_vec(registry).map_err(|error| {
            SchemaError::InvalidData(format!(
                "serialize database-catalog atom reference transition registry: {error}"
            ))
        })?,
    ))
}

fn is_atom_live_ref_count_key(key: &[u8]) -> bool {
    key.ends_with(ATOM_LIVE_REFCOUNT_KEY_END)
}

fn tip_edge(
    molecule_uuid: &str,
    disk_hash: &str,
    disk_range: &str,
    entry: &AtomEntry,
    active: bool,
) -> AtomRefEdge {
    AtomRefEdge {
        atom_uuid: entry.atom_uuid.clone(),
        edge_type: AtomRefEdgeType::Tip,
        molecule_uuid: molecule_uuid.to_string(),
        disk_hash: disk_hash.to_string(),
        disk_range: disk_range.to_string(),
        version_id: tip_edge_identity(entry),
        active,
    }
}

fn mutation_history_edges(event_key: &str, event: &MutationEvent) -> Vec<AtomRefEdge> {
    let disk_hash = event.field_key.hash.as_deref().unwrap_or_default();
    let disk_range = event.field_key.range.as_deref().unwrap_or_default();
    let atoms: BTreeSet<&str> = std::iter::once(event.new_atom_uuid.as_str())
        .chain(event.old_atom_uuid.as_deref())
        .chain(event.conflict_loser_atom.as_deref())
        .collect();
    atoms
        .into_iter()
        .map(|atom_uuid| AtomRefEdge {
            atom_uuid: atom_uuid.to_string(),
            edge_type: AtomRefEdgeType::MutationHistory,
            molecule_uuid: event.molecule_uuid.clone(),
            disk_hash: disk_hash.to_string(),
            disk_range: disk_range.to_string(),
            version_id: event_key.to_string(),
            active: true,
        })
        .collect()
}

pub(crate) fn mutation_history_edge_items(
    event_key: &str,
    event: &MutationEvent,
    storage_prefix: Option<&str>,
) -> Result<Vec<(String, Value)>, SchemaError> {
    mutation_history_edges(event_key, event)
        .into_iter()
        .map(|edge| {
            let key = edge.storage_key_v2(storage_prefix)?;
            let value = serde_json::to_value(edge).map_err(|e| {
                SchemaError::InvalidData(format!("serialize mutation-history atom edge: {e}"))
            })?;
            Ok((key, value))
        })
        .collect()
}

fn tip_edge_identity(entry: &AtomEntry) -> String {
    format!(
        "{:020}:{}:{}",
        entry.written_at,
        entry.lww_device(),
        entry.atom_uuid
    )
}

fn push_edge_item(
    items: &mut Vec<(String, Value)>,
    edge: AtomRefEdge,
    storage_prefix: Option<&str>,
) -> Result<(), SchemaError> {
    let key = edge.storage_key_v2(storage_prefix)?;
    let value = serde_json::to_value(edge)
        .map_err(|e| SchemaError::InvalidData(format!("serialize atom reverse edge: {e}")))?;
    items.push((key, value));
    Ok(())
}

fn strip_storage_prefix<'a>(storage_prefix: Option<&str>, full_key: &'a str) -> Option<&'a str> {
    match storage_prefix {
        Some(prefix) => full_key.strip_prefix(prefix)?.strip_prefix(':'),
        None => Some(full_key),
    }
}

struct CompactEdgePlan {
    active_edges: HashMap<Vec<u8>, AtomRefEdge>,
    inactive_edges: HashMap<Vec<u8>, AtomRefEdge>,
    source_edge_keys: BTreeSet<String>,
}

fn is_atom_ref_edge_item_key(key: &str, storage_prefix: Option<&str>) -> bool {
    strip_storage_prefix(storage_prefix, key).is_some_and(|base_key| {
        base_key.starts_with(ATOM_REF_V2_PREFIX) && !is_atom_live_ref_count_key(base_key.as_bytes())
    })
}
