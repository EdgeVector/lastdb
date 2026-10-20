//! Offline, whole-home rebuild. Ordinary keyed writes never walk these sources.

use super::*;
use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::storage::traits::PhysicalScanCursor;
use crate::storage::KvMutation;
use std::collections::{BTreeSet, HashSet};

mod sources;
use sources::source_parts;

const PAGE_ROWS: usize = 2_048;

type MoleculeSources = BTreeSet<(Option<String>, String)>;

#[derive(Default)]
struct BlobSources {
    edges: Vec<BlobRefEdge>,
    obsolete: Vec<Vec<u8>>,
    molecules: MoleculeSources,
    atoms_read: u64,
    tips_read: u64,
    scopes: Vec<String>,
}

impl AtomStore {
    /// Rebuild node-global roots from every scope and each selected generation.
    /// The caller excludes every other writer with the offline maintenance lock.
    /// A scoped rebuild cannot authorize deletion of node-global file blobs.
    pub async fn bootstrap_blob_ref_edges_from_atoms_on_isolated_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<BlobRefCompleteness, SchemaError> {
        if storage_prefix.is_some() {
            return Err(SchemaError::InvalidData(
                "blob reference completeness requires a whole-home offline rebuild".into(),
            ));
        }
        self.main_store
            .inner()
            .batch_delete(vec![
                BLOB_REF_COMPLETE_KEY.as_bytes().to_vec(),
                LEGACY_COMPLETE_KEY.as_bytes().to_vec(),
            ])
            .await?;
        let mut sources = self.collect_blob_sources().await?;
        // Bounded independent molecule reads share each batch. The existing
        // reader resolves selectors, sparse deletes and logical delete barriers.
        let molecules: Vec<_> = sources.molecules.iter().collect();
        for chunk in molecules.chunks(32) {
            let reads = chunk
                .iter()
                .map(|(scope, molecule)| self.load_all_mk_records(molecule, scope.as_deref()));
            let rows = futures::future::try_join_all(reads).await?;
            for ((scope, molecule), rows) in chunk.iter().zip(rows) {
                for (hash, range, entry, meta) in rows {
                    let key = build_storage_key(
                        scope.as_deref(),
                        &molecule_key_codec::hash_range_record_key(molecule, &hash, &range),
                    );
                    let record = super::super::PerKeyRecord { entry, meta };
                    sources.edges.extend(super::tips::tip_edges(&key, &record));
                    sources.tips_read = sources.tips_read.saturating_add(1);
                }
            }
        }
        self.install_blob_sources(sources).await
    }

    async fn blob_source_scopes(&self) -> Result<Vec<String>, SchemaError> {
        let Some(store) = &self.namespaced_store else {
            return Ok(Vec::new());
        };
        let catalog = store.open_namespace("db_catalog").await?;
        let mut scopes = BTreeSet::new();
        let mut cursor: Option<PhysicalScanCursor> = None;
        loop {
            let page = catalog
                .scan_range_physical_paged(b"v1:", b"v1;", cursor.as_ref(), PAGE_ROWS, 1)
                .await?;
            for (_, value) in page.rows {
                let entry: crate::db_operations::DbCatalogEntry = serde_json::from_slice(&value)
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("decode blob source catalog: {error}"))
                    })?;
                if let Some(scope) = entry.instance_id {
                    let probe = format!("{scope}:mk:probe");
                    if scope.is_empty() {
                        return Err(SchemaError::InvalidData(
                            "empty blob source catalog scope".into(),
                        ));
                    }
                    let fixed_scope = source_parts(&probe, &[]).ok().and_then(|parts| parts.0);
                    if fixed_scope != Some(scope.as_str()) {
                        let first = scope.split([':', '\0']).next().unwrap_or("");
                        if sources::outer_kind(first) {
                            return Err(SchemaError::InvalidData(format!(
                            "blob reference bootstrap cannot certify ambiguous catalog scope {scope:?}"
                        )));
                        }
                    }
                    scopes.insert(scope);
                }
            }
            let Some(next) = page.next_cursor else { break };
            if cursor.as_ref() == Some(&next) {
                return Err(SchemaError::InvalidData(
                    "blob source catalog walk did not advance".into(),
                ));
            }
            cursor = Some(next);
        }
        let mut scopes: Vec<_> = scopes.into_iter().collect();
        scopes.sort_by_key(|scope| std::cmp::Reverse(scope.len()));
        Ok(scopes)
    }

    async fn collect_blob_sources(&self) -> Result<BlobSources, SchemaError> {
        let mut sources = BlobSources {
            scopes: self.blob_source_scopes().await?,
            ..BlobSources::default()
        };
        let mut cursor: Option<PhysicalScanCursor> = None;
        loop {
            let page = self
                .main_store
                .inner()
                .scan_range_physical_paged(b"", b"\xff", cursor.as_ref(), PAGE_ROWS, 1)
                .await?;
            for (key, value) in &page.rows {
                let Ok(key) = std::str::from_utf8(key) else {
                    continue;
                };
                self.collect_blob_source_row(key, value, &mut sources)
                    .await?;
            }
            let Some(next) = page.next_cursor else { break };
            if cursor.as_ref() == Some(&next) {
                return Err(SchemaError::InvalidData(
                    "blob reference bootstrap physical walk did not advance".into(),
                ));
            }
            cursor = Some(next);
        }
        Ok(sources)
    }

    async fn collect_blob_source_row(
        &self,
        key: &str,
        value: &[u8],
        sources: &mut BlobSources,
    ) -> Result<(), SchemaError> {
        let (scope, bare) = source_parts(key, &sources.scopes)?;
        if crate::kind_partition::starts_with_kind(bare, "atom") {
            let uuid = Self::atom_uuid_from_body_key(bare).ok_or_else(|| {
                SchemaError::InvalidData(format!("malformed atom source {key:?}"))
            })?;
            let atom = self.decode_atom_bytes(value).await?;
            for blob_ref in
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
            {
                let mut edge = BlobRefEdge::atom(uuid, &blob_ref);
                edge.atom_scope = scope.map(str::to_string);
                sources.edges.push(edge);
            }
            sources.atoms_read = sources.atoms_read.saturating_add(1);
            // Located atom keys contain an `mk:` partition; it is not a tip.
            return Ok(());
        }
        for token in ["mk:", "mgp:v1:"] {
            if bare.starts_with(token) {
                let molecule = if token == "mk:" {
                    molecule_key_codec::molecule_uuid_from_storage_key(bare)
                } else {
                    bare.strip_prefix(token)
                        .filter(|molecule| !molecule.is_empty())
                }
                .ok_or_else(|| SchemaError::InvalidData(format!("malformed tip source {key:?}")))?;
                sources
                    .molecules
                    .insert((scope.map(str::to_string), molecule.to_string()));
            }
        }
        for token in [BLOB_REF_PREFIX, LEGACY_COMPLETE_KEY, BLOB_REF_COMPLETE_KEY] {
            if bare.starts_with(token) {
                sources.obsolete.push(key.as_bytes().to_vec());
                break;
            }
        }
        if scope.is_none() && !bare.starts_with("mk:") && !bare.starts_with("mgp:v1:") {
            sources::reject_hidden_source(bare, value)?;
        }
        Ok(())
    }

    async fn audit_blob_edge_keys(&self) -> Result<HashSet<String>, SchemaError> {
        let (start, end) = crate::kind_partition::exact_prefix_bounds(BLOB_REF_PREFIX);
        let mut actual = HashSet::new();
        let mut cursor: Option<PhysicalScanCursor> = None;
        loop {
            let page = self
                .main_store
                .inner()
                .scan_range_physical_paged(
                    start.as_bytes(),
                    end.as_bytes(),
                    cursor.as_ref(),
                    PAGE_ROWS,
                    1,
                )
                .await?;
            for (key, value) in page.rows {
                let key = String::from_utf8(key).map_err(|error| {
                    SchemaError::InvalidData(format!("decode blob root audit key: {error}"))
                })?;
                let edge: BlobRefEdge = serde_json::from_slice(&value).map_err(|error| {
                    SchemaError::InvalidData(format!("decode blob root audit value: {error}"))
                })?;
                if edge.storage_key(None) != key {
                    return Err(SchemaError::InvalidData(
                        "blob root audit identity mismatch".into(),
                    ));
                }
                actual.insert(key);
            }
            let Some(next) = page.next_cursor else { break };
            if cursor.as_ref() == Some(&next) {
                return Err(SchemaError::InvalidData(
                    "blob root audit did not advance".into(),
                ));
            }
            cursor = Some(next);
        }
        Ok(actual)
    }

    async fn install_blob_sources(
        &self,
        sources: BlobSources,
    ) -> Result<BlobRefCompleteness, SchemaError> {
        for chunk in sources.obsolete.chunks(PAGE_ROWS) {
            self.main_store.inner().batch_delete(chunk.to_vec()).await?;
        }
        let mut expected = HashSet::new();
        let mut puts = Vec::new();
        for edge in sources.edges {
            let key = edge.storage_key(None);
            if expected.insert(key.clone()) {
                puts.push(KvMutation::put(
                    key.into_bytes(),
                    serde_json::to_vec(&edge).map_err(|error| {
                        SchemaError::InvalidData(format!("encode blob root: {error}"))
                    })?,
                ));
            }
        }
        for chunk in puts.chunks(PAGE_ROWS) {
            self.main_store.inner().batch_mutate(chunk.to_vec()).await?;
        }
        let actual = self.audit_blob_edge_keys().await?;
        if actual != expected {
            return Err(SchemaError::InvalidData(
                "blob reference bootstrap audit mismatch".into(),
            ));
        }
        let proof = BlobRefCompleteness {
            version: 2,
            atoms_read: sources.atoms_read,
            tips_read: sources.tips_read,
            edges_written: expected.len() as u64,
        };
        self.main_store
            .put_item(BLOB_REF_COMPLETE_KEY, &proof)
            .await?;
        Ok(proof)
    }
}
