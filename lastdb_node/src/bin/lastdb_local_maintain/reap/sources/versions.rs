//! Batched version frontiers; no serial read per chain node.

use super::super::keys::mol_key;
use super::super::tripwire::Tripwire;
use super::super::walk::PAGE_ROWS;
use super::super::ReapError;
use super::{abort, ChainRoot, SourceBuilder, VersionNode};
use fold_db::atom::{molecule_key_codec as codec, AtomEntry};
use fold_db::db_operations::atom_store::reap_version_source_keys;
use fold_db::kind_partition::form_twin;
use fold_db::storage::traits::KvStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const MAX_CHAIN: usize = 1_000_000;

impl SourceBuilder {
    pub(super) async fn read_versions(
        &mut self,
        raw: &Arc<dyn KvStore>,
        seam: &Arc<dyn KvStore>,
        tripwire: &Tripwire<'_>,
    ) -> Result<(), ReapError> {
        let mut frontier: Vec<(ChainRoot, String, usize)> = std::mem::take(&mut self.roots)
            .into_iter()
            .map(|mut source| {
                let version = std::mem::take(&mut source.version);
                (source, version, 0)
            })
            .collect();
        let mut source_ids = BTreeMap::new();
        let mut seen = BTreeSet::new();
        while !frontier.is_empty() {
            let missing: BTreeSet<String> = frontier
                .iter()
                .map(|(_, version, _)| version.clone())
                .filter(|version| !self.nodes.contains_key(version))
                .collect();
            let missing: Vec<String> = missing.into_iter().collect();
            for batch in missing.chunks(PAGE_ROWS) {
                self.load_batch(raw, seam, batch).await?;
            }
            let mut next = Vec::new();
            for (source, version, depth) in frontier {
                if depth >= MAX_CHAIN {
                    return Err(abort("tip-version chain exceeds the walk limit"));
                }
                let slot = (
                    source.molecule_uuid.clone(),
                    source.disk_hash.clone(),
                    source.disk_range.clone(),
                );
                let next_id = source_ids.len();
                let source_id = *source_ids.entry(slot).or_insert(next_id);
                if !seen.insert((source_id, version.clone())) {
                    continue;
                }
                let Some(node) = self.nodes.get(&version).cloned() else {
                    if version.len() == 64 && version.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        self.summary.legacy_missing_links += 1;
                        continue;
                    }
                    return Err(abort(format!("missing tip version {version}")));
                };
                tripwire.check(
                    &mol_key(&source.molecule_uuid),
                    node.written_at,
                    &version,
                    &mut self.summary.tripwire,
                )?;
                let (edges, backref) = reap_version_source_keys(
                    &source.molecule_uuid,
                    &source.disk_hash,
                    &source.disk_range,
                    &version,
                    &node.atom_uuid,
                );
                self.edges(edges)?;
                self.tips.add(&backref)?;
                for key in &node.keys {
                    self.tips.add(key)?;
                }
                if !node.prev_tip_id.is_empty() {
                    next.push((source, node.prev_tip_id, depth + 1));
                }
            }
            frontier = next;
        }
        self.check_cycles()?;
        self.summary.version_nodes = self.nodes.len() as u64;
        self.summary.version_storage_keys =
            self.nodes.values().map(|node| node.keys.len() as u64).sum();
        Ok(())
    }

    async fn load_batch(
        &mut self,
        raw: &Arc<dyn KvStore>,
        seam: &Arc<dyn KvStore>,
        batch: &[String],
    ) -> Result<(), ReapError> {
        let keys: Vec<String> = batch
            .iter()
            .flat_map(|version| {
                let key = codec::tip_version_key(version);
                let twin = form_twin(&key).expect("tip version has a colon twin");
                [key, twin]
            })
            .collect();
        let bytes: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let (raw_values, plain_values) =
            tokio::join!(raw.get_many(bytes.clone()), seam.get_many(bytes));
        let raw_values =
            raw_values.map_err(|error| abort(format!("raw version batch: {error}")))?;
        let plain_values =
            plain_values.map_err(|error| abort(format!("decoded version batch: {error}")))?;
        if raw_values.len() != keys.len() || plain_values.len() != keys.len() {
            return Err(abort("version batch length differs"));
        }
        for (index, version) in batch.iter().enumerate() {
            let mut decoded: Option<AtomEntry> = None;
            let mut stored_keys = Vec::new();
            for at in [index * 2, index * 2 + 1] {
                if raw_values[at].is_some() != plain_values[at].is_some() {
                    return Err(abort(format!("hidden version row {:?}", keys[at])));
                }
                let Some(plain) = &plain_values[at] else {
                    continue;
                };
                let entry: AtomEntry = serde_json::from_slice(plain)
                    .map_err(|error| abort(format!("decode version {version}: {error}")))?;
                if decoded.as_ref().is_some_and(|old| old != &entry) {
                    return Err(abort(format!("version twins disagree for {version}")));
                }
                decoded = Some(entry);
                stored_keys.push(keys[at].clone());
            }
            if let Some(entry) = decoded {
                self.nodes.insert(
                    version.clone(),
                    VersionNode {
                        atom_uuid: entry.atom_uuid,
                        written_at: entry.written_at,
                        prev_tip_id: entry.prev_tip_id,
                        keys: stored_keys,
                    },
                );
            }
        }
        Ok(())
    }

    fn check_cycles(&self) -> Result<(), ReapError> {
        let mut finished = BTreeSet::new();
        for start in self.nodes.keys() {
            let mut path = BTreeSet::new();
            let mut cursor = start.as_str();
            while !finished.contains(cursor) {
                let Some(node) = self.nodes.get(cursor) else {
                    break;
                };
                if !path.insert(cursor.to_string()) {
                    return Err(abort(format!("tip-version cycle at {cursor}")));
                }
                cursor = &node.prev_tip_id;
            }
            finished.extend(path);
        }
        Ok(())
    }
}
