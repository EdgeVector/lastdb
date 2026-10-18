//! Native writes share one immutable keyed graph with each captured disk root.

use super::ResidentGraph;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Arc;

/// The request queue and graph share this allocation; graph roots copy no body.
#[derive(Debug)]
pub(crate) struct NativeWriteRecord {
    pub(crate) collection: String,
    pub(crate) key: String,
    pub(crate) body: Option<Vec<u8>>,
}

/// Exact and ordered links share one record allocation, including delete markers.
#[derive(Clone, Debug, Default)]
struct Records {
    exact: HashMap<Vec<u8>, Arc<NativeWriteRecord>>,
    ordered: BTreeSet<Vec<u8>>,
}

/// A read retains this root after later writes or a flush replace the live root.
/// The accepted queue's operation and byte caps bound it; it holds no clean data.
#[derive(Clone, Debug, Default)]
pub(crate) struct NativeWriteGraph {
    collections: HashMap<String, Arc<Records>>,
    len: usize,
}

impl NativeWriteGraph {
    pub(crate) fn get(&self, collection: &str, key: &str) -> Option<Arc<NativeWriteRecord>> {
        self.collections
            .get(collection)?
            .exact
            .get(key.as_bytes())
            .map(Arc::clone)
    }

    pub(crate) fn range(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<NativeWriteRecord>> {
        if start >= end {
            return Vec::new();
        }
        self.collections
            .get(collection)
            .map_or_else(Vec::new, |records| {
                records
                    .ordered
                    .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
                    .map(|key| Arc::clone(&records.exact[key]))
                    .collect()
            })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn insert(&mut self, record: Arc<NativeWriteRecord>) {
        let collection = self
            .collections
            .entry(record.collection.clone())
            .or_default();
        let records = Arc::make_mut(collection);
        let key = record.key.as_bytes().to_vec();
        if records.exact.insert(key.clone(), record).is_none() {
            records.ordered.insert(key);
            self.len += 1;
        }
    }
}

impl ResidentGraph {
    /// Caller holds the native view read lock until it also captures disk.
    pub(crate) fn native_write_root(&self) -> Arc<NativeWriteGraph> {
        Arc::clone(&self.native_writes.read().expect("native write graph"))
    }

    /// Caller holds the native view write lock through complete publication.
    pub(crate) fn publish_native_writes(&self, records: Vec<Arc<NativeWriteRecord>>) {
        let mut root = self.native_writes.write().expect("native write graph");
        let graph = Arc::make_mut(&mut root);
        for record in records {
            graph.insert(record);
        }
    }

    /// A flush publishes its disk root before readers can capture the empty graph.
    pub(crate) fn clear_native_writes(&self) {
        *self.native_writes.write().expect("native write graph") =
            Arc::new(NativeWriteGraph::default());
    }
}
