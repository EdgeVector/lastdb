//! Native counter/body probes keep the legacy planner's fault decisions.

use super::*;

impl AtomStore {
    pub(super) async fn native_counter_probes(
        &self,
        atoms: &[&String],
        scope: Option<&str>,
    ) -> Result<HashMap<String, AtomBefore>, SchemaError> {
        let mut out = HashMap::new();
        let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        else {
            return Ok(out);
        };
        // Each pair has at most two physical selectors, within the existing
        // 10,000-key batch cap. One captured root and budget cover all chunks.
        for chunk in atoms.chunks(READ_CHUNK / 2) {
            let keys = chunk
                .iter()
                .map(|atom| {
                    (
                        build_storage_key(scope, &atom_key_codec::counter_key(atom)),
                        build_storage_key(scope, &atom_key_codec::body_key(atom)),
                    )
                })
                .collect();
            let values = runtime.counter_body_probes(keys).await?;
            for (atom, value) in chunk.iter().zip(values) {
                let Some(value) = value else { continue };
                let counter = value
                    .value
                    .map(|bytes| serde_json::from_slice::<AtomCounter>(&bytes))
                    .transpose()
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read atom counters: {error}"))
                    })?;
                out.insert(
                    (*atom).clone(),
                    AtomBefore {
                        counter,
                        body_without_counter: value.fallback_present,
                    },
                );
            }
        }
        Ok(out)
    }
}
