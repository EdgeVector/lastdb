//! Multi-key coherence on schema load: bind matching fields of sibling key
//! layouts into proteins.

use super::super::locks::read_map;
use super::super::{CoherenceBinding, SchemaCore};
use crate::atom::deterministic_molecule_uuid;
use crate::schema::field_hash_coherence::{
    are_multi_key_siblings, bind_cross_key_field_protein, shared_field_identities,
    KeyLayoutFingerprint,
};
use crate::schema::types::{Schema, SchemaError};

impl SchemaCore {
    /// Bind this schema's fields to the proteins of any multi-key sibling.
    ///
    /// Detection is entirely local: [`are_multi_key_siblings`] over the field
    /// identities minted in [`Self::load_schema_internal`]. Identity equality on
    /// its own is deliberately not enough to bind — see the module docs on
    /// `field_hash_coherence` for the `created_at` case that makes it unsafe.
    ///
    /// Peers are scanned under a shared read lock over the schema cache, reading
    /// only each candidate's key, owner, and field identities. Nothing is cloned
    /// until a sibling is actually found, so the pass stays cheap on a catalog of
    /// a thousand-plus schemas.
    pub async fn apply_field_hash_coherence_on_load(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        let Some(loaded) = self.get_schema_metadata(schema_name)? else {
            return Ok(());
        };
        if loaded.field_hashes.is_empty() {
            return Ok(());
        }
        let target_layout = KeyLayoutFingerprint::from_schema(&loaded);
        if !target_layout.is_keyable() {
            return Ok(());
        }

        let bindings = {
            let schemas = read_map(&self.schemas, "schemas")?;
            let staged = self.staged_schemas()?;
            let committed = schemas
                .iter()
                .filter(|(name, _)| !staged.iter().any(|(staged_name, _)| staged_name == *name));
            let staged = staged.iter().map(|(name, cached)| (name, &cached.schema));
            coherence_bindings(schema_name, &loaded, committed.chain(staged))
        };
        if bindings.is_empty() {
            self.publish_coherence_memo(schema_name, bindings)?;
            return Ok(());
        }

        // Already established durably by this process, and nothing about the
        // binding set has changed — the durable half below would re-derive the
        // same proteins and rewrite the same breadcrumbs. See the
        // `coherence_bound` field docs for why this is safe when a new sibling
        // schema is registered later.
        if self.coherence_memo(schema_name)?.as_ref() == Some(&bindings) {
            return Ok(());
        }

        let outcome = self
            .bind_coherence_proteins(schema_name, &loaded, &target_layout, &bindings)
            .await;

        // Memoize only a clean sweep. A binding that errored is not established,
        // and recording it would turn one transient store failure into a
        // permanently unbound field for the life of the process — the pass would
        // never look at it again.
        if outcome.bound > 0 {
            tracing::info!(
                target: "field_hash_coherence",
                schema = %schema_name,
                fields = outcome.bound,
                "bound multi-key sibling fields into proteins"
            );
        }
        if outcome.failed == 0 {
            self.publish_coherence_memo(schema_name, bindings)?;
        }

        Ok(())
    }

    /// Execute bindings in order. Failures remain nonfatal and prevent memoization.
    async fn bind_coherence_proteins(
        &self,
        schema_name: &str,
        loaded: &Schema,
        target_layout: &KeyLayoutFingerprint,
        bindings: &[CoherenceBinding],
    ) -> BindingOutcome {
        let store = self.db_ops.atoms();
        let mut outcome = BindingOutcome::default();
        for binding in bindings {
            let own_mol = loaded
                .field_molecule_uuids
                .as_ref()
                .and_then(|m| m.get(&binding.field))
                .cloned()
                .unwrap_or_else(|| deterministic_molecule_uuid(schema_name, &binding.field));
            // Peer first: it is the established member and may already own tips.
            match bind_cross_key_field_protein(
                store,
                &binding.field_hash,
                &binding.peer_molecule,
                &binding.peer_layout,
                &own_mol,
                target_layout,
            )
            .await
            {
                Ok(Some(_)) => outcome.bound += 1,
                Ok(None) => {}
                Err(e) => {
                    outcome.failed += 1;
                    tracing::warn!(
                        target: "field_hash_coherence",
                        schema = %schema_name,
                        field = %binding.field,
                        error = %e,
                        "auto protein bind on schema load failed (non-fatal)"
                    );
                }
            }
        }
        outcome
    }
}

#[derive(Default)]
struct BindingOutcome {
    bound: usize,
    failed: usize,
}

/// Compare borrowed peer definitions. No storage calls, locks, or whole-catalog
/// clones. Each side uses its own field name; field identity joins renamed fields.
fn coherence_bindings<'a>(
    schema_name: &str,
    loaded: &Schema,
    peers: impl Iterator<Item = (&'a String, &'a Schema)>,
) -> Vec<CoherenceBinding> {
    let mut out = Vec::new();
    for (peer_name, peer) in peers {
        if peer_name == schema_name || !are_multi_key_siblings(peer, loaded) {
            continue;
        }
        let peer_layout = KeyLayoutFingerprint::from_schema(peer);
        for shared in shared_field_identities(peer, loaded) {
            let peer_molecule = peer
                .field_molecule_uuids
                .as_ref()
                .and_then(|m| m.get(&shared.a_field))
                .cloned()
                .unwrap_or_else(|| deterministic_molecule_uuid(peer_name, &shared.a_field));
            out.push(CoherenceBinding {
                field: shared.b_field,
                field_hash: shared.field_hash,
                peer_molecule,
                peer_layout: peer_layout.clone(),
            });
        }
    }
    // Preserve the comparison and binding order, independent of cache iteration.
    out.sort_by(|x, y| {
        (&x.field_hash, &x.peer_molecule, &x.field).cmp(&(
            &y.field_hash,
            &y.peer_molecule,
            &y.field,
        ))
    });
    out
}
