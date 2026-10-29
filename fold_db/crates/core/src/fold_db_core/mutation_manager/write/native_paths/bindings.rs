//! Global schema definitions supply aliases for scoped logical record paths.

use super::*;

type BindingBatch = BTreeMap<String, Option<Vec<Binding>>>;
type Definitions = BTreeMap<String, Option<crate::schema::types::Schema>>;

impl MutationManager {
    pub(super) async fn path_bindings_batch(
        &self,
        schema: &crate::schema::types::Schema,
        molecules: &std::collections::BTreeSet<String>,
    ) -> Result<BindingBatch, SchemaError> {
        let sources = futures::future::try_join_all(molecules.iter().map(|molecule| async move {
            let edges = self
                .db_ops
                .atoms()
                .molecule_ref_edges_for_molecule(molecule, None)
                .await?;
            Ok::<_, SchemaError>((molecule, binding_sources(edges)))
        }))
        .await?;
        // All molecules refer to the same captured private write view. Read
        // each distinct definition once, rather than decode and rebuild its
        // runtime fields separately for every schema-field edge. Keep this
        // batch local: a later stage can change the private view.
        let names: std::collections::BTreeSet<_> = sources
            .iter()
            .filter_map(|(_, sources)| sources.as_ref())
            .flatten()
            .map(|(name, _)| name.clone())
            .collect();
        let snapshots = futures::future::try_join_all(names.into_iter().map(|name| async move {
            let snapshot = self.db_ops.schemas().read_snapshot(&name).await?;
            Ok::<_, SchemaError>((name, snapshot.schema))
        }))
        .await?
        .into_iter()
        .collect();
        sources
            .into_iter()
            .map(|(molecule, sources)| {
                resolve_bindings(schema, molecule, sources, &snapshots)
                    .map(|bindings| (molecule.clone(), bindings))
            })
            .collect()
    }
}

fn binding_sources(
    edges: crate::db_operations::atom_store::MoleculeRefLookup,
) -> Option<Vec<(String, String)>> {
    if !edges.complete
        || edges.edges.iter().any(|edge| {
            !matches!(
                edge.edge_type.as_str(),
                "schema-field" | "protein-member" | "database-catalog-field"
            )
        })
    {
        return None;
    }
    edges
        .edges
        .into_iter()
        .filter(|edge| edge.edge_type == "schema-field")
        .map(|edge| serde_json::from_str(edge.source.strip_prefix("schema:")?).ok())
        .collect()
}

fn resolve_bindings(
    schema: &crate::schema::types::Schema,
    molecule: &str,
    sources: Option<Vec<(String, String)>>,
    snapshots: &Definitions,
) -> Result<Option<Vec<Binding>>, SchemaError> {
    let mut bindings = schema_bindings(schema, molecule)?;
    let Some(sources) = sources else {
        return Ok(None);
    };
    for (name, field) in sources {
        let Some(Some(schema)) = snapshots.get(&name) else {
            return Ok(None);
        };
        let binding = schema_bindings(schema, molecule)?
            .into_iter()
            .find(|binding| binding.name == field);
        let Some(binding) = binding else {
            return Ok(None);
        };
        if !bindings.contains(&binding) {
            bindings.push(binding);
            if bindings.len() > 10_000 {
                return Ok(None);
            }
        }
    }
    Ok((!bindings.is_empty()).then_some(bindings))
}

pub(super) fn schema_bindings(
    schema: &crate::schema::types::Schema,
    molecule: &str,
) -> Result<Vec<Binding>, SchemaError> {
    if schema.runtime_fields.len() > 10_000 {
        return Ok(Vec::new());
    }
    let mut bindings = Vec::new();
    for (name, field) in &schema.runtime_fields {
        if field.common().molecule_uuid().map(String::as_str) != Some(molecule) {
            continue;
        }
        let Some(identity) = schema.field_hashes.get(name).filter(|id| !id.is_empty()) else {
            return Ok(Vec::new());
        };
        bindings.push(Binding {
            schema: schema_identity(schema)?,
            field: identity.clone(),
            name: name.clone(),
        });
    }
    Ok(bindings)
}
