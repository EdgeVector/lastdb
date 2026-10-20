use super::*;

/// The counters of each named molecule: `slots` and `records` from the header and
/// `bindings` from the binding records. A molecule may go only when all three are
/// zero and none is `suspect` (state `droppable`). With one id the answer also
/// lists the binding records of that molecule, which is one range read under one
/// hash.
pub(super) async fn explain_molecules(
    host: &Host,
    ctx: &AccessContext,
    storage_prefix: Option<&str>,
    id: &str,
    mut ids: Vec<String>,
) -> UdsResponse {
    if !id.is_empty() {
        ids.insert(0, id.to_string());
    }
    ids.truncate(10_000);
    let atoms = host.db.db_ops().atoms();
    let result = async {
        let views = atoms.molecule_counter_views(&ids, storage_prefix).await?;
        let edges = match ids.as_slice() {
            [only] => Some(
                atoms
                    .molecule_ref_edges_for_molecule(only, storage_prefix)
                    .await?,
            ),
            _ => None,
        };
        Ok::<_, fold_db::schema::SchemaError>((views, edges))
    }
    .await;
    match result {
        Ok((views, lookup)) => {
            let rows: Vec<serde_json::Value> = views
                .iter()
                .map(|view| {
                    let mut row = serde_json::json!({
                        "molecule": view.molecule,
                        "header_present": view.header_present,
                        "slots": view.slots,
                        "records": view.records,
                        "bindings": view.bindings,
                        "suspect": view.suspect,
                        "state": view.state,
                        "droppable": view.droppable(),
                    });
                    if let Some(lookup) = &lookup {
                        row["binding_records_agree"] = lookup.complete.into();
                        row["binding_records"] = serde_json::json!(lookup.edges);
                    }
                    row
                })
                .collect();
            json_ok(&envelope(
                &serde_json::json!({ "liveness": { "class": "molecule", "molecules": rows } }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response(
            "liveness explain failed",
            fold_db::error::FoldDbError::Schema(error),
            ctx,
        ),
    }
}
