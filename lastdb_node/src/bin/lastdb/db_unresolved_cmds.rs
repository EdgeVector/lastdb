//! `lastdb db unresolved-atoms`: the atom identities behind skipped reads.

use super::*;

pub(crate) fn db_unresolved_atoms(socket: &Path, json_only: bool) -> Result<(), String> {
    let response = get_path(socket, "/api/db/unresolved-atoms", Duration::from_secs(30))?;
    let value = parse_json_response(&response, "/api/db/unresolved-atoms")?;
    let report = value
        .get("unresolved_atoms")
        .ok_or_else(|| "response missing unresolved_atoms".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let edges = report
        .get("edges")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "response missing unresolved atom edge count".to_string())?;
    let rows = report
        .get("rows")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "response missing unresolved atom record key count".to_string())?;
    let capped = report
        .get("capped")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "response missing unresolved atom cap flag".to_string())?;
    println!("Unresolved atoms: {rows} record key(s), {edges} atom edge(s)");
    if capped {
        println!("Identity cap reached; these counts are lower bounds.");
    }
    for identity in report
        .get("identities")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "response missing unresolved atom identities".to_string())?
    {
        let key: fold_db::schema::types::key_value::KeyValue = serde_json::from_value(
            identity
                .get("key")
                .cloned()
                .ok_or_else(|| "unresolved atom identity lacks key".to_string())?,
        )
        .map_err(|error| format!("invalid unresolved atom key: {error}"))?;
        let atom_uuid = identity
            .get("atom_uuid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "unresolved atom identity lacks atom_uuid".to_string())?;
        let molecule_uuid = identity
            .get("molecule_uuid")
            .and_then(serde_json::Value::as_str);
        let schema = identity.get("schema").and_then(serde_json::Value::as_str);
        let field = identity.get("field").and_then(serde_json::Value::as_str);
        let storage_namespace = identity
            .get("storage_namespace")
            .and_then(serde_json::Value::as_str);
        let tip_storage_key = identity
            .get("tip_storage_key")
            .and_then(serde_json::Value::as_str);
        let atom_storage_key = identity
            .get("atom_storage_key")
            .and_then(serde_json::Value::as_str);
        println!(
            "  key={} atom_uuid={atom_uuid} molecule_uuid={molecule_uuid:?} schema={schema:?} field={field:?} namespace={storage_namespace:?} tip_storage_key={tip_storage_key:?} atom_storage_key={atom_storage_key:?}",
            key.to_storage_key()
        );
    }
    Ok(())
}
