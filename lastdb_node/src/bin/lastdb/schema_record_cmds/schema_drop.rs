use super::*;

pub(crate) fn schema_drop_command(
    data_dir: Option<PathBuf>,
    schema: Option<String>,
    owner_app: Option<String>,
    must_exist: bool,
    json_only: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let mut body = serde_json::json!({ "must_exist": must_exist });
    if let Some(schema) = schema {
        body["schema"] = serde_json::Value::String(schema);
    }
    if let Some(owner_app) = owner_app {
        body["owner_app"] = serde_json::Value::String(owner_app);
    }
    let response = db_post_json(&socket, "/api/schemas/drop", &body)?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize schema drop response: {error}"))?
        );
        return Ok(());
    }
    let dropped = response
        .get("dropped")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if dropped.is_empty() {
        println!("no installed schema dropped");
        return Ok(());
    }
    for name in dropped {
        if let Some(name) = name.as_str() {
            println!("dropped {name}");
        }
    }
    Ok(())
}
