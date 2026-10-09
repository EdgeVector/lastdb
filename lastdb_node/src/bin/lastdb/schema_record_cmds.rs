use super::*;

/// Offline Search rebuild: page product records into apps/search/inbox.
pub(super) fn search_rebuild_command(
    data_dir: Option<PathBuf>,
    page_size: usize,
    json_only: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    // Refuse if daemon holds the store (exclusive open would race).
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    if lastdb_node::health_alert::probe_health(&socket).is_ok() {
        return Err(format!(
            "lastdbd is running at {} — stop the daemon before offline search-rebuild (exclusive LastStore open)",
            socket.display()
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let report = runtime
        .block_on(async {
            fold_db::db_operations::search_index::run_search_rebuild_for_home(&home, page_size)
                .await
        })
        .map_err(|e| format!("search rebuild: {e}"))?;
    if json_only {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "pages": report.pages,
                "batches": report.batches,
                "changes": report.changes,
                "inbox": report.inbox,
                "home": home,
            })
        );
    } else {
        println!(
            "search rebuild: pages={} batches={} changes={} inbox={}",
            report.pages,
            report.batches,
            report.changes,
            report.inbox.display()
        );
    }
    Ok(())
}

pub(super) fn percent_encode_query(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub(super) fn list_record_keys(
    data_dir: Option<PathBuf>,
    schema: &str,
    key_hash: Option<&str>,
    limit: usize,
    cursor: Option<&str>,
    json_only: bool,
    verb: &str,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let mut path = format!(
        "/api/list?schema={}&limit={}",
        percent_encode_query(schema),
        limit.max(1)
    );
    if let Some(hash) = key_hash.filter(|s| !s.is_empty()) {
        path.push_str("&hash=");
        path.push_str(&percent_encode_query(hash));
    }
    if let Some(cursor) = cursor {
        path.push_str("&cursor=");
        path.push_str(&percent_encode_query(cursor));
    }
    let response = get_path(
        &socket,
        &path,
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "list")?;
    let report = value
        .get("list")
        .cloned()
        .ok_or_else(|| "response missing list field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_list_record_keys_human(&report, verb);
    Ok(())
}

pub(super) fn print_list_record_keys_human(report: &serde_json::Value, verb: &str) {
    let keys = report
        .get("keys")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_more = report
        .get("has_more")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let schema = report
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let hash_field = report
        .get("hash_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let range_field = report
        .get("range_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let key_field = report
        .get("key_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let hash_filter = report
        .get("hash_filter")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let layout = match (hash_field, range_field, key_field) {
        (Some(h), Some(r), _) => format!(" hash={h} range={r}"),
        (Some(h), None, _) => format!(" hash={h}"),
        (None, Some(r), _) => format!(" range={r}"),
        (None, None, Some(k)) => format!(" key={k}"),
        _ => String::new(),
    };
    let partition = hash_filter
        .map(|h| format!(" --key-hash {h}"))
        .unwrap_or_default();
    eprintln!(
        "lastdb {verb} {schema}{partition} — {} key{}{layout}{}",
        keys.len(),
        if keys.len() == 1 { "" } else { "s" },
        if has_more { " (more)" } else { "" }
    );
    if hash_field.is_some() || range_field.is_some() {
        match (hash_field, range_field) {
            (Some(h), Some(r)) => eprintln!("{h}\t{r}"),
            (Some(h), None) => eprintln!("{h}"),
            (None, Some(r)) => eprintln!("{r}"),
            _ => {}
        }
    }
    for key in &keys {
        let hash = key
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let range = key
            .get("range")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if range.is_empty() {
            println!("{hash}");
        } else {
            println!("{hash}\t{range}");
        }
    }
    if has_more {
        if let Some(cursor) = report
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
        {
            let hash_flag = hash_filter
                .map(|h| format!(" --key-hash '{h}'"))
                .unwrap_or_default();
            eprintln!("next: lastdb {verb} {schema}{hash_flag} --cursor '{cursor}'");
        }
    }
}

pub(super) fn schema_drop_command(
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

pub(super) fn schema_show_command(
    data_dir: Option<PathBuf>,
    name: &str,
    json_only: bool,
) -> Result<(), String> {
    let schema = fetch_schema(data_dir, name)?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_schema_show_human(&schema);
    Ok(())
}

pub(super) fn schema_storage_command(
    data_dir: Option<PathBuf>,
    schema: &str,
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json(
        &socket,
        "/api/storage/schema",
        &serde_json::json!({ "schema": schema }),
    )?;
    let value = parse_json_response(&response, "schema storage")?;
    let report = value
        .get("schema_storage")
        .cloned()
        .ok_or_else(|| "response missing schema_storage field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| format!("serialize: {error}"))?
        );
        return Ok(());
    }
    let schema_binding = report
        .get("schema_binding")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let current = report
        .get("schema_current_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("Schema logical current storage: {schema_binding}");
    println!("  {}  complete={complete}", format_bytes(current));
    println!("  Metric: logical_current_schema_bytes (shared data can appear in more than one schema total)");
    if !complete {
        println!(
            "  Counter bootstrap or replay is incomplete; this value is a safe partial result."
        );
    }
    Ok(())
}

pub(super) fn schema_storage_report_command(
    data_dir: Option<PathBuf>,
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let req = format!(
        "GET /api/storage/schemas HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(
        &socket,
        req.as_bytes(),
        lastdb_node::exec::admin_handler_timeout(),
    )?;
    let value = parse_json_response(&response, "schema storage report")?;
    let report = value
        .get("schema_storage_report")
        .cloned()
        .ok_or_else(|| "response missing schema_storage_report field".to_string())?;
    let pretty = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("serialize schema storage report: {error}"))?;
    if json_only {
        println!("{pretty}");
        return Ok(());
    }
    print_schema_storage_report_human(&report);
    Ok(())
}

pub(super) fn print_schema_storage_report_human(report: &serde_json::Value) {
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let total = report
        .get("total_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let schema_count = report
        .get("schema_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!(
        "Labelled logical schema storage  {}  schemas  complete={complete}",
        format_bytes(total)
    );
    println!();
    println!("  {:>12}  {:<32}  LABEL", "BYTES", "SCHEMA");
    if let Some(rows) = report
        .get("per_schema")
        .and_then(serde_json::Value::as_array)
    {
        for row in rows {
            let bytes = row
                .get("bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let binding = row
                .get("schema_binding")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let label = row
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(binding);
            println!("  {:>12}  {:<32}  {label}", format_bytes(bytes), binding);
        }
    }
    if !complete {
        let reason = report
            .get("incomplete_reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("stored counters are incomplete");
        println!("  CAUTION: {reason}");
    }
    if schema_count == 0 {
        println!("  (no installed schemas)");
    }
}

pub(super) fn liveness_explain_command(
    data_dir: Option<PathBuf>,
    class: &str,
    id: &str,
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json(
        &socket,
        "/api/storage/liveness/explain",
        &serde_json::json!({ "class": class, "id": id }),
    )?;
    let value = parse_json_response(&response, "liveness explain")?;
    let report = value
        .get("liveness")
        .cloned()
        .ok_or_else(|| "response missing liveness field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| format!("serialize: {error}"))?
        );
        return Ok(());
    }

    let returned_class = report
        .get("class")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(class);
    let returned_id = report
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(id);
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let reclaim_state = report
        .get("reclaim_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let edges = report
        .get("edges")
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len);
    println!("Liveness: {returned_class} {returned_id}");
    println!("  Complete: {complete}");
    println!("  Reclaim state: {reclaim_state}");
    println!("  Active edges in this target partition: {edges}");
    Ok(())
}

pub(super) fn liveness_bootstrap_command(
    data_dir: Option<PathBuf>,
    isolated_copy: bool,
    storage_prefix: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    if !isolated_copy {
        return Err("liveness bootstrap requires --isolated-copy".to_string());
    }
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let response = post_json_admin(
        &socket,
        "/api/storage/liveness/bootstrap",
        &serde_json::json!({
            "isolated_copy": true,
            "storage_prefix": storage_prefix,
        }),
    )?;
    let value = parse_json_response(&response, "liveness bootstrap")?;
    let report = value
        .get("liveness_bootstrap")
        .cloned()
        .ok_or_else(|| "response missing liveness_bootstrap field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| format!("serialize: {error}"))?
        );
        return Ok(());
    }
    let schema_fields = report
        .pointer("/molecule_refs/schema_fields")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let protein_members = report
        .pointer("/molecule_refs/protein_members")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let atoms = report
        .pointer("/blob_refs/atoms_read")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let blob_edges = report
        .pointer("/blob_refs/edges_written")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("Liveness bootstrap complete");
    println!("  Schema field sources: {schema_fields}");
    println!("  Protein member sources: {protein_members}");
    println!("  Atom sources read: {atoms}");
    println!("  Blob edges written: {blob_edges}");
    Ok(())
}

pub(super) fn fetch_schema(
    data_dir: Option<PathBuf>,
    name: &str,
) -> Result<serde_json::Value, String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let path = format!("/api/schema/{}", percent_encode_query(name));
    let response = get_path(&socket, &path, Duration::from_secs(10))?;
    let value = parse_json_response(&response, "schema")?;
    value
        .get("schema")
        .cloned()
        .ok_or_else(|| "response missing schema field".to_string())
}

pub(super) fn print_schema_show_human(schema: &serde_json::Value) {
    for line in schema_show_lines(schema) {
        println!("{line}");
    }
}

pub(super) fn schema_show_lines(schema: &serde_json::Value) -> Vec<String> {
    let name = schema
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let descriptive = schema
        .get("descriptive_name")
        .and_then(serde_json::Value::as_str);
    let identity = schema
        .get("identity_hash")
        .and_then(serde_json::Value::as_str);
    let owner = schema
        .get("owner_app_id")
        .and_then(serde_json::Value::as_str);
    let schema_type = schema
        .get("schema_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let state = schema
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let hash_field = schema
        .pointer("/key/hash_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let range_field = schema
        .pointer("/key/range_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    let mut lines = Vec::new();
    lines.push(format!("schema {name}"));
    if let Some(d) = descriptive {
        lines.push(format!("descriptive_name {d}"));
    }
    if let Some(id) = identity {
        lines.push(format!("identity_hash {id}"));
    }
    if let Some(app) = owner {
        lines.push(format!("owner_app {app}"));
    }
    lines.push(format!("type {schema_type}"));
    lines.push(format!("state {state}"));
    match (hash_field, range_field) {
        (Some(h), Some(r)) => {
            lines.push(format!("hash_field {h}"));
            lines.push(format!("range_field {r}"));
        }
        (Some(h), None) => lines.push(format!("hash_field {h}")),
        (None, Some(r)) => lines.push(format!("range_field {r}")),
        (None, None) => lines.push("key_fields (none)".to_string()),
    }
    if fields.is_empty() {
        lines.push("fields (none)".to_string());
    } else {
        lines.push(format!("fields {fields}"));
    }
    lines.push("queries:".to_string());
    for q in legal_query_shapes(schema_type, hash_field, range_field) {
        lines.push(format!("  {q}"));
    }
    lines
}

pub(super) fn legal_query_shapes(
    schema_type: &str,
    hash_field: Option<&str>,
    range_field: Option<&str>,
) -> Vec<String> {
    let hash = hash_field.unwrap_or("HASH");
    let range = range_field.unwrap_or("RANGE");
    match schema_type {
        "HashRange" => vec![
            format!(
                "point get O(1): lastdb get <schema> --key-hash <{hash}> --key-range <{range}>"
            ),
            format!("range under one hash O(log M): lastdb get-keys <schema> --key-hash <{hash}>"),
            "scan does not exist".to_string(),
        ],
        "Hash" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{hash}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        "Range" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{range}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        _ => vec![
            "point get: lastdb get <schema> --key-hash <KEY>".to_string(),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
    }
}

pub(super) fn schema_is_hash_range(schema: &serde_json::Value) -> bool {
    let typed = schema
        .get("schema_type")
        .and_then(serde_json::Value::as_str)
        == Some("HashRange");
    let both_key_halves = schema
        .pointer("/key/range_field")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.is_empty())
        && schema
            .pointer("/key/hash_field")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.is_empty());
    typed || both_key_halves
}

pub(super) fn schema_field_names(schema: &serde_json::Value) -> Vec<String> {
    schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn get_query_body(
    schema: &str,
    fields: &[String],
    key_hash: &str,
    key_range: Option<&str>,
) -> serde_json::Value {
    let filter = match key_range.filter(|s| !s.is_empty()) {
        Some(range) => serde_json::json!({
            "HashRangeKey": { "hash": key_hash, "range": range }
        }),
        None => serde_json::json!({ "HashKey": key_hash }),
    };
    serde_json::json!({
        "schema_name": schema,
        "fields": fields,
        "filter": filter,
    })
}

pub(super) fn compact_record_command(
    data_dir: Option<PathBuf>,
    schema: &str,
    key_hash: &str,
    key_range: &str,
    json_only: bool,
) -> Result<(), String> {
    if schema.trim().is_empty() || key_hash.trim().is_empty() || key_range.trim().is_empty() {
        return Err("compact-record needs <schema> --key-hash --key-range".into());
    }
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let body = serde_json::json!({
        "schema": schema.trim(),
        "hash": key_hash.trim(),
        "range": key_range.trim(),
    });
    let response = post_json_admin(&socket, "/api/db/compact-record", &body)?;
    let value = parse_json_response(&response, "compact-record")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let keys = value
        .get("keys_compacted")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let r = value
        .get("record_molecule_uuid")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let skipped = value
        .get("keys_skipped")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("compact-record keys_compacted={keys} keys_skipped={skipped} r={r}");
    if skipped > 0 {
        eprintln!(
            "compact-record: {skipped} key(s) skipped: the field zip was empty or missed a live \
             field tip, so no envelope was stamped. Retry after pending writes persist."
        );
    }
    Ok(())
}

pub(super) fn get_record(
    data_dir: Option<PathBuf>,
    schema_name: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    let Some(hash) = key_hash.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err("get needs --key-hash (and --key-range when the schema is HashRange)".into());
    };
    let schema = fetch_schema(data_dir.clone(), schema_name)?;
    if schema_is_hash_range(&schema) && key_range.map(str::trim).filter(|s| !s.is_empty()).is_none()
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let fields = schema_field_names(&schema);
    let body = get_query_body(schema_name, &fields, hash, key_range);
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json_with_timeout(
        &socket,
        "/api/query",
        &body,
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "query")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_get_human(&value);
    Ok(())
}

pub(super) fn print_get_human(value: &serde_json::Value) {
    let results = value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if results.is_empty() {
        eprintln!("lastdb get: no row");
        return;
    }
    for (i, row) in results.iter().enumerate() {
        if results.len() > 1 {
            eprintln!("--- row {} ---", i + 1);
        }
        if let Some(key) = row.get("key") {
            match key {
                serde_json::Value::String(s) => println!("key {s}"),
                serde_json::Value::Object(map) => {
                    let hash = map
                        .get("hash")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let range = map
                        .get("range")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if range.is_empty() {
                        println!("key {hash}");
                    } else {
                        println!("key {hash}\t{range}");
                    }
                }
                _ => {}
            }
        }
        if let Some(fields) = row.get("fields").and_then(serde_json::Value::as_object) {
            let mut names: Vec<&String> = fields.keys().collect();
            names.sort();
            for name in names {
                let rendered = match fields.get(name) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                println!("{name} {rendered}");
            }
        }
    }
}

pub(super) fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    request_with_timeout(socket, req.as_bytes(), timeout)
}

/// Content key for one offline rewrite. Drop zeros the bytes.
pub(super) struct OfflineContentKey([u8; 32]);

impl OfflineContentKey {
    pub(super) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for OfflineContentKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub(super) fn load_offline_content_key(home: &Path) -> Result<OfflineContentKey, String> {
    let path = home.join("identity.key");
    let mut bytes = std::fs::read(&path).map_err(|_| "identity key is missing".to_string())?;
    if bytes.len() != 32 {
        bytes.fill(0);
        return Err("identity key length".to_string());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    bytes.fill(0);
    let keys = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|_| "identity key is not usable".to_string())?;
    seed.fill(0);
    Ok(OfflineContentKey(keys.encryption_key()))
}

pub(super) fn default_version_cutoff_nanos() -> u64 {
    fold_db::clock::unix_nanos().saturating_sub(laststore::SUPERSEDED_VERSION_RETENTION_NANOS)
}

pub(super) fn print_version_retention_report(
    report: &laststore::VersionRetentionReport,
    execute: bool,
    json: bool,
) -> Result<(), String> {
    if json {
        let body = serde_json::json!({
            "execute": execute,
            "version_cutoff_nanos": report.version_cutoff_nanos,
            "version_keys": report.version_keys,
            "heads_with_chain": report.heads_with_chain,
            "heads_truncated": report.heads_truncated,
            "heads_skipped_tombstoned": report.heads_skipped_tombstoned,
            "bodies_unopened": report.bodies_unopened,
            "versions_kept": report.versions_kept,
            "versions_dropped": report.versions_dropped,
            "links_rewritten": report.links_rewritten,
            "backrefs_dropped": report.backrefs_dropped,
            "groups_rewritten": report.groups_rewritten,
            "tips_bytes_before": report.tips_bytes_before,
            "tips_bytes_after": report.tips_bytes_after,
        });
        let text = serde_json::to_string_pretty(&body).map_err(|error| error.to_string())?;
        println!("{text}");
        return Ok(());
    }
    println!("execute={}", if execute { 1 } else { 0 });
    println!("version_cutoff_nanos={}", report.version_cutoff_nanos);
    println!("version_keys={}", report.version_keys);
    println!("heads_with_chain={}", report.heads_with_chain);
    println!("heads_truncated={}", report.heads_truncated);
    println!(
        "heads_skipped_tombstoned={}",
        report.heads_skipped_tombstoned
    );
    println!("bodies_unopened={}", report.bodies_unopened);
    println!("versions_kept={}", report.versions_kept);
    println!("versions_dropped={}", report.versions_dropped);
    println!("links_rewritten={}", report.links_rewritten);
    println!("backrefs_dropped={}", report.backrefs_dropped);
    println!("groups_rewritten={}", report.groups_rewritten);
    println!("tips_bytes_before={}", report.tips_bytes_before);
    println!("tips_bytes_after={}", report.tips_bytes_after);
    Ok(())
}

/// Rewrite tip files for versions of a live record. The daemon must already
/// be stopped. This command does not stop or start the daemon.
pub(super) fn db_retain_superseded_versions_offline(
    data_dir: Option<PathBuf>,
    execute: bool,
    version_cutoff_nanos: Option<u64>,
    json: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let store_root = home.join("data");
    let socket = store_root.join("folddb.sock");
    if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
        return Err(format!(
            "refusing: the database socket is open at {}",
            socket.display()
        ));
    }
    let layout_path = store_root.join("laststore-layout-v1");
    let layout = std::fs::read_to_string(&layout_path)
        .map_err(|_| format!("layout file is missing: {}", layout_path.display()))?;
    if !layout.lines().any(|line| line == "packaging=plain") {
        return Err("refusing: packaging is not plain".to_string());
    }
    if laststore::home_has_frame_aead_segments(&store_root) {
        return Err("refusing: frame segments are present".to_string());
    }
    let key = load_offline_content_key(&home)?;
    let lock_path = store_root.join("maintenance.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(|error| format!("maintenance lock: {error}"))?;
    if let Err(error) = rustix::fs::flock(
        &lock_file,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    ) {
        return Err(format!("maintenance lock is held: {error}"));
    }
    let cutoff = version_cutoff_nanos.unwrap_or_else(default_version_cutoff_nanos);
    eprintln!(
        "retain-superseded-versions-offline store={} execute={execute} version_cutoff_nanos={cutoff}",
        store_root.display()
    );
    let _ = std::io::stderr().flush();
    let store = laststore::LastStore::open(&store_root).map_err(|error| error.to_string())?;
    let open = |body: &[u8]| fold_db::crypto::open_at_rest(key.as_bytes(), body).ok();
    let seal = |plain: &[u8]| fold_db::crypto::seal_at_rest_raw(key.as_bytes(), plain).ok();
    let report = store
        .maintenance_drop_versions_with(cutoff, execute, &open, &seal)
        .map_err(|error| error.to_string())?;
    print_version_retention_report(&report, execute, json)?;
    drop(lock_file);
    drop(key);
    Ok(())
}
