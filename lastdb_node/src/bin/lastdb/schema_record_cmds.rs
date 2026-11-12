use super::*;

fn percent_encode_query(value: &str) -> String {
    value
        .bytes()
        .fold(String::with_capacity(value.len()), |mut out, b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                out.push(b as char);
            } else {
                out.push_str(&format!("%{b:02X}"));
            }
            out
        })
}

fn require_reachable(socket: &Path) -> Result<(), String> {
    probe_health(socket).map_err(|_| {
        format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        )
    })
}

pub(crate) fn list_record_keys(
    target: &DatabaseTarget,
    args: &crate::cli_defs::RecordListArgs,
) -> Result<(), String> {
    require_reachable(&target.socket)?;
    let mut path = format!(
        "/api/list?schema={}&limit={}",
        percent_encode_query(&args.schema),
        args.limit.max(1)
    );
    for (param, opt) in [
        ("key_name", args.key_name.as_deref()),
        ("hash", args.key_hash.as_deref()),
        ("cursor", args.cursor.as_deref()),
    ] {
        if let Some(val) = opt {
            path.push_str(&format!("&{param}={}", percent_encode_query(val)));
        }
    }
    let response = get_path(
        &target.socket,
        &target.path(&path),
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let report = parse_json_response(&response, "list")?
        .get("list")
        .cloned()
        .ok_or("response missing list field")?;
    if let Some(name) = args.key_name.as_deref() {
        if report["key_name"].as_str() != Some(name) {
            return Err("node did not honor the requested alternate key; upgrade LastDB".into());
        }
        let keys = report["keys"].as_array();
        if keys.is_none() || keys.map(Vec::len) != report["primary_keys"].as_array().map(Vec::len) {
            return Err("alternate list lost its primary mutation keys".into());
        }
    }
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
    } else {
        print_list_record_keys_human(&report);
    }
    Ok(())
}

fn print_list_record_keys_human(report: &serde_json::Value) {
    let empty = Vec::new();
    let keys = report["keys"].as_array().unwrap_or(&empty);
    let has_more = report["has_more"].as_bool().unwrap_or(false);
    let schema = report["schema"].as_str().unwrap_or("?");
    let ranged = report
        .pointer("/key_definition/kind")
        .and_then(serde_json::Value::as_str)
        == Some("hash_range");
    let hash_filter = report["hash_filter"].as_str();
    let partition = hash_filter.map_or_else(String::new, |h| format!(" --key-hash {h:?}"));
    let key_flag = report["key_name"]
        .as_str()
        .map_or_else(String::new, |n| format!(" --key-name {n}"));
    eprintln!(
        "lastdb get-keys {schema}{key_flag}{partition} — {} key{}{}{}",
        keys.len(),
        if keys.len() == 1 { "" } else { "s" },
        if ranged { " hash+range" } else { " hash" },
        if has_more { " (more)" } else { "" }
    );
    if !key_flag.is_empty() {
        eprintln!("For writes, use primary_keys from --json.");
    }
    eprintln!("{}", if ranged { "hash\trange" } else { "hash" });
    for key in keys {
        let hash = key["hash"].as_str().unwrap_or("");
        let range = key["range"].as_str().unwrap_or("");
        if range.is_empty() {
            println!("{hash}");
        } else {
            println!("{hash}\t{range}");
        }
    }
    if let Some(cursor) = report["next_cursor"].as_str().filter(|_| has_more) {
        let hash_flag = hash_filter.map_or_else(String::new, |h| format!(" --key-hash '{h}'"));
        eprintln!("next: lastdb get-keys {schema}{key_flag}{hash_flag} --cursor '{cursor}'");
    }
}

pub(crate) fn fetch_schema(
    target: &DatabaseTarget,
    name: &str,
) -> Result<serde_json::Value, String> {
    require_reachable(&target.socket)?;
    let path = format!("/api/schema/{}", percent_encode_query(name));
    let response = get_path(&target.socket, &target.path(&path), Duration::from_secs(10))?;
    parse_json_response(&response, "schema")?
        .get("schema")
        .cloned()
        .ok_or_else(|| "response missing schema field".into())
}
