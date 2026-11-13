use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::OpenOptionsExt;

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

pub(super) fn list_record_keys(
    target: &DatabaseTarget,
    args: &RecordListArgs,
) -> Result<(), String> {
    require_reachable(&target.socket)?;
    let mut path = format!(
        "/api/list?schema={}&limit={}",
        percent_encode_query(&args.schema),
        args.limit.max(1)
    );
    for (p, opt) in [
        ("key_name", args.key_name.as_deref()),
        ("hash", args.key_hash.as_deref()),
        ("cursor", args.cursor.as_deref()),
    ] {
        if let Some(v) = opt {
            path.push_str(&format!("&{p}={}", percent_encode_query(v)));
        }
    }
    let raw = get_path(
        &target.socket,
        &target.path(&path),
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let report = parse_json_response(&raw, "list")?
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
        print_pretty_json(&report);
    } else {
        print_list_record_keys_human(&report);
    }
    Ok(())
}

fn print_list_record_keys_human(report: &serde_json::Value) {
    let empty = Vec::new();
    let keys = report["keys"].as_array().unwrap_or(&empty);
    let (has_more, schema) = (
        report["has_more"].as_bool().unwrap_or(false),
        report["schema"].as_str().unwrap_or("?"),
    );
    let ranged = report
        .pointer("/key_definition/kind")
        .and_then(serde_json::Value::as_str)
        == Some("hash_range");
    let hf = report["hash_filter"].as_str();
    let part = hf.map_or_else(String::new, |h| format!(" --key-hash {h:?}"));
    let kf = report["key_name"]
        .as_str()
        .map_or_else(String::new, |n| format!(" --key-name {n}"));
    eprintln!(
        "lastdb get-keys {schema}{kf}{part} — {} key{}{}{}",
        keys.len(),
        if keys.len() == 1 { "" } else { "s" },
        if ranged { " hash+range" } else { " hash" },
        if has_more { " (more)" } else { "" }
    );
    if !kf.is_empty() {
        eprintln!("For writes, use primary_keys from --json.");
    }
    eprintln!("{}", if ranged { "hash\trange" } else { "hash" });
    for k in keys {
        let (h, r) = (
            k["hash"].as_str().unwrap_or(""),
            k["range"].as_str().unwrap_or(""),
        );
        if r.is_empty() {
            println!("{h}");
        } else {
            println!("{h}\t{r}");
        }
    }
    if let Some(cur) = report["next_cursor"].as_str().filter(|_| has_more) {
        let h_flag = hf.map_or_else(String::new, |h| format!(" --key-hash '{h}'"));
        eprintln!("next: lastdb get-keys {schema}{kf}{h_flag} --cursor '{cur}'");
    }
}

pub(crate) fn get_query_body(
    schema: &str,
    key_hash: &str,
    key_range: Option<&str>,
) -> serde_json::Value {
    let filter = if let Some(range) = key_range.filter(|s| !s.is_empty()) {
        serde_json::json!({ "HashRangeKey": { "hash": key_hash, "range": range } })
    } else {
        serde_json::json!({ "HashKey": key_hash })
    };
    serde_json::json!({ "schema_name": schema, "filter": filter })
}

pub(super) fn get_record(
    target: &DatabaseTarget,
    schema_name: &str,
    (key_hash, key_range): (Option<&str>, Option<&str>),
    json_only: bool,
    (hydrate, hydrate_inline): (Option<&Path>, Option<&Path>),
) -> Result<(), String> {
    let Some(hash) = key_hash.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err("get needs --key-hash (and --key-range when the schema is HashRange)".into());
    };
    require_reachable(&target.socket)?;
    let schema_route = format!("/api/schema/{}", percent_encode_query(schema_name));
    let s_raw = get_path(
        &target.socket,
        &target.path(&schema_route),
        Duration::from_secs(10),
    )?;
    let schema = parse_json_response(&s_raw, "schema")?
        .get("schema")
        .cloned()
        .ok_or("response missing schema field")?;
    let has = |p| {
        schema
            .pointer(p)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.is_empty())
    };
    if (schema["schema_type"].as_str() == Some("HashRange")
        || (has("/key/range_field") && has("/key/hash_field")))
        && key_range.map(str::trim).filter(|s| !s.is_empty()).is_none()
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let resp = post_json_with_timeout(
        &target.socket,
        &target.path("/api/query"),
        &get_query_body(schema_name, hash, key_range),
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let mut val = parse_json_response(&resp, "query")?;
    if let Some(out) = hydrate {
        hydrate_record_file(
            target,
            schema_name,
            &mut val,
            out,
            schema["file_required"].as_bool().unwrap_or(false),
            false,
        )?;
    }
    if let Some(out) = hydrate_inline {
        hydrate_record_file(target, schema_name, &mut val, out, false, true)?;
    }
    if json_only {
        print_pretty_json(&val);
    } else {
        print_get_human(&val);
    }
    Ok(())
}

fn print_get_human(value: &serde_json::Value) {
    let Some(results) = value["results"].as_array().filter(|r| !r.is_empty()) else {
        eprintln!("lastdb get: no row");
        return;
    };
    for (i, row) in results.iter().enumerate() {
        if results.len() > 1 {
            eprintln!("--- row {} ---", i + 1);
        }
        if let Some(s) = row["key"].as_str() {
            println!("key {s}");
        } else if row["key"].is_object() {
            let (h, r) = (
                row["key"]["hash"].as_str().unwrap_or(""),
                row["key"]["range"].as_str().unwrap_or(""),
            );
            if r.is_empty() {
                println!("key {h}");
            } else {
                println!("key {h}\t{r}");
            }
        }
        if let Some(fields) = row["fields"].as_object() {
            let mut names: Vec<_> = fields.keys().collect();
            names.sort();
            for n in names {
                let v = &fields[n];
                println!(
                    "{n} {}",
                    v.as_str().map_or_else(|| v.to_string(), str::to_owned)
                );
            }
        }
        for (hyd, ptr, pfx) in [
            ("hydrated_file", "/metadata/file", "file"),
            (
                "hydrated_inline_file",
                "/metadata/inline_file",
                "inline_file",
            ),
        ] {
            if let Some(p) = row.get(hyd).and_then(serde_json::Value::as_str) {
                println!("{pfx} {p}");
            } else if let Some(f) = row.pointer(ptr) {
                println!("{pfx} {f}");
            }
        }
    }
}

fn hydrate_record_file(
    target: &DatabaseTarget,
    schema: &str,
    value: &mut serde_json::Value,
    out: &Path,
    file_required: bool,
    inline: bool,
) -> Result<(), String> {
    let (ptr, name, field) = if inline {
        ("/metadata/inline_file", "inline", "hydrated_inline_file")
    } else {
        ("/metadata/file", "blob", "hydrated_file")
    };
    let [rec] = value["results"]
        .as_array_mut()
        .ok_or("missing query records")?
        .as_mut_slice()
    else {
        return Err("hydration requires exactly one record".into());
    };
    let Some(m) = rec.pointer(ptr).filter(|v| !v.is_null()) else {
        return if inline {
            Err("record has no inline file".into())
        } else if file_required {
            Err("record is missing its required file".into())
        } else {
            Ok(())
        };
    };
    let size = m["size_bytes"].as_u64().ok_or("file marker has no size")?;
    let digest = m["sha256"].as_str().ok_or("file marker has no digest")?;
    let rev = rec["revision"].as_str().ok_or("record has no revision")?;
    let body = serde_json::to_vec(&serde_json::json!({
        "schema": schema, "key": rec["key"], "expected_revision": rev, "placement": name,
    }))
    .map_err(|e| format!("encode hydrate request: {e}"))?;
    let mut req = format!(
        "POST {} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nAccept: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        target.path("/api/db/fetch-blob"),
        client_headers(),
        body.len(),
    )
    .into_bytes();
    req.extend_from_slice(&body);
    let resp = request_with_timeout_bytes(
        &target.socket,
        &req,
        lastdb_node::exec::admin_handler_timeout(),
    )?;
    let bytes = split_http_200(&resp, "record hydration")?;
    if bytes.len() as u64 != size || hex::encode(Sha256::digest(bytes)) != digest {
        return Err("hydrated file differs from the queried record version".into());
    }
    let tmp = out.with_file_name(format!(".lastdb-hydrate-{}", uuid::Uuid::new_v4()));
    let mut f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|e| format!("create hydration output: {e}"))?;
    let res = f
        .write_all(bytes)
        .and_then(|()| std::fs::hard_link(&tmp, out));
    let clean = std::fs::remove_file(&tmp);
    res.map_err(|e| format!("publish hydration output: {e}"))?;
    clean.map_err(|e| format!("clean up hydration output: {e}"))?;
    rec[field] = serde_json::json!(out);
    Ok(())
}
