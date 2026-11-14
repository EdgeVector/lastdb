//! Record lookup commands (`lastdb get` and `lastdb get-keys`).

use super::*;
use lastdb_node::lastdb_uds::{admin_handler_timeout, DEFAULT_CLI_UDS_TIMEOUT_SECS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

fn percent_encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
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
        ("key_name", &args.key_name),
        ("hash", &args.key_hash),
        ("cursor", &args.cursor),
    ] {
        if let Some(v) = opt {
            path.push_str(&format!("&{p}={}", percent_encode_query(v)));
        }
    }
    let timeout = Duration::from_secs(DEFAULT_CLI_UDS_TIMEOUT_SECS);
    let raw = get_path(&target.socket, &target.path(&path), timeout)?;
    let r = parse_json_response(&raw, "list")?
        .get("list")
        .cloned()
        .ok_or("response missing list field")?;
    if let Some(name) = args.key_name.as_deref() {
        if r["key_name"].as_str() != Some(name) {
            return Err("node did not honor the requested alternate key; upgrade LastDB".into());
        }
        let keys = r["keys"].as_array();
        if keys.is_none() || keys.map(Vec::len) != r["primary_keys"].as_array().map(Vec::len) {
            return Err("alternate list lost its primary mutation keys".into());
        }
    }
    if args.json {
        print_pretty_json(&r);
    } else {
        print_list_record_keys_human(&r);
    }
    Ok(())
}

fn print_list_record_keys_human(r: &Value) {
    let empty = Vec::new();
    let keys = r["keys"].as_array().unwrap_or(&empty);
    let (more, schema) = (
        r["has_more"].as_bool().unwrap_or(false),
        r["schema"].as_str().unwrap_or("?"),
    );
    let ranged = r.pointer("/key_definition/kind").and_then(Value::as_str) == Some("hash_range");
    let hf = r["hash_filter"].as_str();
    let part = hf.map_or_else(String::new, |h| format!(" --key-hash {h:?}"));
    let kf = r["key_name"]
        .as_str()
        .map_or_else(String::new, |n| format!(" --key-name {n}"));
    let (n, sfx) = (keys.len(), if keys.len() == 1 { "" } else { "s" });
    let (kind, m_str) = (
        if ranged { " hash+range" } else { " hash" },
        if more { " (more)" } else { "" },
    );
    eprintln!("lastdb get-keys {schema}{kf}{part} — {n} key{sfx}{kind}{m_str}");
    if !kf.is_empty() {
        eprintln!("For writes, use primary_keys from --json.");
    }
    eprintln!("{}", if ranged { "hash\trange" } else { "hash" });
    for k in keys {
        let (h, rg) = (
            k["hash"].as_str().unwrap_or(""),
            k["range"].as_str().unwrap_or(""),
        );
        if rg.is_empty() {
            println!("{h}");
        } else {
            println!("{h}\t{rg}");
        }
    }
    if let Some(cur) = r["next_cursor"].as_str().filter(|_| more) {
        let h_flag = hf.map_or_else(String::new, |h| format!(" --key-hash '{h}'"));
        eprintln!("next: lastdb get-keys {schema}{kf}{h_flag} --cursor '{cur}'");
    }
}

pub(crate) fn get_query_body(schema: &str, key_hash: &str, key_range: Option<&str>) -> Value {
    let filter = if let Some(range) = key_range.filter(|s| !s.is_empty()) {
        json!({ "HashRangeKey": { "hash": key_hash, "range": range } })
    } else {
        json!({ "HashKey": key_hash })
    };
    json!({ "schema_name": schema, "filter": filter })
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
    let route = format!("/api/schema/{}", percent_encode_query(schema_name));
    let s_raw = get_path(
        &target.socket,
        &target.path(&route),
        Duration::from_secs(10),
    )?;
    let schema = parse_json_response(&s_raw, "schema")?
        .get("schema")
        .cloned()
        .ok_or("response missing schema field")?;
    let has = |p| {
        schema
            .pointer(p)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
    };
    if (schema["schema_type"].as_str() == Some("HashRange")
        || (has("/key/range_field") && has("/key/hash_field")))
        && key_range.is_none_or(|s| s.trim().is_empty())
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let body = get_query_body(schema_name, hash, key_range);
    let timeout = Duration::from_secs(DEFAULT_CLI_UDS_TIMEOUT_SECS);
    let mut val = parse_json_response(
        &post_json_with_timeout(&target.socket, &target.path("/api/query"), &body, timeout)?,
        "query",
    )?;
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

fn print_get_human(value: &Value) {
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
            if let Some(p) = row.get(hyd).and_then(Value::as_str) {
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
    value: &mut Value,
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
        return match (inline, file_required) {
            (true, _) => Err("record has no inline file".into()),
            (_, true) => Err("record is missing its required file".into()),
            _ => Ok(()),
        };
    };
    let size = m["size_bytes"].as_u64().ok_or("file marker has no size")?;
    let digest = m["sha256"].as_str().ok_or("file marker has no digest")?;
    let rev = rec["revision"].as_str().ok_or("record has no revision")?;
    let req_json =
        json!({ "schema": schema, "key": rec["key"], "expected_revision": rev, "placement": name });
    let req = build_post_request(
        &target.path("/api/db/fetch-blob"),
        "Content-Type: application/json\r\nAccept: application/octet-stream\r\n",
        &serde_json::to_vec(&req_json).map_err(|e| format!("encode hydrate request: {e}"))?,
    );
    let resp = request_with_timeout_bytes(&target.socket, &req, admin_handler_timeout())?;
    let bytes = split_http_200(&resp, "record hydration")?;
    if bytes.len() as u64 != size || hex::encode(Sha256::digest(bytes)) != digest {
        return Err("hydrated file differs from the queried record version".into());
    }
    let tmp = out.with_file_name(format!(".lastdb-hydrate-{}", Uuid::new_v4()));
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
    rec[field] = json!(out);
    Ok(())
}
