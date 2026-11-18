//! Record lookup commands (`lastdb get` and `lastdb get-keys`).

use super::v1_records;
use super::*;
use lastdb_node::lastdb_uds::DEFAULT_CLI_UDS_TIMEOUT_SECS;
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

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
    let timeout = Duration::from_secs(DEFAULT_CLI_UDS_TIMEOUT_SECS);
    let schema = fetch_schema(target, &args.schema)?;
    let item = v1_records::key_list_item(args, v1_records::database(target));
    let page = v1_records::read_page(target, &item, timeout)?;
    let report = v1_records::key_report(args, &schema, &page)?;
    if args.json {
        print_pretty_json(&report);
    } else {
        print_list_record_keys_human(&report);
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
    let (n, sfx) = (keys.len(), if keys.len() == 1 { "" } else { "s" });
    let (kind, m_str) = (
        if ranged { " hash+range" } else { " hash" },
        if more { " (more)" } else { "" },
    );
    eprintln!("lastdb get-keys {schema}{part} — {n} key{sfx}{kind}{m_str}");
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
        eprintln!("next: lastdb get-keys {schema}{h_flag} --cursor '{cur}'");
    }
}

fn fetch_schema(target: &DatabaseTarget, schema_name: &str) -> Result<Value, String> {
    let route = format!("/api/schema/{}", percent_encode_query(schema_name));
    let raw = get_path(
        &target.socket,
        &target.path(&route),
        Duration::from_secs(10),
    )?;
    parse_json_response(&raw, "schema")?
        .get("schema")
        .cloned()
        .ok_or_else(|| "response missing schema field".into())
}

pub(super) fn get_record(
    target: &DatabaseTarget,
    schema_name: &str,
    (key_hash, key_range): (Option<&str>, Option<&str>),
    json_only: bool,
    hydrate: Option<&Path>,
) -> Result<(), String> {
    let Some(hash) = key_hash.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err("get needs --key-hash (and --key-range when the schema is HashRange)".into());
    };
    require_reachable(&target.socket)?;
    let schema = fetch_schema(target, schema_name)?;
    if schema["schema_type"].as_str() == Some("HashRange")
        && key_range.is_none_or(|s| s.trim().is_empty())
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let timeout = Duration::from_secs(DEFAULT_CLI_UDS_TIMEOUT_SECS);
    let mut item =
        v1_records::record_item(schema_name, hash, key_range, v1_records::database(target));
    if let Some(dir) = hydrate {
        let folder = dir.to_str().ok_or("hydrate folder is not a utf-8 path")?;
        let obj = item.as_object_mut().ok_or("read item is incomplete")?;
        obj.insert("file".into(), json!(true));
        obj.insert("file_dir".into(), json!(folder));
    }
    let page = v1_records::read_page(target, &item, timeout)?;
    let mut val = v1_records::query_view(&page)?;
    if hydrate.is_some() {
        note_hydrated_file(
            &page,
            &mut val,
            schema["file_required"].as_bool().unwrap_or(false),
        )?;
    }
    if json_only {
        print_pretty_json(&val);
    } else {
        print_get_human(&val);
    }
    Ok(())
}

fn note_hydrated_file(page: &Value, view: &mut Value, file_required: bool) -> Result<(), String> {
    let file = page.pointer("/records/0/file");
    let path = file.and_then(|marker| marker["path"].as_str());
    let Some(path) = path.filter(|path| !path.is_empty()) else {
        if file.is_some_and(|marker| !marker.is_null()) {
            return Err("record file has no path".into());
        }
        return if file_required {
            Err("record is missing its required file".into())
        } else {
            Ok(())
        };
    };
    let row = view
        .pointer_mut("/results/0")
        .ok_or("hydration requires exactly one record")?;
    row["hydrated_file"] = json!(path);
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
        if let Some(path) = row.get("hydrated_file").and_then(Value::as_str) {
            println!("file {path}");
        } else if let Some(file) = row.pointer("/metadata/file") {
            println!("file {file}");
        }
    }
}
