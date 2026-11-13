//! Database target resolution, `db list`, and `lastdb mutate`.

use super::*;
use lastdb_node::lastdb_uds::database_path::{self, DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use serde_json::json;
use std::borrow::Cow;

const PUT_BLOB_PATH: &str = "/api/db/put-blob";

pub(crate) struct DatabaseTarget {
    pub(crate) socket: PathBuf,
    selector: Option<String>,
}

impl DatabaseTarget {
    pub(crate) fn open(data_dir: Option<PathBuf>, selector: Option<&str>) -> Result<Self, String> {
        let (_, socket) = resolve_client_home_and_socket(data_dir)?;
        let selector = selector.map(|v| resolve_database(&socket, v)).transpose()?;
        Ok(Self { socket, selector })
    }

    pub(crate) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        let Some(sel) = self.selector.as_deref() else {
            return Cow::Borrowed(path);
        };
        let suffix = path
            .strip_prefix("/api")
            .expect("database path starts with /api");
        Cow::Owned(format!("{DATABASE_SCOPE_PREFIX}{sel}{suffix}"))
    }

    pub(crate) fn selector(&self) -> Option<&str> {
        self.selector.as_deref()
    }
}

fn resolve_database(socket: &Path, selector: &str) -> Result<String, String> {
    if !database_path::valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" || cloud_backup_protocol::Id::try_from(selector.to_owned()).is_ok() {
        return Ok(selector.to_owned());
    }
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")?["databases"]
        .as_array()
        .ok_or("missing database list")?
        .iter()
        .find_map(|db| {
            (db["name"].as_str() == Some(selector)).then_some(())?;
            let id = db["id"].as_str()?;
            cloud_backup_protocol::Id::try_from(id.to_owned()).ok()?;
            Some(id.to_owned())
        })
        .ok_or_else(|| "database is not registered on this node".into())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json_only: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let resp = parse_json_response(
        &get_path(&target.socket, DATABASES_PATH, Duration::from_secs(10))?,
        "database list",
    )?;
    if json_only {
        println!("{resp}");
        return Ok(());
    }
    for db in resp["databases"]
        .as_array()
        .ok_or("missing database list")?
    {
        let id = db["id"].as_str().ok_or("database has no ID")?;
        let name = db["name"].as_str().unwrap_or(id);
        let acc = db["access"].as_str().ok_or("database has no access mode")?;
        let st = db["state"].as_str().ok_or("database has no state")?;
        println!("{name}\t{id}\t{acc}\t{st}");
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: Value,
    file: Option<InputFile>,
    #[serde(default)]
    placement: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
    #[serde(default)]
    local: bool,
    thumbnail: Option<ThumbFile>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThumbFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
}

fn submit_input_file(
    target: &DatabaseTarget,
    base: &Path,
    body: &Value,
    file: &InputFile,
    placement: &str,
) -> Result<(), String> {
    use base64::Engine as _;
    if placement == "inline" && file.thumbnail.is_some() {
        return Err("an inline file input takes no thumbnail".into());
    }
    if file.local && placement == "inline" {
        return Err("a local file reference cannot be an inline file".into());
    }
    let source = base.join(&file.source);
    let thumb = if let Some(t) = &file.thumbnail {
        let p = base.join(&t.source);
        let bytes = std::fs::read(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
        json!({ "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes), "name": t.name, "media_type": t.media_type })
    } else {
        Value::Null
    };
    let mut meta = json!({ "mutation": body, "name": file.name, "media_type": file.media_type, "placement": placement, "thumbnail": thumb });
    let receipt = if file.local {
        meta["local_path"] = json!(std::fs::canonicalize(&source)
            .map_err(|e| format!("open {}: {e}", source.display()))?);
        parse_json_response(
            &post_json_admin(&target.socket, &target.path(PUT_BLOB_PATH), &meta)?,
            PUT_BLOB_PATH,
        )?
    } else {
        put_record_file(target, &meta, &source)?
    };
    println!("{receipt}");
    Ok(())
}

pub(super) fn mutate_command(
    target: &DatabaseTarget,
    request_file: &Path,
    durable: bool,
    mutation_id: Option<Uuid>,
) -> Result<(), String> {
    if mutation_id.is_some() && target.selector.is_none() {
        return Err("--mutation-id requires --db".into());
    }
    let text = if request_file == Path::new("-") {
        let mut s = String::new();
        std::io::stdin()
            .lock()
            .read_to_string(&mut s)
            .map_err(|e| format!("read mutation: {e}"))?;
        s
    } else {
        std::fs::read_to_string(request_file).map_err(|e| format!("read mutation: {e}"))?
    };
    let mut body: Value =
        serde_json::from_str(&text).map_err(|e| format!("invalid mutation JSON: {e}"))?;
    let file = if body.get("mutation").is_some() {
        if body.get("file").is_none() {
            return Err("record input requires an explicit file object or null".into());
        }
        let input: RecordInput =
            serde_json::from_value(body).map_err(|e| format!("invalid record input: {e}"))?;
        let place = match input.placement.as_deref() {
            None | Some("blob") => "blob",
            Some("inline") => "inline",
            Some(o) => {
                return Err(format!(
                    "record input placement must be blob or inline, not {o}"
                ));
            }
        };
        body = input.mutation;
        Some((input.file, place))
    } else {
        None
    };
    if file.is_some() && mutation_id.is_some() {
        return Err("--mutation-id applies to record submissions without file changes".into());
    }
    if let Some(obj) = body.as_object_mut().filter(|_| durable) {
        obj.insert("durability".into(), json!("durable"));
    }
    lastdb_node::exec::parse_mutation_body(&serde_json::to_vec(&body).map_err(|e| e.to_string())?)
        .map_err(|r| format!("invalid complete mutation: {r:?}"))?;
    let (route, body) = match file {
        Some((Some(f), p)) => {
            return submit_input_file(
                target,
                request_file.parent().unwrap_or(Path::new("")),
                &body,
                &f,
                p,
            );
        }
        Some((None, p)) => (
            "/api/db/delete-blob",
            json!({ "mutation": body, "placement": p }),
        ),
        None => ("/api/mutation", body),
    };
    let id = (target.selector.is_some() && route == "/api/mutation")
        .then(|| mutation_id.unwrap_or_else(Uuid::new_v4));
    let with_id = |e: String| {
        if let Some(id) = id {
            format!("{e} (mutation ID {id})")
        } else {
            e
        }
    };
    let timeout = if durable {
        lastdb_node::exec::handler_timeout().saturating_add(Duration::from_secs(20))
    } else {
        Duration::from_secs(10)
    };
    let raw = post_json_with_id(&target.socket, &target.path(route), &body, timeout, id)
        .map_err(with_id)?;
    let (_, resp_body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| with_id("mutation response had no body".into()))?;
    if raw.starts_with("HTTP/1.1 202 ") {
        let r: Value = serde_json::from_str(resp_body)
            .map_err(|_| with_id("invalid mutation submission receipt".into()))?;
        if r["status"] != "submitted" || r["mutation_id"].as_str().is_none() {
            return Err(with_id("invalid mutation submission receipt".into()));
        }
    } else {
        parse_json_response(&raw, "mutation").map_err(with_id)?;
    }
    println!("{}", resp_body.trim_end_matches('\n'));
    Ok(())
}

fn put_record_file(
    target: &DatabaseTarget,
    metadata: &Value,
    source: &Path,
) -> Result<Value, String> {
    let max = fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
    let admit = |n: u64| -> Result<(), String> {
        if n <= max {
            Ok(())
        } else {
            Err("local file input exceeds native pending memory allowance".into())
        }
    };
    let f = std::fs::File::open(source).map_err(|e| format!("open {}: {e}", source.display()))?;
    let len = f
        .metadata()
        .map_err(|e| format!("file metadata: {e}"))?
        .len();
    admit(len)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(len).map_err(|_| "local file input length overflow")?)
        .map_err(|_| "local file input allocation refused")?;
    f.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| format!("read local file input: {e}"))?;
    admit(bytes.len() as u64)?;
    let json = serde_json::to_vec(metadata).map_err(|e| format!("serialize file metadata: {e}"))?;
    let body_len = json
        .len()
        .checked_add(bytes.len())
        .ok_or("local file request length overflow")?;
    let (route, hdr, jlen) = (target.path(PUT_BLOB_PATH), client_headers(), json.len());
    let head = format!("POST {route} HTTP/1.1\r\nHost: localhost\r\n{hdr}Content-Type: application/octet-stream\r\nX-LastDB-File-Blob-Metadata-Length: {jlen}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n");
    let cap = head
        .len()
        .checked_add(body_len)
        .ok_or("local file request length overflow")?;
    admit(cap as u64)?;
    let mut req = Vec::new();
    req.try_reserve_exact(cap)
        .map_err(|_| "local file request allocation refused")?;
    req.extend_from_slice(head.as_bytes());
    req.extend_from_slice(&json);
    req.extend_from_slice(&bytes);
    drop(bytes);
    parse_json_response(
        &request_with_timeout(
            &target.socket,
            &req,
            lastdb_node::exec::admin_handler_timeout(),
        )?,
        PUT_BLOB_PATH,
    )
}
