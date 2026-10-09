use super::*;

/// Read and sanitize the self-reported client label from request headers.
#[must_use]
pub fn client_from_request(req: &UdsRequest) -> String {
    let raw = req
        .header(CLIENT_HEADER)
        .or_else(|| req.header(CLIENT_HEADER_FALLBACK))
        .unwrap_or("unknown");
    sanitize_label(raw, MAX_CLIENT_LEN, "unknown")
}

/// Replace an unlabeled request with a schema-owner label when the route can
/// infer one from schema metadata. Explicit caller headers still win.
#[must_use]
pub fn client_or_schema_owner(client: String, schema_owner: Option<&str>) -> String {
    if client != "unknown" {
        return client;
    }
    match schema_owner {
        Some(owner) => sanitize_label(owner, MAX_CLIENT_LEN, "unknown"),
        None => client,
    }
}

/// Label prefix for a client attributed from UDS peer credentials rather than
/// a self-reported header. Keeps kernel-derived labels visually distinct from
/// header-declared ones in `lastdb ops`.
pub const PEER_CLIENT_PREFIX: &str = "peer:";

/// Last resort in the client ladder: name an unlabeled request by the process
/// on the other end of the socket, so `lastdb ops` never reports traffic it
/// cannot attribute.
///
/// Renders as `peer:<comm>` (for example `peer:curl`), falling back to
/// `peer:pid-<n>` when the process name cannot be read. `"unknown"` survives
/// only when the kernel gave us no pid at all.
///
/// # Why the process name and not the pid
///
/// The aggregate map keys on the client label and evicts by age past
/// [`MAX_AGGREGATE_KEYS`]. A short-lived poller (a `curl` in a shell loop)
/// gets a fresh pid every request, so a `pid:<n>` label would mint a new key
/// per call: the offender would split into hundreds of `count=1` rows that can
/// never rank in `top_by_count` / `top_by_total_ms`, while evicting the
/// genuinely long-lived clients. Keying on the process name keeps one row per
/// caller — which is what "name the worst offender" needs. The exact pid is
/// not lost: every sample still carries [`OpSample::peer_pid`], and
/// [`peer_detail`] prints it on the recent-ring lines.
///
/// A self-reported header still wins (see [`client_from_request`]). Peer
/// attribution does not make the label a security boundary — any local process
/// can still claim any header value; this only fills in the blanks.
#[must_use]
pub fn client_or_peer(client: String, peer_pid: Option<i32>, peer_comm: Option<&str>) -> String {
    if client != "unknown" {
        return client;
    }
    let Some(pid) = peer_pid else {
        return client;
    };
    let raw = match peer_comm {
        Some(comm) if !comm.trim().is_empty() => {
            format!("{PEER_CLIENT_PREFIX}{}", comm.trim())
        }
        _ => format!("{PEER_CLIENT_PREFIX}pid-{pid}"),
    };
    sanitize_label(&raw, MAX_CLIENT_LEN, "unknown")
}

/// Read and sanitize the optional caller-minted request correlation ID.
#[must_use]
pub fn request_id_from_request(req: &UdsRequest) -> Option<String> {
    let raw = req.header(REQUEST_ID_HEADER)?;
    let id = sanitize_label(raw, MAX_REQUEST_ID_LEN, "");
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Best-effort schema name from a JSON request body (`schema_name` or `schema`).
#[must_use]
pub fn schema_from_body(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let name = value
        .get("schema_name")
        .or_else(|| value.get("schema"))
        .and_then(|v| v.as_str())?;
    Some(sanitize_label(name, MAX_SCHEMA_LEN, "unknown"))
}

pub(super) const MAX_PATH_LEN: usize = 160;
pub(super) const MAX_PEER_COMM_LEN: usize = 64;

/// Route label for ops samples: `METHOD /path` without query/fragment.
///
/// Caps length and strips control characters so a hostile target cannot blow
/// up the ring or the `lastdb ops` table.
#[must_use]
pub fn path_from_request(method: &str, target: &str) -> String {
    let path = target.split(['?', '#']).next().unwrap_or(target);
    let method = method.trim();
    let path = if path.is_empty() { "/" } else { path };
    let raw = if method.is_empty() {
        path.to_string()
    } else {
        format!("{method} {path}")
    };
    sanitize_label(&raw, MAX_PATH_LEN, "-")
}

/// Best-effort short process name for a peer pid (ops attribution).
#[must_use]
pub fn peer_comm_from_pid(pid: i32) -> Option<String> {
    let name = fold_db::access::process_name(pid)?;
    let cleaned = sanitize_label(&name, MAX_PEER_COMM_LEN, "");
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

/// Compact `pid=<n>` / `pid=<n> comm=<name>` fragment for `lastdb ops` lines.
#[must_use]
pub fn peer_detail(peer_pid: Option<i32>, peer_comm: Option<&str>) -> String {
    match (peer_pid, peer_comm) {
        (Some(pid), Some(comm)) if !comm.is_empty() => format!(" pid={pid} comm={comm}"),
        (Some(pid), _) => format!(" pid={pid}"),
        (None, Some(comm)) if !comm.is_empty() => format!(" comm={comm}"),
        _ => String::new(),
    }
}

/// For mutation-batch bodies, report the first mutation's schema (if any).
#[must_use]
pub fn schema_from_batch_body(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let first = match &value {
        serde_json::Value::Array(items) => items.first(),
        other => other
            .get("mutations")
            .and_then(|m| m.as_array())
            .and_then(|a| a.first()),
    }?;
    let name = first
        .get("schema_name")
        .or_else(|| first.get("schema"))
        .and_then(|v| v.as_str())?;
    Some(sanitize_label(name, MAX_SCHEMA_LEN, "unknown"))
}

/// Pull `returned_count` / `results.len()` from a successful JSON response body.
#[must_use]
pub fn rows_from_response_body(status: u16, body: &[u8]) -> Option<u64> {
    if status != 200 || body.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    if let Some(n) = value
        .get("returned_count")
        .and_then(serde_json::Value::as_u64)
    {
        return Some(n);
    }
    if let Some(n) = value.get("total_count").and_then(serde_json::Value::as_u64) {
        return Some(n);
    }
    value
        .get("results")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as u64)
}
