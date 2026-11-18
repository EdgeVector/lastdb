//! Route label for one completed request. Known routes are `'static`. An
//! unknown path is copied into a fixed buffer, truncated on a char boundary.

use super::{CALL_CLIP, OTHER};

#[derive(Clone, Copy)]
pub(crate) struct CallLabel {
    known: Option<&'static str>,
    buf: [u8; CALL_CLIP],
    len: u8,
}

impl CallLabel {
    pub(super) fn as_str(&self) -> &str {
        if let Some(path) = self.known {
            return path;
        }
        let end = usize::from(self.len);
        std::str::from_utf8(&self.buf[..end]).unwrap_or(OTHER)
    }

    /// The static route, when this label is not an unknown path.
    pub(super) fn known(&self) -> Option<&'static str> {
        self.known
    }

    pub(super) fn is_unknown(&self) -> bool {
        self.known.is_none()
    }

    fn from_static(path: &'static str) -> Self {
        Self {
            known: Some(path),
            buf: [0; CALL_CLIP],
            len: 0,
        }
    }

    fn from_path(path: &str) -> Self {
        let bytes = path.as_bytes();
        if bytes.len() > CALL_CLIP {
            return Self::from_static(OTHER);
        }
        let mut buf = [0u8; CALL_CLIP];
        buf[..bytes.len()].copy_from_slice(bytes);
        let Ok(len) = u8::try_from(bytes.len()) else {
            return Self::from_static(OTHER);
        };
        Self {
            known: None,
            buf,
            len,
        }
    }
}

pub(crate) fn call_of(target: &str) -> CallLabel {
    let path = target.split(['?', '#']).next().unwrap_or("");
    known_path(path).map_or_else(
        || CallLabel::from_path(clip(path, CALL_CLIP)),
        CallLabel::from_static,
    )
}

pub(super) fn clip(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = 0;
    for (index, ch) in value.char_indices() {
        let next = index + ch.len_utf8();
        if next > max {
            break;
        }
        end = next;
    }
    &value[..end]
}

fn known_path(path: &str) -> Option<&'static str> {
    exact(path).or_else(|| placeholder(path))
}

fn exact(path: &str) -> Option<&'static str> {
    Some(match path {
        "/health" => "/health",
        "/api/version" => "/api/version",
        "/api/admin/shed" => "/api/admin/shed",
        "/api/apps/declare-schema" => "/api/apps/declare-schema",
        "/api/apps/register" => "/api/apps/register",
        "/api/backup/snapshot" => "/api/backup/snapshot",
        "/api/cloud/wake" => "/api/cloud/wake",
        "/api/databases" => "/api/databases",
        "/api/databases/accept" => "/api/databases/accept",
        "/api/databases/spec" => "/api/databases/spec",
        "/api/db/cleanup" => "/api/db/cleanup",
        "/api/db/delete-ledger" => "/api/db/delete-ledger",
        "/api/db/flush" => "/api/db/flush",
        "/api/db/native-status" => "/api/db/native-status",
        "/api/db/schema-retention" => "/api/db/schema-retention",
        "/api/db/schemas" => "/api/db/schemas",
        "/api/ops" => "/api/ops",
        "/api/schemas" => "/api/schemas",
        "/api/schemas/drop" => "/api/schemas/drop",
        "/api/schemas/seed-system" => "/api/schemas/seed-system",
        "/api/status" => "/api/status",
        "/api/system/auto-identity" => "/api/system/auto-identity",
        "/api/system/boot-identity" => "/api/system/boot-identity",
        "/v1/read" => "/v1/read",
        "/v1/write" => "/v1/write",
        "/v1/x/changes" => "/v1/x/changes",
        "/v1/x/databases" => "/v1/x/databases",
        _ => return None,
    })
}

fn placeholder(path: &str) -> Option<&'static str> {
    if let Some(name) = path.strip_prefix("/api/schema/") {
        if !name.is_empty() && !name.contains('/') {
            return Some("/api/schema/{schema}");
        }
    }
    let rest = path.strip_prefix("/api/databases/")?;
    let (id, suffix) = rest.split_once('/').unwrap_or((rest, ""));
    if id.is_empty() || id.contains('/') {
        return None;
    }
    if suffix.is_empty() {
        return Some("/api/databases/{database}");
    }
    Some(match suffix {
        "submissions" => "/api/databases/{database}/submissions",
        "submissions/status" => "/api/databases/{database}/submissions/status",
        "schemas" => "/api/databases/{database}/schemas",
        "schemas/seed-system" => "/api/databases/{database}/schemas/seed-system",
        "schemas/drop" => "/api/databases/{database}/schemas/drop",
        "share/config" => "/api/databases/{database}/share/config",
        "share/members" => "/api/databases/{database}/share/members",
        "invitations" => "/api/databases/{database}/invitations",
        "apps/declare-schema" => "/api/databases/{database}/apps/declare-schema",
        _ => schema_suffix(suffix)?,
    })
}

fn schema_suffix(suffix: &str) -> Option<&'static str> {
    let name = suffix.strip_prefix("schema/")?;
    (!name.is_empty() && !name.contains('/')).then_some("/api/databases/{database}/schema/{schema}")
}
