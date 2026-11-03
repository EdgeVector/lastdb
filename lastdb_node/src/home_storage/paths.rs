use super::*;

pub(super) fn classify_path(relative: &Path) -> HomeStorageBucketKind {
    let first = relative.components().find_map(|component| match component {
        Component::Normal(value) => Some(value.as_bytes()),
        _ => None,
    });
    match first {
        Some(b"data") => HomeStorageBucketKind::DatabaseStore,
        Some(b"backup" | b"backups" | b"backup-cut-freeze") => HomeStorageBucketKind::Backup,
        Some(b"recovery" | b"restore") => HomeStorageBucketKind::Recovery,
        Some(
            b"current"
            | b"native-run-v2-programs"
            | b"bin"
            | b"bin-with-upload-cap"
            | b"lastdb"
            | b"lastdbd"
            | b"launchd"
            | b"watchdog.sh",
        ) => HomeStorageBucketKind::RuntimeBinary,
        Some(name) if name == b"apps" || name.starts_with(b"admin-") => {
            HomeStorageBucketKind::RuntimeApp
        }
        Some(b"log" | b"logs" | b"crash-reports" | b"current-session.json" | b"sessions.jsonl") => {
            HomeStorageBucketKind::Log
        }
        Some(name) if name.starts_with(b"observability.") => HomeStorageBucketKind::Log,
        Some(b"candidate" | b"candidates" | b".candidates" | b".lastdb-staged") => {
            HomeStorageBucketKind::Candidate
        }
        Some(name) if name.starts_with(b"cloud_sync.") => HomeStorageBucketKind::DatabaseAuxiliary,
        None
        | Some(
            b".cloud_resume_required"
            | b".fastembed_cache"
            | b".metadata_never_index"
            | b"cloud-byte-backup-v2-manual.json"
            | b"identity.key"
            | b"autostart-enrolled"
            | b"config.json"
            | b"folddb.sock"
            | b"install_id"
            | b"metering-webhook-secret-dev"
            | b"monitoring"
            | b"schema_resolver.json",
        ) => HomeStorageBucketKind::DatabaseAuxiliary,
        Some(_) => HomeStorageBucketKind::UnknownPath,
    }
}

pub(super) fn display_root(relative: &Path) -> String {
    relative
        .components()
        .find_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            _ => None,
        })
        .unwrap_or_else(|| ".".to_string())
}

pub(super) fn safe_relative(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::CurDir | Component::Normal(_)))
}

pub(super) fn encode_path(path: &Path) -> String {
    fold_db::hex::hex_lower(path.as_os_str().as_bytes())
}

pub(super) fn decode_path(encoded: &str) -> Result<PathBuf, ()> {
    if !encoded.len().is_multiple_of(2) || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(());
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|_| ())?;
            u8::from_str_radix(pair, 16).map_err(|_| ())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}
