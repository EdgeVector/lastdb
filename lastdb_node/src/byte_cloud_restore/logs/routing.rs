//! Bounded routing facts after complete authenticated checkpoint download.
use super::{local, plan, MutationLogRoot, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::mutation_log::CommitRole;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Routing {
    version: u16,
    commit_sha256: Digest,
    native_files: u64,
    native_bytes: u64,
    main_r2_bytes: u64,
    cas_add_b2_bytes: u64,
    cas_delete_b2_bytes: u64,
    metadata_r2: bool,
}

/// Coverage checked every metadata and native receipt destination; download
/// authenticated every original file. Retain only counts before cache release.
pub(super) fn save(session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
    let mut routing = Routing {
        version: 1,
        commit_sha256: session.accepted().commit_sha256.clone(),
        native_files: root.native_file_count,
        native_bytes: root.native_bytes,
        main_r2_bytes: 0,
        cas_add_b2_bytes: 0,
        cas_delete_b2_bytes: 0,
        metadata_r2: true,
    };
    for part in root.entries.iter().flat_map(|entry| &entry.parts) {
        let bytes = match part.role {
            CommitRole::Main => &mut routing.main_r2_bytes,
            CommitRole::CasAdd => &mut routing.cas_add_b2_bytes,
            CommitRole::CasDelete => &mut routing.cas_delete_b2_bytes,
        };
        *bytes = bytes
            .checked_add(part.bytes)
            .ok_or(RestoreError::Limit("mutation routing bytes"))?;
    }
    let path = session.root.join(format!(
        "chain/routing-{}.json",
        routing.commit_sha256.as_str()
    ));
    if path.try_exists()? {
        let saved: Routing = local::read_json(&path, 4096)?;
        if saved != routing {
            return Err(RestoreError::Invalid("saved mutation routing identity"));
        }
        return Ok(());
    }
    plan::save(session, &path, &routing)
}
