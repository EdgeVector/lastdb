use super::{budget, files, invalid, map, read_budget, rows::Rows, Proof, StorageResult};
use crate::atom::Atom;
use crate::db_operations::atom_store::decode_plain_atom;
use crate::schema::types::field::build_storage_key;
use crate::storage::laststore::native_routing;

pub(super) fn verify(proof: &Proof<'_>) -> StorageResult<u64> {
    let mut rows = Rows::new(proof.main, "atoms");
    let mut count = 0u64;
    while let Some((coordinate, body)) = rows.next()? {
        let decoded_coordinate = native_routing::decode(&coordinate)?;
        let decoded_coordinate = std::str::from_utf8(&decoded_coordinate)
            .map_err(|_| invalid("run backup atom coordinate encoding"))?;
        // This collection also holds blob counters. Validate an atom body's
        // or counter's identity, then let the current-key law below validate
        // and classify every coordinate before non-body records are skipped.
        if let Some(identity) = crate::atom::atom_key_codec::uuid_of(decoded_coordinate) {
            cloud_backup_protocol::Digest::try_from(identity.to_owned())
                .map_err(|_| invalid("run backup atom identity format"))?;
        }
        let Some((scope, id)) = files::atom_coordinate(&coordinate)? else {
            continue;
        };
        let atom = open(&body)?;
        drop(body);
        if atom.uuid() != id {
            return Err(invalid("run backup atom identity differs"));
        }
        let dependencies = files::expected(&atom)?;
        drop(atom);
        directories(proof, scope.as_deref(), &id, &dependencies)?;
        for dependency in dependencies {
            super::native::verify(proof, &dependency)?;
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("run backup atom count overflow"))?;
    }
    Ok(count)
}

pub(super) fn open(stored: &[u8]) -> StorageResult<Atom> {
    if stored.len() as u64 > budget::ATOM_BODY_MAX_BYTES {
        return Err(invalid("run backup atom exceeds decode allowance"));
    }
    // Drop the allocation-charged tree before the existing strict plain decoder
    // constructs the Atom. Disk V2 never admits a sealed V1 atom row.
    drop(files::decode_bounded_json(stored)?);
    decode_plain_atom(stored).map_err(|_| invalid("run backup plain atom format"))
}

pub(super) fn directories(
    proof: &Proof<'_>,
    scope: Option<&str>,
    atom_id: &str,
    dependencies: &[files::FileDependency],
) -> StorageResult<()> {
    if dependencies.len() > 3 {
        return Err(invalid("run backup file dependency selector count"));
    }
    let kinds = [
        files::FileDependencyKind::Main,
        files::FileDependencyKind::Thumbnail,
        files::FileDependencyKind::Poster,
    ];
    let coordinates = kinds.map(|kind| {
        let raw = build_storage_key(scope, &files::directory_key(atom_id, kind));
        (
            native_routing::collection_for_key(raw.as_bytes()).to_owned(),
            native_routing::encode(raw.as_bytes()),
        )
    });
    let rows = {
        let mut read = proof
            .main
            .fork_bounded(read_budget(budget::DIRECTORY_READ_BYTES, 3))
            .map_err(map)?;
        read.get_many_bounded(&coordinates, files::DIRECTORY_MAX_BYTES * 2)
            .map_err(map)?
    };
    if rows.len() != kinds.len() {
        return Err(invalid("run backup directory batch shape differs"));
    }
    for (row, kind) in rows.into_iter().zip(kinds) {
        let dependency = dependencies
            .iter()
            .find(|dependency| dependency.kind() == kind);
        match (row.map_err(map)?, dependency) {
            (None, None) => {}
            (Some(row), Some(dependency)) => {
                files::decode_directory(&row, dependency)?;
            }
            _ => return Err(invalid("run backup file dependency directory differs")),
        }
    }
    Ok(())
}
