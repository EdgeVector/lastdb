//! Pointer directory mutations join the body/counter/tip native batch.
use crate::atom::Atom;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::disk_pack::file_dependencies::{directory_key, expected, FileDependencyKind};
use crate::storage::KvMutation;

pub(super) fn create(scope: Option<&str>, atom: &Atom) -> Result<Vec<KvMutation>, SchemaError> {
    // An unsupported legacy pointer remains a valid local write. Its missing
    // directory certificate causes explicit backup refusal, never false coverage.
    let Ok(dependencies) = expected(atom) else {
        return Ok(Vec::new());
    };
    dependencies
        .into_iter()
        .map(|dependency| {
            let key = build_storage_key(scope, &directory_key(atom.uuid(), dependency.kind()));
            let value = serde_json::to_vec(&dependency.record).map_err(|error| {
                SchemaError::InvalidData(format!("file dependency encode: {error}"))
            })?;
            Ok(KvMutation::put(key.into_bytes(), value))
        })
        .collect()
}

pub(super) fn remove(scope: Option<&str>, atom: &str) -> Vec<KvMutation> {
    [
        FileDependencyKind::Main,
        FileDependencyKind::Thumbnail,
        FileDependencyKind::Poster,
    ]
    .into_iter()
    .map(|kind| {
        KvMutation::delete(build_storage_key(scope, &directory_key(atom, kind)).into_bytes())
    })
    .collect()
}
