//! Use the existing exact product verifier before an inactive target publishes.

use super::{invalid, StorageResult};
use crate::storage::disk_pack::{
    physical, restore_safety, space, CoreNativeBackupBoundary, PackNamespacedStore,
};
use laststore::disk_run::home::RunHome;
use schema_types::key_comparison::KeyOrderVersion;
use std::sync::Arc;

pub(super) async fn validate(homes: &[Arc<RunHome>; 2]) -> StorageResult<()> {
    // This private runtime uses already-owned writers. It never admits a
    // node, starts a daemon, resumes a conversion, or removes its fence.
    let home = physical::V2Home::new(
        Arc::clone(&homes[0]),
        Arc::clone(&homes[1]),
        KeyOrderVersion::TypedScalarV1,
    );
    let root = home.current_root();
    let before = [
        homes[0].current_root().frontier(),
        homes[1].current_root().frontier(),
    ];
    let boundary = tokio::sync::watch::channel(CoreNativeBackupBoundary {
        source_home: home.home.identity(),
        frontier: root.frontier(),
    })
    .0;
    let store =
        PackNamespacedStore::assemble(home, root, boundary, false, space::policy_from_env()?);
    restore_safety::exclusive(
        Arc::clone(&store.runtime),
        crate::db_operations::atom_store::counter::restore_validation::validate(&store),
    )
    .await
    .map_err(|_| {
        invalid(
            "target atoms, counters, retained records, molecules, or files fail exact validation",
        )
    })?;
    if before
        != [
            homes[0].current_root().frontier(),
            homes[1].current_root().frontier(),
        ]
    {
        return Err(invalid("reference validation changed the target"));
    }
    Ok(())
}
