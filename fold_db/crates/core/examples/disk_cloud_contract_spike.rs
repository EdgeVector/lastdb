//! Disk-to-cloud contract spike.
//!
//! CI creates a synthetic format 12 home, pins the frozen run capture, and
//! derives a cloud plan from the disk export spec. Placement changes the plan.
//! It does not change the pinned bytes. This command does not upload, and it
//! does not open a real LastDB home.

use std::sync::Arc;

use fold_db::db_operations::DbOperations;
use fold_db::storage::cloud_contract::{
    derive_cloud_format, plan, CloudEnvelope, Destination, ExportRole, PlacementPolicy,
    BACKUP_PROFILE, CLOUD_PROTOCOL,
};
use fold_db::storage::disk_pack::{pin_disk_export, PackNamespacedStore, CAS_COLLECTION};
use fold_db::storage::traits::NamespacedStore;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "disk_cloud_contract_spike/checks.rs"]
mod checks;

const NODE_KEY: [u8; 32] = [0x11; 32];

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("disk_cloud_contract_spike runs only in CI (GITHUB_ACTIONS is not set)");
        std::process::exit(2);
    }
    run().await
}

async fn run() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let node = dir.path().join("node");
    let hold = dir.path().join("hold");
    let store = PackNamespacedStore::create_run(&node, &NODE_KEY)?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    write_generated(&shared).await?;
    let pin = tokio::task::block_in_place(|| pin_disk_export(&store, &hold))?;
    let policy_r2 = records_on_r2();
    let policy_b2 = records_on_b2();
    let format_r2 = derive_cloud_format(envelope(BACKUP_PROFILE), policy_r2)?;
    let format_b2 = derive_cloud_format(envelope(BACKUP_PROFILE), policy_b2)?;
    let profile_one = derive_cloud_format(envelope(1), policy_r2)?;
    let selected = [ExportRole::Snapshot, ExportRole::RecordOwnedBlob];
    let plan_r2 = plan(pin.descriptors(), &format_r2, &selected)?;
    let plan_b2 = plan(pin.descriptors(), &format_b2, &selected)?;
    checks::check_derived(&format_r2, &profile_one)?;
    checks::check_capture(&plan_r2)?;
    checks::check_placement(&plan_r2, &plan_b2)?;
    checks::check_bytes(&pin, &plan_r2, &plan_b2)?;
    checks::check_refusals(pin.descriptors(), &format_r2)?;
    checks::check_snapshot_only(pin.descriptors(), &format_r2)?;
    println!("PASS: pinned frozen run capture");
    println!("PASS: placement change keeps source bytes");
    println!("PASS: replay gap refused");
    println!("PASS: unsupported disk format refused");
    println!("PASS: unclassified part refused");
    println!("PASS: blob-only closure refused");
    println!("PASS: backup profile does not describe the disk format");
    println!("{}", checks::summary(&plan_r2, &plan_b2));
    Ok(())
}

fn envelope(backup_profile: u16) -> CloudEnvelope {
    CloudEnvelope {
        protocol: CLOUD_PROTOCOL,
        backup_profile,
    }
}

fn records_on_r2() -> PlacementPolicy {
    PlacementPolicy {
        snapshot: Destination::R2,
        replay_log: Destination::R2,
        record_owned_blob: Destination::B2,
    }
}

fn records_on_b2() -> PlacementPolicy {
    PlacementPolicy {
        snapshot: Destination::B2,
        replay_log: Destination::B2,
        record_owned_blob: Destination::R2,
    }
}

async fn write_generated(store: &Arc<dyn NamespacedStore>) -> Result<()> {
    let records = store.open_namespace("spike-records").await?;
    records
        .batch_put(vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
            (b"d".to_vec(), b"four".to_vec()),
        ])
        .await?;
    let blobs = store.open_namespace(CAS_COLLECTION).await?;
    blobs
        .batch_put(vec![
            (b"blob-1".to_vec(), b"blob-bytes-1".to_vec()),
            (b"blob-2".to_vec(), b"blob-bytes-2".to_vec()),
        ])
        .await?;
    store.restore_durability_barrier().await?;
    Ok(())
}
