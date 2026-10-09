//! Exclusive-open writer for cloud-sync e2e: put durable LastStore rows under
//! frame-AEAD packaging so sealed chunks exist for `lastdb cloud snapshot`.
//!
//! Usage: csync_e2e_write <home> [row_count]

use fold_db::crypto::E2eKeys;
use fold_db::storage::laststore::LastStoreNamespacedStore;
use fold_db::storage::traits::NamespacedStore;
use laststore::LastStoreOptions;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let home = PathBuf::from(std::env::args().nth(1).expect("home arg"));
    let n: usize = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "32".into())
        .parse()
        .unwrap_or(32);

    let seed_bytes = std::fs::read(home.join("identity.key")).expect("identity.key");
    assert_eq!(seed_bytes.len(), 32);
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let e2e = E2eKeys::from_ed25519_seed(&seed).expect("e2e");
    let data_key = e2e.encryption_key();

    let data = home.join("data");
    let hw = home.join("laststore_high_water.json");
    // Cloud backup enumerates sealed encrypted chunks; frame AEAD + data_key
    // is required for seal_open during snapshot/cut.
    let store = LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
        &data,
        LastStoreOptions::hash_group_frame_aead(data_key),
        hw,
    )
    .expect("open LastStore hash_group frame_aead high-water");

    let atoms = store
        .open_namespace("main")
        .await
        .expect("open_namespace main");
    let tips = store
        .open_namespace("tips")
        .await
        .expect("open_namespace tips");

    let ts = fold_db::clock::unix_secs();

    for i in 0..n {
        let atom_key = format!("atom:csync-e2e-{i:04}");
        let tip_key = format!("mk:csync-e2e:{i:04}\0");
        let val = format!("payload-{i}-ts-{ts}");
        atoms
            .put(atom_key.as_bytes(), val.clone().into_bytes())
            .await
            .unwrap_or_else(|e| panic!("put atom {i}: {e}"));
        tips.put(tip_key.as_bytes(), val.into_bytes())
            .await
            .unwrap_or_else(|e| panic!("put tip {i}: {e}"));
    }
    atoms.flush().await.expect("flush atoms/main");
    tips.flush().await.expect("flush tips");

    let manifest = store
        .cut_backup_manifest(None)
        .expect("cut_backup_manifest");
    println!(
        "e2e_write_ok home={} rows={n} cut_csn={} counter={} mutable_chunks={} atom_chunks={}",
        home.display(),
        manifest.cut_csn,
        manifest.counter,
        manifest.mutable_chunks.len(),
        manifest.atom_chunks.len()
    );
    assert!(
        !manifest.mutable_chunks.is_empty() || !manifest.atom_chunks.is_empty(),
        "expected sealed chunks after writes+flush; cut_csn={} counter={}",
        manifest.cut_csn,
        manifest.counter
    );
}
