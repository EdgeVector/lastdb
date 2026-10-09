//! At-rest-encryption bulk-scan overhead benchmarks (Gap G1 slice d).
//!
//! The G1 flip turned a set of metadata namespaces — `schemas`,
//! `schema_states`, `lineage_forward`, `lineage_reverse`, and the rest of
//! [`DEFAULT_ENCRYPT_FLIPPED_NAMESPACES`] — from plaintext-at-rest to
//! AES-256-GCM-at-rest. Every row those namespaces return now incurs an AES-GCM
//! *open* on the read path, including on the **bulk-scan** paths that touch many
//! rows at once:
//!
//! - **schema-load-at-boot**: `DbOperations::schemas().get_all_schemas()` scans
//!   the whole (now-encrypted) `schemas` namespace and deserializes every entry.
//!   A node loads its full schema set this way at startup.
//! - **lineage bulk traversal**: the `lineage_forward` / `lineage_reverse`
//!   namespaces are walked with `KvStore::scan_prefix`, which decrypts every row
//!   it returns (`EncryptingKvStore::scan_prefix` opens each value). Those
//!   namespaces have no production scan API of their own yet, so the
//!   representative cost is measured at the layer it actually lands on: a
//!   `scan_prefix` over an encrypted vs. a plaintext namespace KvStore.
//!
//! # What these benches establish
//!
//! Each group runs the SAME workload twice — once over an **encrypted** store
//! and once over a **plaintext** control on the same Sled backend and the same
//! corpus — so the delta between the two ids *is* the per-row AES-256-GCM
//! decrypt overhead on a bulk scan. The PR description reports that delta; the
//! committed baseline (`benches/baseline/baseline.json`) then guards the
//! *encrypted* ids so a future change that makes the decrypt path materially
//! worse (e.g. a per-row key re-derivation, a redundant clone, a fallback to a
//! slower cipher) trips the regression gate.
//!
//! Run:               `cargo bench -p fold_db --bench encrypt_scan_bench`
//! Run one group:     `cargo bench -p fold_db --bench encrypt_scan_bench -- schema_load_at_boot`
//!
//! The decrypt cost is attributable to the storage seam only, so — like the
//! other storage-core benches — this runs with `FOLD_DISABLE_NATIVE_INDEX=1`
//! (no fastembed / ONNX init).

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fold_db::crypto::LocalCryptoProvider;
use fold_db::db_operations::DbOperations;
use fold_db::schema::Schema;
use fold_db::storage::{EncryptingNamespacedStore, LastStoreNamespacedStore, NamespacedStore};
use serde_json::json;
use tokio::runtime::Runtime;

/// A fresh leaked-temp Sled pool (the dir outlives the bench, like the other
/// storage benches' `create_temp_sled_pool`).
fn temp_dir() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("tempdir")
}

/// A bare (plaintext) Sled namespaced store — the control.
fn plaintext_store(dir: &tempfile::TempDir) -> Arc<dyn NamespacedStore> {
    Arc::new(LastStoreNamespacedStore::open(dir.path()).unwrap()) as Arc<dyn NamespacedStore>
}

/// The same Sled backend wrapped in the production at-rest encryption seam.
/// `migration_mode = false` so reads take the strict (always-decrypt) path,
/// matching a node that has completed the legacy-plaintext sweep — the steady
/// state this card measures.
fn encrypted_store(dir: &tempfile::TempDir) -> Arc<dyn NamespacedStore> {
    let base =
        Arc::new(LastStoreNamespacedStore::open(dir.path()).unwrap()) as Arc<dyn NamespacedStore>;
    let crypto = Arc::new(LocalCryptoProvider::from_key([0x42u8; 32]));
    Arc::new(EncryptingNamespacedStore::new(base, crypto)) as Arc<dyn NamespacedStore>
}

/// Build a representative test schema (mirrors the schema_store test fixture).
fn bench_schema(name: &str) -> Schema {
    let v = json!({
        "name": name,
        "schema_type": "Single",
        "fields": ["pk", "title", "body"],
        "field_data_classifications": {
            "pk": { "sensitivity_level": 0, "data_domain": "general" },
            "title": { "sensitivity_level": 0, "data_domain": "general" },
            "body": { "sensitivity_level": 0, "data_domain": "general" }
        }
    });
    serde_json::from_value(v).expect("bench schema must deserialize")
}

/// Schema-load-at-boot: `get_all_schemas()` over the (encrypted) `schemas`
/// namespace vs. a plaintext control on the same corpus. The "encrypted" id's
/// extra time over "plaintext" is the per-row decrypt cost on the boot scan.
///
/// Corpus sizes span a small-but-realistic schema set up to a stress count;
/// schema sets are normally tens, not thousands, so even the high end stays a
/// fast bench.
fn bench_schema_load_at_boot(c: &mut Criterion) {
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    async fn seeded_db(store: Arc<dyn NamespacedStore>, n: usize) -> Arc<DbOperations> {
        let db = Arc::new(
            DbOperations::from_namespaced_store(store)
                .await
                .expect("db_ops from store"),
        );
        for i in 0..n {
            let name = format!("Schema{i}");
            db.schemas()
                .store_schema(&name, &bench_schema(&name))
                .await
                .expect("store schema");
        }
        db
    }

    let mut group = c.benchmark_group("schema_load_at_boot");
    group.sample_size(20);
    for n in [16_usize, 256, 1_000] {
        group.throughput(Throughput::Elements(n as u64));

        let plain = rt.block_on(seeded_db(plaintext_store(&temp_dir()), n));
        group.bench_with_input(BenchmarkId::new("plaintext", n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = plain.clone();
                async move {
                    let schemas = db.schemas().get_all_schemas().await.expect("load schemas");
                    black_box(schemas);
                }
            });
        });

        let enc = rt.block_on(seeded_db(encrypted_store(&temp_dir()), n));
        group.bench_with_input(BenchmarkId::new("encrypted", n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = enc.clone();
                async move {
                    let schemas = db.schemas().get_all_schemas().await.expect("load schemas");
                    black_box(schemas);
                }
            });
        });
    }
    group.finish();
}

/// Lineage-style bulk scan: a `scan_prefix` over a whole namespace, encrypted
/// vs. plaintext. This is the exact code path the `lineage_forward` /
/// `lineage_reverse` traversal lands on — `EncryptingKvStore::scan_prefix`
/// AES-opens every value it returns — isolated from the schema-deserialize work
/// so the delta is purely the per-row decrypt. Each row carries a ~256-byte
/// value so the AES-GCM open operates on a realistic payload, not a trivial one.
fn bench_namespace_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    // ~256-byte body so each row's open is over a meaningful payload.
    let body: Vec<u8> = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
        .to_vec();

    async fn seeded_namespace(
        store: Arc<dyn NamespacedStore>,
        n: usize,
        body: &[u8],
    ) -> Arc<dyn fold_db::storage::traits::KvStore> {
        // `lineage_forward` is one of the flipped-encrypted namespaces, so the
        // encrypting store wraps it; the plaintext control opens the same name
        // on a bare backend.
        let kv = store
            .open_namespace("lineage_forward")
            .await
            .expect("open namespace");
        for i in 0..n {
            let key = format!("derived-{i:08}");
            kv.put(key.as_bytes(), body.to_vec())
                .await
                .expect("seed row");
        }
        kv.flush().await.expect("flush");
        kv
    }

    let mut group = c.benchmark_group("encrypted_namespace_scan");
    group.sample_size(20);
    for n in [1_000_usize, 10_000, 50_000] {
        group.throughput(Throughput::Elements(n as u64));

        let plain = rt.block_on(seeded_namespace(plaintext_store(&temp_dir()), n, &body));
        group.bench_with_input(BenchmarkId::new("plaintext", n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let kv = plain.clone();
                async move {
                    let rows = kv.scan_prefix(b"derived-").await.expect("scan");
                    black_box(rows);
                }
            });
        });

        let enc = rt.block_on(seeded_namespace(encrypted_store(&temp_dir()), n, &body));
        group.bench_with_input(BenchmarkId::new("encrypted", n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let kv = enc.clone();
                async move {
                    let rows = kv.scan_prefix(b"derived-").await.expect("scan");
                    black_box(rows);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_schema_load_at_boot, bench_namespace_scan);
criterion_main!(benches);
