use crate::benchmark_support::BenchmarkSchemaBuilder;
use crate::crypto::E2eKeys;
use crate::fold_db_core::factory::{create_fold_db_with_options, FoldDbOptions};
use crate::fold_db_core::FoldDB;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::security::Ed25519KeyPair;
use crate::storage::config::DatabaseConfig;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub(super) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const SCHEMA: &str = "LifecycleDrill";

pub(super) fn require(ok: bool, reason: &str) -> Result<()> {
    if !ok {
        return Err(std::io::Error::other(reason).into());
    }
    Ok(())
}

pub(super) fn keys() -> E2eKeys {
    // Public synthetic bytes protect generated CI data only.
    E2eKeys::from_secret(&Sha256::digest(b"PR369 generated lifecycle drill").into())
}

pub(super) fn signer() -> Result<Arc<Ed25519KeyPair>> {
    Ok(Arc::new(
        Ed25519KeyPair::generate().map_err(std::io::Error::other)?,
    ))
}

pub(super) async fn open(path: &Path, initialize: bool, retained: bool) -> Result<Arc<FoldDB>> {
    let db = create_fold_db_with_options(
        &DatabaseConfig::local(path.to_path_buf()),
        &keys(),
        signer()?,
        FoldDbOptions {
            offline_maintenance: true,
            search_outbox_inbox: Some(path.join("inbox")),
            ..Default::default()
        },
    )
    .await?;
    if initialize {
        db.load_schema_from_json(
            &BenchmarkSchemaBuilder::new(SCHEMA)
                .fields(&["id", "payload"])
                .hash_key("id")
                .field_type("payload", json!("Any"))
                .build_json(),
        )
        .await?;
        if retained {
            db.db_ops()
                .schemas()
                .set_schema_retention_policy(
                    SCHEMA,
                    crate::schema::SchemaRetentionPolicy::operator(30, Vec::new()),
                )
                .await?;
        }
    }
    Ok(db)
}

pub(super) fn mutation(key: &str, value: Value, kind: MutationType) -> Mutation {
    let fields = if matches!(kind, MutationType::Create | MutationType::Update) {
        HashMap::from([("id".into(), json!(key)), ("payload".into(), value)])
    } else {
        HashMap::new()
    };
    let mut mutation = Mutation::new(
        SCHEMA.into(),
        fields,
        KeyValue::new(Some(key.into()), None),
        "generated-ci".into(),
        kind,
    );
    mutation.synchronous = Some(true);
    mutation
}

pub(super) fn sign(mut mutation: Mutation, seconds: u64, cutoff: Option<u64>) -> Result<Mutation> {
    let writer = signer()?;
    mutation.imported_written_at = Some(seconds * 1_000_000_000);
    mutation.author_clock_writer_id = writer.public_key_base64();
    mutation.author_clock_signature_version =
        crate::schema::types::MUTATION_AUTHOR_SIGNATURE_VERSION;
    if let Some(cutoff) = cutoff {
        mutation = mutation.with_erase_tips_before_nanos(cutoff * 1_000_000_000);
    }
    mutation.author_clock_signature =
        crate::schema::types::author_clock::sign_mutation_author_clock(&mutation, writer.as_ref());
    Ok(mutation)
}

pub(super) async fn replay(db: &FoldDB, mutations: Vec<Mutation>, device: &str) -> Result<()> {
    db.mutation_manager()
        .apply_replayed_mutations(mutations, None, device)
        .await?;
    db.flush().await?;
    Ok(())
}

pub(super) async fn put(db: &FoldDB, mutations: Vec<Mutation>) -> Result<Vec<String>> {
    Ok(db
        .mutation_manager()
        .write_mutations_batch_async(mutations, None)
        .await?)
}

pub(super) async fn values(db: &FoldDB, keys: &[String]) -> Result<Vec<Option<Value>>> {
    let schema = db
        .schema_manager()
        .get_schema_metadata(SCHEMA)?
        .ok_or("schema missing")?;
    let field = &schema.runtime_fields["payload"];
    let molecule = field.common().molecule_uuid().ok_or("molecule missing")?;
    // Match the normal query executor: a cold process must load this molecule's
    // stored key bundle before it encodes API keys for exact disk reads.
    db.db_ops()
        .atoms()
        .load_molecule_key_bundle(molecule)
        .await?;
    let codec = db.db_ops().atoms().key_codec_for_molecule(molecule)?;
    let bases = keys
        .iter()
        .map(|key| {
            let (hash, range) = field
                .slot_coordinates(&KeyValue::new(Some(key.clone()), None))
                .ok_or_else(|| std::io::Error::other("slot missing"))?;
            Ok(codec.api_hash_range_record_key(molecule, &hash, &range)?)
        })
        .collect::<Result<Vec<_>>>()?;
    let tips = db
        .db_ops()
        .atoms()
        .load_visible_per_key_records(None, &bases)
        .await?;
    let ids: Vec<&str> = tips
        .iter()
        .flatten()
        .map(|tip| tip.entry.atom_uuid.as_str())
        .collect();
    let mut bodies = db.db_ops().atoms().get_atoms(&ids, None).await?.into_iter();
    Ok(tips
        .iter()
        .map(|tip| {
            tip.as_ref()
                .and_then(|_| bodies.next().flatten().map(|atom| atom.content().clone()))
        })
        .collect())
}

pub(super) async fn expired(db: &FoldDB, key: &str, cutoff: u64) -> Result<bool> {
    Ok(db
        .db_ops()
        .schemas()
        .expired_schema_retention_keys(SCHEMA, cutoff)
        .await?
        .contains(&KeyValue::new(Some(key.into()), None)))
}

pub(super) fn tombstones(path: &Path) -> Result<usize> {
    let inbox = path.join("inbox");
    if !inbox.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for (index, entry) in std::fs::read_dir(inbox)?.enumerate() {
        require(index < 1000, "inbox exceeded drill bound")?;
        let entry = entry?;
        if entry.path().extension().is_some_and(|ext| ext == "json") {
            let batch: crate::db_operations::search_index::IndexChangeBatch =
                serde_json::from_slice(&std::fs::read(entry.path())?)?;
            count += batch
                .changes
                .iter()
                .filter(|change| {
                    change.kind == crate::db_operations::search_index::IndexChangeKind::Tombstone
                })
                .count();
        }
    }
    Ok(count)
}

pub(super) async fn verify(db: &FoldDB) -> Result<Value> {
    let mut request = crate::db_operations::VerifyRequest {
        max_pages: Some(128),
        pause_ms: Some(0),
        page_rows: Some(128),
        ..Default::default()
    };
    for _ in 0..64 {
        let report = db.verify_refs_step(request.clone()).await?;
        if report.done {
            require(
                report.tally_complete
                    && report.mismatches_total == 0
                    && report.mismatches.unconfirmed_count_mismatch == 0,
                "reference verifier found a fault",
            )?;
            return Ok(
                json!({"faults": report.mismatches_total, "tips": report.tips_read,
                "records": report.records_read, "counters": report.counters_read}),
            );
        }
        request.cursor = report.next_cursor;
    }
    Err("reference verifier exceeded drill bound".into())
}

pub(super) async fn compact(db: &FoldDB) -> Result<Value> {
    let runtime = db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or("native runtime missing")?;
    let report = runtime
        .maintain_native(crate::storage::disk_pack::NativeMaintenanceOptions {
            dry_run: false,
            reclaim_only: false,
            full_copy: true,
            grace_ms: Some(0),
            report_usage: true,
            reclaim_generations: None,
        })
        .await?;
    require(
        report.executed && !report.reclaim_deferred && report.reclaim_error.is_none(),
        "native compaction did not complete",
    )?;
    Ok(serde_json::to_value(report)?)
}
