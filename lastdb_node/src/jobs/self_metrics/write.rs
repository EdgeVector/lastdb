use super::write_fields::*;
use super::*;
use fold_db::schema::types::MutationIds;

pub(super) async fn write_snapshot(host: &Host, snapshot: &StatusSnapshot) -> Result<(), String> {
    let sample_id = sample_id(snapshot.sampled_at);
    let mut fields = HashMap::new();
    fields.insert("series".to_string(), json!(SELF_METRIC_SERIES));
    fields.insert("sample_id".to_string(), json!(sample_id));
    insert_process_and_memory_fields(&mut fields, snapshot);
    insert_allocator_and_disk_fields(&mut fields, snapshot);
    insert_backup_durability_fields(&mut fields, snapshot);
    insert_sampler_qos_uds_fields(&mut fields, snapshot);

    write_mutation(
        host,
        SELF_METRIC_SCHEMA,
        fields,
        KeyValue::new(Some(SELF_METRIC_SERIES.to_string()), Some(sample_id)),
        MutationType::Create,
    )
    .await
}

/// Persist a rollup counter only when it carries a measurement.
///
/// Absence and zero are the same value to every reader of this schema, and
/// they are not the same cost. `merge_request_ops_rollup_rows` reads every
/// numeric field on a rollup row as `field_u64(..).unwrap_or(0)` and then
/// merges it with either `saturating_add` or `max` — and 0 is the identity
/// element for both. So a stored 0 and a missing field produce a
/// bit-identical aggregate, which is what
/// `sparse_and_dense_rollup_rows_merge_identically` pins.
///
/// The cost is not identical. Every zero written here is a stored field value
/// of the record, kept until retention drops the row. For phase sums
/// that is most of the row: a request reports the phases it went through, and
/// the ~11-18 it did not are not zeros, they are *unmeasured*.
///
/// `OpAggregate` already applies exactly this rule to its JSON wire form
/// (`skip_serializing_if = "is_zero_u64"`); this extends it to the durable
/// write, which is the copy that is kept.
pub(super) fn insert_if_nonzero(
    fields: &mut HashMap<String, serde_json::Value>,
    name: &str,
    value: u64,
) {
    if value != 0 {
        fields.insert(name.to_string(), json!(value));
    }
}

pub(super) async fn write_request_ops_rollup(
    host: &Host,
    snapshot: &StatusSnapshot,
) -> Result<(), String> {
    let aggregates = host
        .self_metrics
        .request_ops_rollup_delta(&snapshot.request_ops);
    for (index, aggregate) in aggregates.iter().enumerate() {
        let sampled_at_ms = fold_db::clock::unix_millis();
        let nanos = fold_db::clock::unix_nanos() % 1_000_000_000;
        let sample_id = format!(
            "{sampled_at_ms:020}-{nanos:09}-{index:02}-{}",
            std::process::id()
        );
        let mut fields = HashMap::new();
        // IDENTITY — always written. These are the row's key, its ordering,
        // and the two fields `merge_request_ops_rollup_rows` requires before
        // it will consider a row at all (`client`, `kind`); omitting any of
        // them drops the row, not a zero.
        fields.insert("series".to_string(), json!(REQUEST_OPS_ROLLUP_SERIES));
        fields.insert("sample_id".to_string(), json!(sample_id));
        fields.insert("sampled_at_ms".to_string(), json!(sampled_at_ms));
        fields.insert(
            "process_start_ts".to_string(),
            json!(snapshot.process_start_ts),
        );
        fields.insert("client".to_string(), json!(&aggregate.client));
        fields.insert("kind".to_string(), json!(aggregate.kind.as_str()));
        // A schema-less op (idle wait, `status`) previously persisted a JSON
        // `null` here — a stored field value to say "no schema". The reader
        // maps absent, null and "" onto the same `None`.
        if let Some(schema) = aggregate.schema.as_deref().filter(|s| !s.is_empty()) {
            fields.insert("schema".to_string(), json!(schema));
        }
        // Same absent-means-absent convention as `schema` above: only a row
        // whose identity IS its route carries one.
        if let Some(route) = aggregate.route.as_deref().filter(|r| !r.is_empty()) {
            fields.insert("route".to_string(), json!(route));
        }
        // COUNTERS — written only when non-zero. See `insert_if_nonzero`.
        insert_if_nonzero(&mut fields, "count", aggregate.count);
        insert_if_nonzero(&mut fields, "sum_ms", aggregate.sum_ms);
        insert_if_nonzero(&mut fields, "max_ms", aggregate.max_ms);
        insert_if_nonzero(&mut fields, "last_ts_ms", aggregate.last_ts_ms);
        insert_if_nonzero(&mut fields, "error_count", aggregate.error_count);
        insert_if_nonzero(&mut fields, "sum_body_bytes", aggregate.sum_body_bytes);
        insert_if_nonzero(&mut fields, "max_body_bytes", aggregate.max_body_bytes);
        // Field names derive from the model's PHASE_NAMES so a phase added
        // there is persisted here without a second edit (the schema list
        // above is pinned to PHASE_NAMES by test). This is where omission
        // pays most: a request reports a handful of the 24 phases, and the
        // rest were never measured — persisting them as 0 stores a
        // measurement that was never taken, at one field value each.
        for (name, value) in aggregate.phase_sums.named_us() {
            insert_if_nonzero(&mut fields, &format!("sum_{name}_us"), value);
        }
        insert_if_nonzero(&mut fields, "phase_count", aggregate.phase_count);
        insert_if_nonzero(&mut fields, "phased_sum_ms", aggregate.phased_sum_ms);
        insert_if_nonzero(
            &mut fields,
            "sum_field_keys_persisted",
            aggregate.sum_field_keys_persisted,
        );
        insert_if_nonzero(
            &mut fields,
            "sum_field_key_store_commits",
            aggregate.sum_field_key_store_commits,
        );
        insert_if_nonzero(
            &mut fields,
            "sum_resident_commits",
            aggregate.sum_resident_commits,
        );
        insert_if_nonzero(
            &mut fields,
            "sum_resident_operations",
            aggregate.sum_resident_operations,
        );
        write_mutation(
            host,
            REQUEST_OPS_ROLLUP_SCHEMA,
            fields,
            KeyValue::new(Some(REQUEST_OPS_ROLLUP_SERIES.to_string()), Some(sample_id)),
            MutationType::Create,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn write_mutation(
    host: &Host,
    schema_name: &str,
    fields: HashMap<String, Value>,
    key_value: KeyValue,
    mutation_type: MutationType,
) -> Result<(), String> {
    let mutation = Mutation::new(
        schema_name.to_string(),
        fields,
        key_value,
        host.public_key(),
        mutation_type,
    );
    host.db
        .mutation_manager()
        .write_mutations_with_access(vec![mutation], &owner_context(host))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

pub(super) fn owner_context(host: &Host) -> AccessContext {
    AccessContext::owner(host.user_hash.clone()).with_transport(CallerTransport::InProcess)
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
