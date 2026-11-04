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
        // Every declared field on every record: counters and phase sums as
        // numbers, including 0, and `schema` / `route` as "" when the op has
        // none. The reader requires all of them.
        let mut fields: HashMap<String, Value> = HashMap::new();
        fields.insert("series".to_string(), json!(REQUEST_OPS_ROLLUP_SERIES));
        fields.insert("sample_id".to_string(), json!(sample_id));
        fields.insert("sampled_at_ms".to_string(), json!(sampled_at_ms));
        fields.insert(
            "process_start_ts".to_string(),
            json!(snapshot.process_start_ts),
        );
        fields.insert("client".to_string(), json!(&aggregate.client));
        fields.insert("kind".to_string(), json!(aggregate.kind.as_str()));
        fields.insert(
            "schema".to_string(),
            json!(aggregate.schema.as_deref().unwrap_or("")),
        );
        fields.insert(
            "route".to_string(),
            json!(aggregate.route.as_deref().unwrap_or("")),
        );
        for (name, value) in [
            ("count", aggregate.count),
            ("sum_ms", aggregate.sum_ms),
            ("max_ms", aggregate.max_ms),
            ("last_ts_ms", aggregate.last_ts_ms),
            ("error_count", aggregate.error_count),
            ("sum_body_bytes", aggregate.sum_body_bytes),
            ("max_body_bytes", aggregate.max_body_bytes),
            ("phase_count", aggregate.phase_count),
            ("phased_sum_ms", aggregate.phased_sum_ms),
            ("sum_resident_commits", aggregate.sum_resident_commits),
            ("sum_resident_operations", aggregate.sum_resident_operations),
        ] {
            fields.insert(name.to_string(), json!(value));
        }
        // Field names derive from the model's PHASE_NAMES.
        // `REQUEST_OPS_ROLLUP_FIELDS` must declare the same names.
        for (name, value) in aggregate.phase_sums.named_us() {
            fields.insert(format!("sum_{name}_us"), json!(value));
        }
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
        uuid::Uuid::new_v4().to_string(),
        fold_db::record_replacement::RecordTarget {
            schema: schema_name.to_string(),
            key: key_value,
            alternates: Default::default(),
        },
        fields,
        host.public_key(),
        mutation_type,
        fold_db::record_replacement::RecordExpectation::Absent,
    );
    host.db
        .mutation_manager()
        .write_mutations(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
