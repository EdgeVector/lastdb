use super::support::*;
use schema_service_core::{ExternalSchemaPersistence, InMemorySchemaPersistence};
use schema_types::{compute_field_hash, FieldValueType};
use std::sync::Arc;

#[test]
fn generated_batches_survive_reload_with_stable_field_identity() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let batch: Vec<_> = (0..128)
            .map(|number| {
                (
                    format!("thermometer_sensor{number}"),
                    field(
                        &format!("Thermometer measurement from sensor number {number}"),
                        FieldValueType::Integer,
                    ),
                )
            })
            .collect();
        state
            .add_canonical_fields(&batch)
            .await
            .expect("valid batch");
        state
            .add_canonical_fields(&batch)
            .await
            .expect("idempotent batch");
        let reloaded = super::support::state(backend.clone()).await;
        let durable = backend
            .load_all_canonical_fields()
            .await
            .expect("durable fields");
        assert_eq!(durable.len(), batch.len());
        assert_eq!(*reloaded.canonical_fields.read().expect("cache"), durable);
        for (name, definition) in batch {
            assert_eq!(
                durable[&name].field_hash(&name),
                definition.field_hash(&name)
            );
        }
    });
}

#[test]
fn same_name_requires_exact_definition_type_and_version() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let original = field(
            "Parcel delivery status code from the carrier",
            FieldValueType::String,
        );
        state
            .add_canonical_field("parcel_delivery_status", original.clone())
            .await
            .expect("first");
        let mut changed = vec![original.clone(); 3];
        changed[0].description = "Parcel delivery status of customs clearance".into();
        changed[1].field_type = FieldValueType::Integer;
        changed[2].version = 2;
        for proposed in changed {
            assert!(state
                .add_canonical_field("parcel_delivery_status", proposed)
                .await
                .is_err());
        }
        assert_eq!(
            backend.load_all_canonical_fields().await.expect("fields")["parcel_delivery_status"],
            original
        );
    });
}

#[test]
fn invalid_batch_does_not_publish_a_valid_prefix_or_advance_version() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let before = state.current_state_version();
        let batch = vec![
            (
                "parcel_identifier".into(),
                field("Parcel identifier from the carrier", FieldValueType::String),
            ),
            (
                "status".into(),
                field("Parcel status from the carrier", FieldValueType::String),
            ),
        ];
        assert!(state.add_canonical_fields(&batch).await.is_err());
        assert!(backend
            .load_all_canonical_fields()
            .await
            .expect("fields")
            .is_empty());
        assert!(state.canonical_fields.read().expect("cache").is_empty());
        assert_eq!(state.current_state_version(), before);
    });
}

#[test]
fn normalized_duplicate_descriptions_are_rejected_inside_one_batch() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let batch = vec![
            (
                "parcel_delivery_status".into(),
                field(
                    "Parcel delivery status from carrier",
                    FieldValueType::String,
                ),
            ),
            (
                "parcel_delivery_state".into(),
                field(
                    "PARCEL, delivery status from carrier!",
                    FieldValueType::String,
                ),
            ),
        ];
        let error = state
            .add_canonical_fields(&batch)
            .await
            .expect_err("one meaning requires reuse");
        assert!(
            error.to_string().contains("canonical_field_reuse_required"),
            "{error}"
        );
        assert!(backend
            .load_all_canonical_fields()
            .await
            .expect("fields")
            .is_empty());
    });
}

#[test]
fn units_roles_and_types_remain_distinct() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let batch = vec![
            (
                "file_size_bytes".into(),
                field("File size measured in bytes", FieldValueType::Integer),
            ),
            (
                "file_size_bits".into(),
                field("File size measured in bits", FieldValueType::Integer),
            ),
            (
                "parcel_source_identifier".into(),
                field(
                    "Parcel source identifier assigned by carrier",
                    FieldValueType::String,
                ),
            ),
            (
                "parcel_target_identifier".into(),
                field(
                    "Parcel target identifier assigned by carrier",
                    FieldValueType::String,
                ),
            ),
            (
                "parcel_weight_decimal_text".into(),
                field(
                    "Parcel weight encoded as decimal text in kilograms",
                    FieldValueType::String,
                ),
            ),
            (
                "parcel_weight_integer".into(),
                field(
                    "Parcel weight encoded as integer count in kilograms",
                    FieldValueType::Integer,
                ),
            ),
        ];
        state
            .add_canonical_fields(&batch)
            .await
            .expect("distinct concepts");
        assert_eq!(
            backend
                .load_all_canonical_fields()
                .await
                .expect("fields")
                .len(),
            6
        );
    });
}

#[test]
fn field_hash_separates_name_meaning_type_and_version() {
    let reference = compute_field_hash(
        "parcel_status",
        "Parcel status code",
        &FieldValueType::String,
        1,
    );
    let variants = [
        compute_field_hash(
            "parcel_state",
            "Parcel status code",
            &FieldValueType::String,
            1,
        ),
        compute_field_hash(
            "parcel_status",
            "Parcel customs status",
            &FieldValueType::String,
            1,
        ),
        compute_field_hash(
            "parcel_status",
            "Parcel status code",
            &FieldValueType::Integer,
            1,
        ),
        compute_field_hash(
            "parcel_status",
            "Parcel status code",
            &FieldValueType::String,
            2,
        ),
    ];
    assert!(variants.iter().all(|variant| variant != &reference));
    assert_eq!(
        reference,
        compute_field_hash(
            " parcel_status ",
            " Parcel status code ",
            &FieldValueType::String,
            1
        )
    );
}
