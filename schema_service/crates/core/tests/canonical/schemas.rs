use super::support::*;
use schema_service_core::{
    types::SchemaAddOutcome, ExternalSchemaPersistence, InMemorySchemaPersistence,
};
use schema_types::{FieldValueType, KeyConfig};
use std::{collections::HashMap, sync::Arc};

fn accepted(outcome: SchemaAddOutcome) -> schema_types::Schema {
    match outcome {
        SchemaAddOutcome::Added(schema, _)
        | SchemaAddOutcome::AlreadyExists(schema, _)
        | SchemaAddOutcome::Expanded(_, schema, _)
        | SchemaAddOutcome::Composed(schema, _, _) => schema,
        SchemaAddOutcome::DescriptiveNameConflict(conflict) => {
            panic!("unexpected conflict: {conflict:?}")
        }
    }
}

#[test]
fn schema_creation_repost_and_field_permutation_keep_identity() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let first = accepted(
            state
                .add_schema(schema("parcel-app", false), HashMap::new())
                .await
                .expect("first"),
        );
        let second = accepted(
            state
                .add_schema(schema("parcel-app", true), HashMap::new())
                .await
                .expect("repost"),
        );
        assert_eq!(first.name, second.name);
        assert_eq!(first.field_hashes, second.field_hashes);
        assert_eq!(backend.load_all_schemas().await.expect("schemas").len(), 1);
        assert_eq!(
            backend
                .load_all_canonical_fields()
                .await
                .expect("fields")
                .len(),
            2
        );
        let reloaded = super::support::state(backend.clone()).await;
        assert_eq!(
            reloaded.get_schema_names().expect("names"),
            vec![first.name]
        );
    });
}

#[test]
fn schema_identity_separates_owner_and_key_layout() {
    for reverse in [false, true] {
        let mut first = schema("parcel-app", reverse);
        first.compute_identity_hash();
        let mut owner = schema("warehouse-app", reverse);
        owner.compute_identity_hash();
        let mut key = schema("parcel-app", reverse);
        key.key = Some(KeyConfig::new(Some("parcel_delivery_status".into()), None));
        key.compute_identity_hash();
        assert_ne!(first.identity_hash, owner.identity_hash);
        assert_ne!(first.identity_hash, key.identity_hash);
    }
}

#[test]
fn conflicting_schema_definition_does_not_publish_new_concepts() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        state
            .add_canonical_field(
                "parcel_identifier",
                field(
                    "Parcel identifier assigned by the carrier",
                    FieldValueType::Integer,
                ),
            )
            .await
            .expect("existing definition");
        assert!(state
            .add_schema(schema("parcel-app", false), HashMap::new())
            .await
            .is_err());
        assert!(backend
            .load_all_schemas()
            .await
            .expect("schemas")
            .is_empty());
        assert_eq!(
            backend
                .load_all_canonical_fields()
                .await
                .expect("fields")
                .len(),
            1
        );
        assert!(state.schemas.read().expect("cache").is_empty());
    });
}

#[test]
fn forged_field_hash_cannot_replace_the_canonical_identity() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        let mut proposal = schema("parcel-app", false);
        proposal
            .field_hashes
            .insert("parcel_delivery_status".into(), "00".repeat(32));
        let result = state.add_schema(proposal, HashMap::new()).await;
        match result {
            Ok(outcome) => {
                let stored = accepted(outcome);
                let vocabulary = backend.load_all_canonical_fields().await.expect("fields");
                assert_eq!(
                    stored.field_hashes["parcel_delivery_status"],
                    vocabulary["parcel_delivery_status"].field_hash("parcel_delivery_status"),
                    "service accepted a caller-supplied noncanonical identity"
                );
            }
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("field_hash") || message.contains("field identity"),
                    "unrelated failure does not prove identity rejection: {message}"
                );
            }
        }
    });
}

#[test]
fn invalid_snapshot_policy_preserves_existing_registry() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend.clone()).await;
        state
            .add_canonical_field(
                "parcel_identifier",
                field(
                    "Parcel identifier assigned by the carrier",
                    FieldValueType::String,
                ),
            )
            .await
            .expect("first");
        let durable_before = backend.load_all_canonical_fields().await.expect("fields");
        let mut snapshot = state.export_snapshot().expect("snapshot");
        snapshot.format_version += 1;
        assert!(state.import_snapshot(snapshot).is_err());
        assert_eq!(
            backend.load_all_canonical_fields().await.expect("fields"),
            durable_before
        );
        let mut snapshot = state.export_snapshot().expect("snapshot");
        snapshot
            .canonical_fields
            .get_mut("parcel_identifier")
            .expect("field")
            .description = String::new();
        assert!(state.import_snapshot(snapshot).is_err());
        assert_eq!(
            backend.load_all_canonical_fields().await.expect("fields"),
            durable_before
        );
    });
}
