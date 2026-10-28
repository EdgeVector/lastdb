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
        assert_declared_hash_cases(state, backend).await;
    });
}

async fn assert_declared_hash_cases(
    state: schema_service_core::SchemaServiceState,
    backend: Arc<InMemorySchemaPersistence>,
) {
    let record = state
        .declare_field(&declaration("parcel-app", "delivery"))
        .await
        .expect("declaration");
    let identity = record.fields[0].identity.clone();
    let mut proposal = schema("parcel-app", false);
    proposal
        .field_hashes
        .insert("parcel_delivery_status".into(), identity.clone());
    proposal.field_declarations.insert(
        "parcel_delivery_status".into(),
        record.declaration_id.clone(),
    );
    let stored = accepted(
        state
            .add_schema(proposal.clone(), HashMap::new())
            .await
            .expect("verified declaration"),
    );
    assert_eq!(stored.field_hashes["parcel_delivery_status"], identity);
    let reloaded = super::support::state(backend.clone()).await;
    assert_eq!(
        reloaded
            .get_schema_by_name(&stored.name)
            .expect("lookup")
            .expect("stored schema")
            .field_hashes["parcel_delivery_status"],
        identity
    );

    let mut forged_hash = proposal.clone();
    forged_hash
        .field_hashes
        .insert("parcel_delivery_status".into(), "00".repeat(32));
    let mut unknown_declaration = proposal.clone();
    unknown_declaration
        .field_declarations
        .insert("parcel_delivery_status".into(), "fd_unknown".into());
    let mut different_meaning = proposal.clone();
    different_meaning
        .field_declarations
        .insert("parcel_identifier".into(), record.declaration_id);
    different_meaning
        .field_hashes
        .insert("parcel_identifier".into(), identity);
    let mut unpermitted_app = proposal;
    unpermitted_app.owner_app_id = Some("warehouse-app".into());
    for invalid in [
        forged_hash,
        unknown_declaration,
        different_meaning,
        unpermitted_app,
    ] {
        let error = reloaded
            .add_schema(invalid, HashMap::new())
            .await
            .expect_err("forged declaration reference");
        assert!(error.to_string().contains("invalid_field_hash"), "{error}");
    }
    assert_eq!(backend.load_all_schemas().await.expect("schemas").len(), 1);
    assert_declared_expansion(reloaded, backend, stored).await;
}

async fn assert_declared_expansion(
    state: schema_service_core::SchemaServiceState,
    backend: Arc<InMemorySchemaPersistence>,
    predecessor: schema_types::Schema,
) {
    // The proposal omits the predecessor's declared field. Expansion must
    // inherit both the identity and the declaration that authorizes it.
    let mut incoming = schema("parcel-app", false);
    incoming.fields = Some(vec![
        "parcel_identifier".into(),
        "parcel_weight_grams".into(),
    ]);
    incoming.field_descriptions.remove("parcel_delivery_status");
    incoming.field_types.remove("parcel_delivery_status");
    incoming.field_descriptions.insert(
        "parcel_weight_grams".into(),
        "Parcel weight measured in grams".into(),
    );
    incoming
        .field_types
        .insert("parcel_weight_grams".into(), FieldValueType::Integer);
    let expanded = match state
        .add_schema(incoming, HashMap::new())
        .await
        .expect("inherit declared field")
    {
        SchemaAddOutcome::Expanded(old, expanded, _) => {
            assert_eq!(old, predecessor.name);
            expanded
        }
        other => panic!("expected expansion, got {other:?}"),
    };
    assert_eq!(
        expanded.field_hashes["parcel_delivery_status"],
        predecessor.field_hashes["parcel_delivery_status"]
    );
    assert_eq!(
        expanded.field_declarations["parcel_delivery_status"],
        predecessor.field_declarations["parcel_delivery_status"]
    );
    let reloaded = super::support::state(backend.clone()).await;
    assert_eq!(
        reloaded
            .get_schema_by_name(&expanded.name)
            .expect("lookup")
            .expect("expanded schema")
            .field_declarations,
        expanded.field_declarations
    );

    // An explicit verified canonical hash must not inherit the predecessor's
    // declaration label. The app intentionally changes this field's claim.
    let mut incoming = expanded.clone();
    incoming
        .fields
        .as_mut()
        .expect("fields")
        .push("parcel_height_millimeters".into());
    incoming.field_descriptions.insert(
        "parcel_height_millimeters".into(),
        "Parcel height measured in millimeters".into(),
    );
    incoming
        .field_types
        .insert("parcel_height_millimeters".into(), FieldValueType::Integer);
    incoming.field_declarations.remove("parcel_delivery_status");
    let canonical = schema_types::compute_field_hash(
        "parcel_delivery_status",
        &incoming.field_descriptions["parcel_delivery_status"],
        &FieldValueType::String,
        1,
    );
    incoming
        .field_hashes
        .insert("parcel_delivery_status".into(), canonical.clone());
    let previous_name = expanded.name;
    let replaced = match reloaded
        .add_schema(incoming, HashMap::new())
        .await
        .expect("preserve incoming canonical identity")
    {
        SchemaAddOutcome::Expanded(old, expanded, _) => {
            assert_eq!(old, previous_name);
            expanded
        }
        other => panic!("expected second expansion, got {other:?}"),
    };
    assert_eq!(replaced.field_hashes["parcel_delivery_status"], canonical);
    assert!(!replaced
        .field_declarations
        .contains_key("parcel_delivery_status"));
    let final_state = super::support::state(backend.clone()).await;
    assert_eq!(final_state.get_schema_count(), 3);
    assert_eq!(backend.load_all_schemas().await.expect("schemas").len(), 3);
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
