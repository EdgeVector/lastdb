use super::support::*;
use schema_service_core::{
    CanonicalVocabularyWrite, DeclaredFieldRegistry, EmbedError, Embedder,
    ExternalSchemaPersistence, InMemorySchemaPersistence, SchemaConfig, SchemaServiceState,
};
use schema_types::FieldValueType;
use std::{
    collections::HashSet,
    sync::{Arc, Barrier},
};

#[test]
fn conflicting_same_name_race_commits_exactly_one_definition() {
    let backend = Arc::new(InMemorySchemaPersistence::default());
    let first = run(state(backend.clone()));
    let second = run(state(backend.clone()));
    let barrier = Arc::new(Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = [first, second]
            .into_iter()
            .enumerate()
            .map(|(number, state)| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    run(state.add_canonical_field(
                        "parcel_delivery_status",
                        field(
                            &format!("Parcel delivery status from carrier number {number}"),
                            FieldValueType::String,
                        ),
                    ))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("worker"))
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        run(backend.load_all_canonical_fields())
            .expect("fields")
            .len(),
        1
    );
    run(state(backend));
}

#[test]
fn independent_instances_keep_one_declaration_per_owner_handle() {
    let backend = Arc::new(InMemorySchemaPersistence::default());
    let states: Vec<_> = (0..16).map(|_| run(state(backend.clone()))).collect();
    let barrier = Arc::new(Barrier::new(states.len()));
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = states
            .into_iter()
            .map(|state| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    run(state.declare_field(&declaration("parcel-app", "delivery")))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("worker"))
            .collect::<Vec<_>>()
    });
    let successes: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().ok())
        .collect();
    assert!(!successes.is_empty());
    let identities: HashSet<_> = successes
        .iter()
        .map(|record| &record.declaration_id)
        .collect();
    assert_eq!(
        identities.len(),
        1,
        "successful concurrent declarations must agree"
    );
    for error in outcomes.iter().filter_map(|outcome| outcome.as_ref().err()) {
        assert!(
            format!("{error:?}").contains("declared_field_write_conflict"),
            "{error:?}"
        );
    }
    let reloaded = run(state(backend.clone()));
    let retry = run(reloaded.declare_field(&declaration("parcel-app", "delivery"))).expect("retry");
    assert_eq!(
        &retry.declaration_id,
        identities.into_iter().next().expect("winner")
    );
    assert_eq!(
        run(backend.load_all_declared_fields())
            .expect("declarations")
            .len(),
        1
    );
}

#[test]
fn concurrent_schema_creation_keeps_every_app_and_one_shared_vocabulary() {
    let backend = Arc::new(InMemorySchemaPersistence::default());
    let states: Vec<_> = (0..16).map(|_| run(state(backend.clone()))).collect();
    let barrier = Arc::new(Barrier::new(states.len()));
    std::thread::scope(|scope| {
        let handles: Vec<_> = states
            .into_iter()
            .enumerate()
            .map(|(number, state)| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    run(state.add_schema(
                        schema(&format!("parcel-app-{number}"), number % 2 == 0),
                        std::collections::HashMap::new(),
                    ))
                    .expect("schema creation")
                })
            })
            .collect();
        for handle in handles {
            assert!(matches!(
                handle.join().expect("worker"),
                schema_service_core::types::SchemaAddOutcome::Added(_, _)
            ));
        }
    });
    let schemas = run(backend.load_all_schemas()).expect("schemas");
    let owners: HashSet<_> = schemas
        .values()
        .map(|schema| &schema.owner_app_id)
        .collect();
    assert_eq!(schemas.len(), 16);
    assert_eq!(owners.len(), 16);
    assert_eq!(
        run(backend.load_all_canonical_fields())
            .expect("fields")
            .len(),
        2
    );
    assert_eq!(
        run(state(backend))
            .get_schema_names()
            .expect("reload")
            .len(),
        16
    );
}

#[test]
fn same_concept_in_unrelated_apps_does_not_share_declaration_identity() {
    run(async {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let state = state(backend).await;
        let first = state
            .declare_field(&declaration("parcel-app", "delivery"))
            .await
            .expect("first");
        let second = state
            .declare_field(&declaration("warehouse-app", "delivery"))
            .await
            .expect("second");
        assert_ne!(first.declaration_id, second.declaration_id);
        assert_ne!(first.fields[0].identity, second.fields[0].identity);
        let hashes = std::collections::HashMap::from([(
            first.fields[0].name.clone(),
            first.fields[0].identity.clone(),
        )]);
        assert!(state
            .authorize_declared_field_references(Some("warehouse-app"), &hashes)
            .is_err());
        assert!(state
            .authorize_declared_field_references(Some("parcel-app"), &hashes)
            .is_ok());
    });
}

/// Deliberate vectors exercise the policy; they do not measure model quality.
struct ParcelEmbedder;

impl Embedder for ParcelEmbedder {
    fn embed_text(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(if text.to_ascii_lowercase().contains("parcel") {
            vec![1.0, 0.0]
        } else {
            vec![0.0, 1.0]
        })
    }
}

#[test]
fn semantic_duplicate_admission_is_consistent_across_independent_caches() {
    run(async {
        stale_admission_images_cannot_publish_definitions().await;
        let backend = Arc::new(InMemorySchemaPersistence::default());
        let first = SchemaServiceState::new_with_external(
            backend.clone(),
            Arc::new(ParcelEmbedder),
            SchemaConfig::default(),
            process_clock(),
        )
        .await
        .expect("first state");
        let stale = SchemaServiceState::new_with_external(
            backend.clone(),
            Arc::new(ParcelEmbedder),
            SchemaConfig::default(),
            process_clock(),
        )
        .await
        .expect("stale state");
        let first_definition = field(
            "Parcel delivery status code supplied by carrier",
            FieldValueType::String,
        );
        let second_definition = field(
            "Parcel delivery status code reported by carrier",
            FieldValueType::String,
        );
        first
            .add_canonical_field("parcel_delivery_status", first_definition)
            .await
            .expect("first concept");
        assert!(
            first
                .add_canonical_field("parcel_delivery_state", second_definition.clone())
                .await
                .is_err(),
            "warm policy rejects semantic duplicate"
        );
        let outcome = stale
            .add_canonical_field("parcel_delivery_state", second_definition)
            .await;
        assert!(
            outcome.is_err(),
            "stale instance admitted a semantic duplicate: durable vocabulary = {:?}",
            backend
                .load_all_canonical_fields()
                .await
                .expect("fields")
                .keys()
        );
    });
}

/// Force a write between admission and commit. A mere refresh before an
/// unguarded write passes the stale-cache scenario but fails this boundary.
async fn stale_admission_images_cannot_publish_definitions() {
    let backend = Arc::new(InMemorySchemaPersistence::default());
    let vocabulary = CanonicalVocabularyWrite {
        expected: backend
            .load_all_canonical_fields()
            .await
            .expect("admission image"),
        fields: vec![(
            "parcel_delivery_status".into(),
            field(
                "Parcel delivery status code from the carrier",
                FieldValueType::String,
            ),
        )],
    };
    let writer = state(backend.clone()).await;
    writer
        .add_canonical_field(
            "parcel_current_location",
            field(
                "Parcel current location from the carrier",
                FieldValueType::String,
            ),
        )
        .await
        .expect("concurrent durable vocabulary change");
    let committed = backend
        .load_all_canonical_fields()
        .await
        .expect("committed image");
    let error = backend
        .save_schema_definitions_if_vocabulary(&[schema("parcel-app", false)], &vocabulary)
        .await
        .expect_err("a stale admission image cannot publish a schema");
    assert!(
        error
            .to_string()
            .contains("canonical_vocabulary_write_conflict"),
        "{error}"
    );
    assert!(backend
        .load_all_schemas()
        .await
        .expect("schemas")
        .is_empty());
    assert_eq!(
        backend.load_all_canonical_fields().await.expect("fields"),
        committed
    );

    let record = DeclaredFieldRegistry::new()
        .declare(&declaration("parcel-app", "delivery"), || {
            "fd_stale_image".into()
        })
        .expect("staged declaration");
    let error = backend
        .save_declared_field_definitions_if_vocabulary(&record, &vocabulary, None)
        .await
        .expect_err("a stale admission image cannot publish a declaration");
    assert!(
        error
            .to_string()
            .contains("canonical_vocabulary_write_conflict"),
        "{error}"
    );
    assert!(backend
        .load_all_declared_fields()
        .await
        .expect("declarations")
        .is_empty());
    assert_eq!(
        backend.load_all_canonical_fields().await.expect("fields"),
        committed
    );
}
