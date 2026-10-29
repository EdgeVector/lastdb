use super::support::*;
use schema_service_core::{DeclareFieldInput, DeclareFieldRequest, ExternalSchemaPersistence};
use schema_types::FieldValueType;
use std::sync::atomic::Ordering;

#[test]
fn protocol_conflicting_instances_have_one_durable_winner() {
    run(async {
        let harness = Harness::protocol("conflict").await;
        let first = harness.state().await;
        let second = harness.state().await;
        harness.faults().pair_next_puts();
        let (a, b) = tokio::join!(
            first.add_canonical_field(
                "parcel_delivery_status",
                field("Parcel delivery status from first carrier")
            ),
            second.add_canonical_field(
                "parcel_delivery_status",
                field("Parcel delivery status from second carrier")
            ),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a
            .err()
            .or_else(|| b.err())
            .expect("one rejected definition");
        assert!(
            error.to_string().contains("canonical_field_conflict"),
            "{error}"
        );
        assert!(
            harness.faults().actual_conflicts.load(Ordering::SeqCst) >= 1,
            "the real server must reject a simultaneous conditional PUT"
        );
        let durable = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("durable winner");
        assert_eq!(durable.len(), 1);
        assert_eq!(
            *harness
                .state()
                .await
                .canonical_fields
                .read()
                .expect("cold cache"),
            durable
        );
        harness.cleanup().await;
    });
}

#[test]
fn protocol_disjoint_instances_retry_without_lost_writes() {
    run(async {
        let harness = Harness::protocol("disjoint").await;
        let first = harness.state().await;
        let second = harness.state().await;
        harness.faults().pair_next_puts();
        disjoint_writes(&first, &second).await;
        assert!(harness.faults().actual_conflicts.load(Ordering::SeqCst) >= 1);
        let durable = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("durable fields");
        assert_eq!(durable.len(), 2);
        assert_eq!(
            *harness
                .state()
                .await
                .canonical_fields
                .read()
                .expect("cold cache"),
            durable
        );
        harness.cleanup().await;
    });
}

async fn disjoint_writes(
    first: &schema_service_core::SchemaServiceState,
    second: &schema_service_core::SchemaServiceState,
) {
    let (a, b) = tokio::join!(
        first.add_canonical_field(
            "parcel_delivery_status",
            field("Parcel delivery status from the carrier")
        ),
        second.add_canonical_field(
            "parcel_current_location",
            field("Parcel current location from the carrier")
        ),
    );
    a.expect("first disjoint definition");
    b.expect("second disjoint definition");
}

#[test]
fn protocol_precondition_retry_exhaustion_is_bounded_and_atomic() {
    run(async {
        let harness = Harness::protocol("exhaust").await;
        let state = harness.state().await;
        harness.faults().reject_next_puts(10);
        let error = state
            .add_canonical_field(
                "parcel_delivery_status",
                field("Parcel delivery status from the carrier"),
            )
            .await
            .expect_err("retry budget");
        assert!(error.to_string().contains("Exceeded 10 retries"), "{error}");
        assert_eq!(harness.faults().puts.load(Ordering::SeqCst), 10);
        assert_eq!(
            harness.faults().injected_conflicts.load(Ordering::SeqCst),
            10
        );
        assert!(harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("durable fields")
            .is_empty());
        assert!(state
            .canonical_fields
            .read()
            .expect("warm cache")
            .is_empty());
        assert!(harness
            .state()
            .await
            .canonical_fields
            .read()
            .expect("cold cache")
            .is_empty());
        harness.cleanup().await;
    });
}

#[test]
fn protocol_conditional_request_conflict_retries_the_current_image() {
    run(async {
        let harness = Harness::protocol("requestconflict").await;
        let state = harness.state().await;
        harness.faults().conflict_next_put();
        let definition = field("Parcel delivery status from the carrier");
        state
            .add_canonical_field("parcel_delivery_status", definition.clone())
            .await
            .expect("409 ConditionalRequestConflict must re-read and retry");
        assert_eq!(harness.faults().puts.load(Ordering::SeqCst), 2);
        assert_eq!(
            harness
                .faults()
                .injected_request_conflicts
                .load(Ordering::SeqCst),
            1
        );
        let durable = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("durable fields");
        assert_eq!(durable.len(), 1);
        assert_eq!(durable["parcel_delivery_status"], definition);
        assert_eq!(
            *harness
                .state()
                .await
                .canonical_fields
                .read()
                .expect("cold cache"),
            durable
        );
        harness.cleanup().await;
    });
}

#[test]
fn protocol_lost_commit_replies_recover_without_duplicate_identity() {
    run(async {
        let harness = Harness::protocol("lostack").await;
        let state = harness.state().await;
        let definition = field("Parcel delivery status from the carrier");
        harness.faults().lose_next_committed_reply();
        state
            .add_canonical_field("parcel_delivery_status", definition.clone())
            .await
            .expect_err("a dropped successful reply is an unknown write outcome");
        assert_eq!(
            harness
                .faults()
                .committed_lost_replies
                .load(Ordering::SeqCst),
            1
        );
        assert!(state
            .canonical_fields
            .read()
            .expect("unpublished cache")
            .is_empty());
        let durable = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("committed field");
        assert_eq!(durable.len(), 1);
        assert_eq!(durable["parcel_delivery_status"], definition);
        state
            .add_canonical_field("parcel_delivery_status", definition)
            .await
            .expect("idempotent retry");
        lost_declaration_reply(&harness, &state).await;
        assert_eq!(
            harness
                .faults()
                .committed_lost_replies
                .load(Ordering::SeqCst),
            2
        );
        let cold = harness.state().await;
        assert_eq!(
            *cold.canonical_fields.read().expect("cold vocabulary"),
            durable
        );
        assert_eq!(
            cold.declared_fields
                .read()
                .expect("cold declarations")
                .len(),
            1
        );
        harness.cleanup().await;
    });
}

async fn lost_declaration_reply(
    harness: &Harness,
    state: &schema_service_core::SchemaServiceState,
) {
    let request = DeclareFieldRequest {
        owner_app_id: "parcel-app".into(),
        handle: "delivery".into(),
        readers: Vec::new(),
        fields: vec![DeclareFieldInput {
            name: "parcel_delivery_status".into(),
            field_type: FieldValueType::String,
            description: "Parcel delivery status from the carrier".into(),
            version: 1,
        }],
    };
    harness.faults().lose_next_committed_reply();
    state
        .declare_field(&request)
        .await
        .expect_err("lost declaration ack");
    let records = harness
        .backend()
        .load_all_declared_fields()
        .await
        .expect("committed declaration");
    assert_eq!(records.len(), 1);
    assert!(state
        .declared_fields
        .read()
        .expect("unpublished declaration cache")
        .is_empty());
    let retried = state
        .declare_field(&request)
        .await
        .expect("retry persisted handle");
    assert_eq!(retried.declaration_id, records[0].declaration_id);
    assert_eq!(retried.fields, records[0].fields);
    assert_eq!(
        harness
            .backend()
            .load_all_declared_fields()
            .await
            .expect("one declaration")
            .len(),
        1
    );
}
