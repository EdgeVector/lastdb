use super::support::*;
use schema_service_core::{types::SchemaAddOutcome, ExternalSchemaPersistence};
use schema_types::{FieldValueType, Schema};
use std::{collections::HashMap, sync::atomic::Ordering};

fn accepted(outcome: SchemaAddOutcome) -> Schema {
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
fn protocol_expansion_fault_keeps_predecessor_successor_and_caches_atomic() {
    run(async {
        let harness = Harness::protocol("expand").await;
        let state = harness.state().await;
        let initial = accepted(
            state
                .add_schema(schema(), HashMap::new())
                .await
                .expect("initial schema"),
        );
        let before_schemas = harness
            .backend()
            .load_all_schemas()
            .await
            .expect("initial schemas");
        let before_fields = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("initial fields");
        let mut proposal = schema();
        proposal
            .fields
            .as_mut()
            .expect("fields")
            .push("parcel_delivery_location".into());
        proposal
            .field_types
            .insert("parcel_delivery_location".into(), FieldValueType::String);
        proposal.field_descriptions.insert(
            "parcel_delivery_location".into(),
            "Parcel delivery location from the carrier".into(),
        );
        let before_puts = harness.faults().puts.load(Ordering::SeqCst);
        harness.faults().fail_nth_next_put(2);
        let outcome = state.add_schema(proposal, HashMap::new()).await;
        let durable = harness
            .backend()
            .load_all_schemas()
            .await
            .expect("durable schemas");
        match outcome {
            Ok(outcome) => {
                let successor = accepted(outcome);
                assert_ne!(initial.name, successor.name);
                assert_eq!(
                    durable[&initial.name].superseded_by.as_deref(),
                    Some(successor.name.as_str())
                );
                assert_eq!(
                    harness.faults().puts.load(Ordering::SeqCst) - before_puts,
                    1,
                    "a successful expansion must commit both schemas in one conditional write"
                );
            }
            Err(_) => {
                assert_eq!(harness.faults().injected_failures.load(Ordering::SeqCst), 1);
                assert_eq!(
                    durable, before_schemas,
                    "a failed expansion cannot publish only its successor"
                );
                assert_eq!(
                    harness
                        .backend()
                        .load_all_canonical_fields()
                        .await
                        .expect("durable fields"),
                    before_fields
                );
            }
        }
        assert_eq!(*state.schemas.read().expect("warm schemas"), durable);
        assert_eq!(
            *harness.state().await.schemas.read().expect("cold schemas"),
            durable
        );
        harness.cleanup().await;
    });
}

fn proposal_with(name: &str, description: &str) -> Schema {
    let mut proposal = schema();
    proposal.fields.as_mut().expect("fields").push(name.into());
    proposal
        .field_types
        .insert(name.into(), FieldValueType::String);
    proposal
        .field_descriptions
        .insert(name.into(), description.into());
    proposal
}

#[test]
fn protocol_competing_expansions_do_not_fork_a_predecessor() {
    run(async {
        let harness = Harness::protocol("fork").await;
        let seeded = harness.state().await;
        let initial = accepted(
            seeded
                .add_schema(schema(), HashMap::new())
                .await
                .expect("initial"),
        );
        let first = harness.state().await;
        let second = harness.state().await;
        harness.faults().pair_next_puts();
        let (a, b) = tokio::join!(
            first.add_schema(
                proposal_with(
                    "parcel_delivery_location",
                    "Parcel delivery location from the carrier"
                ),
                HashMap::new()
            ),
            second.add_schema(
                proposal_with(
                    "parcel_customs_status",
                    "Parcel customs status from the carrier"
                ),
                HashMap::new()
            ),
        );
        let (winner, rejected, absent) = match (a, b) {
            (Ok(outcome), Err(error)) => (accepted(outcome), error, "parcel_customs_status"),
            (Err(error), Ok(outcome)) => (accepted(outcome), error, "parcel_delivery_location"),
            other => {
                panic!("one complete expansion and one explicit conflict are required: {other:?}")
            }
        };
        assert!(
            rejected
                .to_string()
                .contains("schema_expansion_write_conflict"),
            "{rejected}"
        );
        assert!(harness.faults().actual_conflicts.load(Ordering::SeqCst) >= 1);
        let durable = harness
            .backend()
            .load_all_schemas()
            .await
            .expect("durable pair");
        assert_eq!(durable.len(), 2);
        assert!(durable.contains_key(&winner.name));
        assert_eq!(
            durable[&initial.name].superseded_by.as_deref(),
            Some(winner.name.as_str())
        );
        let fields = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("atomic vocabulary");
        assert_eq!(fields.len(), 3);
        assert!(
            !fields.contains_key(absent),
            "the rejected expansion cannot publish its field"
        );
        assert_eq!(
            *harness
                .state()
                .await
                .schemas
                .read()
                .expect("cold definitions"),
            durable
        );
        harness.cleanup().await;
    });
}

#[test]
fn protocol_lost_expansion_reply_replays_one_complete_pair() {
    run(async {
        let harness = Harness::protocol("expandlost").await;
        let state = harness.state().await;
        let initial = accepted(
            state
                .add_schema(schema(), HashMap::new())
                .await
                .expect("initial"),
        );
        let before = harness.backend().load_all_schemas().await.expect("before");
        let proposal = proposal_with(
            "parcel_delivery_location",
            "Parcel delivery location from the carrier",
        );
        harness.faults().lose_next_committed_reply();
        state
            .add_schema(proposal.clone(), HashMap::new())
            .await
            .expect_err("lost atomic commit reply");
        assert_eq!(
            harness
                .faults()
                .committed_lost_replies
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            *state.schemas.read().expect("unpublished schema cache"),
            before
        );
        assert_eq!(
            state
                .canonical_fields
                .read()
                .expect("unpublished vocabulary cache")
                .len(),
            2
        );
        let committed = harness
            .backend()
            .load_all_schemas()
            .await
            .expect("committed pair");
        assert_eq!(committed.len(), 2);
        let target = committed[&initial.name]
            .superseded_by
            .clone()
            .expect("committed predecessor marker");
        assert!(committed.contains_key(&target));
        let retried = accepted(
            state
                .add_schema(proposal, HashMap::new())
                .await
                .expect("idempotent expansion retry"),
        );
        assert_eq!(retried.name, target);
        let durable = harness
            .backend()
            .load_all_schemas()
            .await
            .expect("same pair after retry");
        assert_eq!(durable, committed);
        assert_eq!(
            *state.schemas.read().expect("published warm definitions"),
            durable
        );
        assert_eq!(
            *harness
                .state()
                .await
                .schemas
                .read()
                .expect("cold definitions"),
            durable
        );
        harness.cleanup().await;
    });
}
