use super::*;

#[test]
fn caught_up_receiver_does_not_pin_the_floor_or_expire() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = stream("contrib");
    state = grant_receive(&state, &publisher, &receiver);
    state = grant_submit(&state, &publisher, &contributor, &submission);
    let visible = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    state = apply(&state, &publisher, &published, vec![visible], 0);
    let hidden = (1..=3)
        .map(|seq| {
            entry(
                &state.database_id,
                &submission,
                &contributor,
                "photos",
                seq,
                seq,
            )
        })
        .collect();
    state = apply(&state, &contributor, &submission, hidden, 0);
    assert_eq!(state.head.as_ref().map(|head| head.delivery_seq), Some(4));

    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.next, 1);
    let head = state
        .head_for(&receiver, &hearth)
        .expect("head")
        .expect("row");
    assert_eq!(head.cursor, 1);
    let contributor_page = state.list(&contributor, 0, 10, 1, &hearth).expect("own");
    assert_eq!(contributor_page.next, 3);

    assert_eq!(
        state.ack(&receiver, 4, &hearth).err(),
        Some(LogError::AckAhead)
    );
    assert_eq!(
        state.ack(&receiver, 2, &hearth).err(),
        Some(LogError::AckAhead)
    );
    state = ack(&state, &receiver, 1);
    state = ack(&state, &contributor, 3);
    state = ack(&state, &publisher, 4);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher,
            at_ms: 1,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;

    assert_eq!(state.effective_floor(MAX_RETENTION_MS, &hearth), 4);
    let caught_up = state
        .list(&receiver, 1, 10, MAX_RETENTION_MS, &hearth)
        .expect("caught up receiver stays readable");
    assert!(caught_up.entries.is_empty());
    assert_eq!(caught_up.next, 1);
    assert_eq!(
        state.list(&receiver, 0, 10, MAX_RETENTION_MS, &hearth),
        Err(LogError::CursorExpired {
            snapshot_through: 1
        })
    );
}

fn grant_receive(state: &LogState, publisher: &PrincipalId, receiver: &PrincipalId) -> LogState {
    step(
        state,
        Command::Grant {
            caller: publisher.clone(),
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("receive")
    .state
}

fn grant_submit(
    state: &LogState,
    publisher: &PrincipalId,
    contributor: &PrincipalId,
    submission: &StreamId,
) -> LogState {
    step(
        state,
        Command::Grant {
            caller: publisher.clone(),
            principal: contributor.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("submit")
    .state
}

fn ack(state: &LogState, consumer: &PrincipalId, through: u64) -> LogState {
    step(
        state,
        Command::Ack {
            consumer: consumer.clone(),
            through,
        },
        &hearth,
    )
    .expect("ack")
    .state
}
