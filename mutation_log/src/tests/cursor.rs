use super::*;

#[test]
fn caught_up_receiver_does_not_pin_the_floor_or_expire() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = writer_of(&contributor, "contrib");
    state = grant_receive(&state, &publisher, &receiver);
    state = grant_submit(&state, &publisher, &contributor);
    let visible = entry(&state.database_id, &published, "photos", 1, 1);
    state = apply(&state, &published, vec![visible], 0);
    let hidden = (1..=3)
        .map(|seq| entry(&state.database_id, &submission, "photos", seq, seq))
        .collect();
    state = apply(&state, &submission, hidden, 0);
    assert_eq!(state.head.as_ref().map(|head| head.delivery_seq), Some(4));

    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.next, 4);
    let head = state
        .head_for(&receiver, &hearth)
        .expect("head")
        .expect("row");
    assert_eq!(head.cursor, 1);
    let contributor_page = state.list(&contributor, 0, 10, 1, &hearth).expect("own");
    assert_eq!(contributor_page.entries.len(), 3);
    assert_eq!(contributor_page.next, 4);

    assert_eq!(state.ack(&receiver, 5).err(), Some(LogError::AckAhead));
    state = ack(&state, &receiver, 4);
    state = ack(&state, &contributor, 4);
    state = ack(&state, &publisher, 4);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 1,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;

    assert_eq!(state.effective_floor(MAX_RETENTION_MS), 4);
    let caught_up = state
        .list(&receiver, 4, 10, MAX_RETENTION_MS, &hearth)
        .expect("caught up receiver stays readable");
    assert!(caught_up.entries.is_empty());
    assert_eq!(caught_up.next, 4);
    assert_eq!(
        state.list(&receiver, 0, 10, MAX_RETENTION_MS, &hearth),
        Err(LogError::CursorExpired {
            snapshot_through: 4
        })
    );
    state = step(
        &state,
        Command::Trim {
            caller: publisher,
            now_ms: MAX_RETENTION_MS,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert!(state.entries.is_empty());
}
