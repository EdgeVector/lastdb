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
    assert_eq!(head.cursor, 4);
    let contributor_page = state.list(&contributor, 0, 10, 1, &hearth).expect("own");
    assert_eq!(contributor_page.entries.len(), 3);
    assert_eq!(contributor_page.next, 4);

    let receiver_device = writer_of(&receiver, "phone");
    assert_eq!(
        state.ack(&receiver_device, 5).err(),
        Some(LogError::AckAhead)
    );
    state = ack(&state, &receiver_device, 4);
    state = ack(&state, &submission, 4);
    state = ack(&state, &published, 4);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 1,
            snapshot_ref: snap("caught-up"),
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

#[test]
fn scoped_head_ack_does_not_pin_trim() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = writer_of(&contributor, "contrib");
    state = grant_receive(&state, &publisher, &receiver);
    state = grant_submit(&state, &publisher, &contributor);
    let visible = entry(&state.database_id, &published, "photos", 1, 1);
    state = apply(&state, &published, vec![visible], 1);
    let hidden = entry(&state.database_id, &submission, "photos", 1, 2);
    state = apply(&state, &submission, vec![hidden], 2);
    let head = state
        .head_for(&receiver, &hearth)
        .expect("head")
        .expect("row");
    assert_eq!(state.entries[0].delivery_seq, 1);
    assert_eq!(head.cursor, 2);
    state = ack(&state, &writer_of(&receiver, "phone"), head.cursor);
    state = ack(&state, &submission, head.cursor);
    state = ack(&state, &published, head.cursor);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 3,
            snapshot_ref: snap("hidden-tail"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    state = step(
        &state,
        Command::Trim {
            caller: publisher,
            now_ms: 3,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert!(state.entries.is_empty());
    assert_eq!(state.trimmed_through, head.cursor);
}

#[test]
fn head_is_present_when_every_entry_is_hidden() {
    let (mut state, publisher, _published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = writer_of(&contributor, "contrib");
    state = grant_receive(&state, &publisher, &receiver);
    state = grant_submit(&state, &publisher, &contributor);
    let hidden = entry(&state.database_id, &submission, "photos", 1, 1);
    state = apply(&state, &submission, vec![hidden], 1);
    let head = state
        .head_for(&receiver, &hearth)
        .expect("head")
        .expect("hidden entry");
    assert_eq!(head.cursor, 1);
    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("page");
    assert!(page.entries.is_empty());
    assert_eq!(page.next, 1);
}
