use super::*;

#[test]
fn trim_floor_is_the_minimum_ack() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let laptop = writer_of(&owner, "laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &owner, &owner);
    let first = entry(&state.database_id, &phone, "notes", 1, 1);
    let second = entry(&state.database_id, &laptop, "notes", 1, 2);
    state = apply(&state, &phone, vec![first], 10);
    state = apply(&state, &laptop, vec![second], 11);
    state = step(
        &state,
        Command::Snapshot {
            caller: owner.clone(),
            at_ms: 12,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    state = ack(&state, &owner, 1);
    assert_eq!(state.effective_floor(12), 1);
    state = step(
        &state,
        Command::Trim {
            caller: owner.clone(),
            now_ms: 12,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].delivery_seq, 2);
    state = ack(&state, &owner, 2);
    state = step(
        &state,
        Command::Trim {
            caller: owner,
            now_ms: 12,
        },
        &hearth,
    )
    .expect("trim rest")
    .state;
    assert!(state.entries.is_empty());
    assert_eq!(state.trimmed_through, 2);
}

#[test]
fn two_devices_hold_the_floor_at_the_lesser_ack() {
    let phone_owner = principal("phone");
    let laptop_owner = principal("laptop");
    let phone = writer_of(&phone_owner, "phone");
    let laptop = writer_of(&laptop_owner, "laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &phone_owner, &phone_owner);
    state = grant_owner(&state, &phone_owner, &laptop_owner);
    let first = entry(&state.database_id, &phone, "notes", 1, 1);
    let second = entry(&state.database_id, &laptop, "notes", 1, 2);
    state = apply(&state, &phone, vec![first], 10);
    state = apply(&state, &laptop, vec![second], 11);
    state = step(
        &state,
        Command::Snapshot {
            caller: phone_owner.clone(),
            at_ms: 12,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    state = ack(&state, &phone_owner, 2);
    state = ack(&state, &laptop_owner, 1);
    assert_eq!(state.effective_floor(12), 1);
    state = step(
        &state,
        Command::Trim {
            caller: phone_owner,
            now_ms: 12,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].delivery_seq, 2);
}

#[test]
fn retention_expires_a_cursor_and_points_at_the_snapshot() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    state = grant_receive(&state, &publisher, &receiver);
    let note = entry(&state.database_id, &published, "photos", 1, 1);
    state = apply(&state, &published, vec![note], 0);
    let fresh = state.list(&receiver, 0, 10, 1, &hearth).expect("fresh");
    assert_eq!(fresh.entries.len(), 1);
    assert!(state.snapshot_due(MAX_RETENTION_MS));
    let held = state
        .list(&receiver, 0, 10, MAX_RETENTION_MS, &hearth)
        .expect("still readable until a snapshot exists");
    assert_eq!(held.entries.len(), 1);
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
    let expired = state.list(&receiver, 0, 10, MAX_RETENTION_MS, &hearth);
    assert_eq!(
        expired,
        Err(LogError::CursorExpired {
            snapshot_through: 1
        })
    );
    let caught_up = state
        .list(&receiver, 1, 10, MAX_RETENTION_MS, &hearth)
        .expect("after snapshot");
    assert!(caught_up.entries.is_empty());
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
    assert_eq!(state.trimmed_through, 1);
}
