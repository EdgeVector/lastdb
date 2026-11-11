use super::*;

#[test]
fn duplicate_accepts_next_and_gap_stores_nothing() {
    let (state, publisher, writer) = shareable();
    let photos = entry(&state.database_id, &writer, "photos", 1, 10);
    let state = apply(&state, &writer, vec![photos], 10);
    assert_eq!(state.head.as_ref().map(|head| head.delivery_seq), Some(1));

    let again = entry(&state.database_id, &writer, "photos", 1, 10);
    let begun = step(
        &state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let duplicate = step(
        &begun,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries: vec![again],
            accepted_at_ms: 11,
        }),
        &hearth,
    )
    .expect("duplicate");
    assert_eq!(
        duplicate.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
    assert_eq!(duplicate.state.entries.len(), 1);

    let skipped = entry(&state.database_id, &writer, "photos", 3, 12);
    let gap = step(
        &begun,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries: vec![skipped],
            accepted_at_ms: 12,
        }),
        &hearth,
    );
    assert_eq!(
        gap,
        Err(LogError::Gap {
            expected: 2,
            got: 3
        })
    );
    assert_eq!(state.entries.len(), 1);
    let closed = step(
        &duplicate.state,
        Command::FinishAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state;

    let next = entry(&state.database_id, &writer, "photos", 2, 13);
    let state = apply(&closed, &writer, vec![next], 13);
    assert_eq!(state.entries.len(), 2);
    assert_eq!(state.entries[1].delivery_seq, 2);
    let _ = publisher;
}

#[test]
fn prefix_duplicates_then_one_new_entry_are_one_transaction() {
    let (state, _publisher, writer) = shareable();
    let first = entry(&state.database_id, &writer, "photos", 1, 1);
    let state = apply(&state, &writer, vec![first.clone()], 1);
    let second = entry(&state.database_id, &writer, "photos", 2, 2);
    let begun = step(
        &state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let applied = step(
        &begun,
        Command::Append(AppendBatch {
            writer,
            entries: vec![first, second],
            accepted_at_ms: 2,
        }),
        &hearth,
    )
    .expect("append");
    assert_eq!(
        applied.outcome,
        Outcome::Appended(AppendOutcome::Accepted {
            delivery_seqs: vec![2]
        })
    );
}

#[test]
fn one_inflight_append_per_writer_and_batch_limit() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let laptop = writer_of(&owner, "laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &owner, &owner);
    state = step(
        &state,
        Command::BeginAppend {
            writer: phone.clone(),
        },
        &hearth,
    )
    .expect("open phone")
    .state;
    state = step(&state, Command::BeginAppend { writer: laptop }, &hearth)
        .expect("open laptop")
        .state;
    let second = step(&state, Command::BeginAppend { writer: phone }, &hearth);
    assert_eq!(second, Err(LogError::AppendInFlight));

    let (share, _publisher, writer) = shareable();
    let mut entries = Vec::new();
    for seq in 1..=91 {
        entries.push(entry(&share.database_id, &writer, "photos", seq, seq));
    }
    let opened = step(
        &share,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let too_big = step(
        &opened,
        Command::Append(AppendBatch {
            writer,
            entries,
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(too_big, Err(LogError::BatchTooLarge));
    assert!(share.entries.is_empty());
}

#[test]
fn merge_order_follows_the_stamp_and_delivery_order_does_not() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let laptop = writer_of(&owner, "laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &owner, &owner);
    let later = entry(&state.database_id, &phone, "hearth", 1, 200);
    let earlier = entry(&state.database_id, &laptop, "hearth", 1, 100);
    state = apply(&state, &phone, vec![later], 1);
    state = apply(&state, &laptop, vec![earlier], 2);
    assert_eq!(state.entries[0].delivery_seq, 1);
    assert_eq!(state.entries[0].entry.writer.device.as_str(), "phone");
    let merged = merge_order(&state.entries);
    assert_eq!(merged[0].entry.writer.device.as_str(), "laptop");
    assert_eq!(merged[1].entry.writer.device.as_str(), "phone");
    assert_eq!(merged[0].delivery_seq, 2);
}

#[test]
fn two_devices_of_one_key_append_without_conflict() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let laptop = writer_of(&owner, "laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &owner, &owner);
    let phone_begun = step(
        &state,
        Command::BeginAppend {
            writer: phone.clone(),
        },
        &hearth,
    )
    .expect("phone begin")
    .state;
    let both = step(
        &phone_begun,
        Command::BeginAppend {
            writer: laptop.clone(),
        },
        &hearth,
    )
    .expect("laptop begin")
    .state;
    let phone_entry = entry(&state.database_id, &phone, "notes", 1, 1);
    let laptop_entry = entry(&state.database_id, &laptop, "notes", 1, 2);
    let stored = step(
        &both,
        Command::Append(AppendBatch {
            writer: phone,
            entries: vec![phone_entry],
            accepted_at_ms: 1,
        }),
        &hearth,
    )
    .expect("phone");
    let stored = step(
        &stored.state,
        Command::Append(AppendBatch {
            writer: laptop,
            entries: vec![laptop_entry],
            accepted_at_ms: 2,
        }),
        &hearth,
    )
    .expect("laptop");
    assert!(matches!(
        stored.outcome,
        Outcome::Appended(AppendOutcome::Accepted { .. })
    ));
    assert_eq!(stored.state.entries.len(), 2);
    let visible = filter_readable(&stored.state.entries, &owner, &stored.state, &hearth);
    assert_eq!(visible.len(), 2);
}
