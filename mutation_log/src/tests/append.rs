use super::*;

#[test]
fn duplicate_accepts_next_and_gap_stores_nothing() {
    let (state, publisher, published) = shareable();
    let photos = entry(&state.database_id, &published, &publisher, "photos", 1, 10);
    let state = apply(&state, &publisher, &published, vec![photos], 10);
    assert_eq!(state.head.as_ref().map(|head| head.delivery_seq), Some(1));

    let again = entry(&state.database_id, &published, &publisher, "photos", 1, 10);
    let begun = step(
        &state,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let duplicate = step(
        &begun,
        Command::Append(AppendBatch {
            author: publisher.clone(),
            stream: published.clone(),
            entries: vec![again],
            accepted_at_ms: 11,
        }),
        &hearth,
    )
    .expect("duplicate");
    assert_eq!(
        duplicate.outcome,
        Outcome::Appended(AppendOutcome::Duplicate)
    );
    assert_eq!(duplicate.state.entries.len(), 1);

    let skipped = entry(&state.database_id, &published, &publisher, "photos", 3, 12);
    let gap = step(
        &begun,
        Command::Append(AppendBatch {
            author: publisher.clone(),
            stream: published.clone(),
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
            caller: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state;

    let next = entry(&state.database_id, &published, &publisher, "photos", 2, 13);
    let state = apply(&closed, &publisher, &published, vec![next], 13);
    assert_eq!(state.entries.len(), 2);
    assert_eq!(state.entries[1].delivery_seq, 2);
}

#[test]
fn prefix_duplicates_then_one_new_entry_are_one_transaction() {
    let (state, publisher, published) = shareable();
    let first = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    let state = apply(&state, &publisher, &published, vec![first.clone()], 1);
    let second = entry(&state.database_id, &published, &publisher, "photos", 2, 2);
    let begun = step(
        &state,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let applied = step(
        &begun,
        Command::Append(AppendBatch {
            author: publisher,
            stream: published,
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
fn one_inflight_append_per_stream_and_batch_limit() {
    let owner = principal("owner");
    let phone = stream("phone");
    let laptop = stream("laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = step(
        &state,
        Command::Grant {
            caller: owner.clone(),
            principal: owner.clone(),
            capability: Capability::Device {
                stream: phone.clone(),
            },
        },
        &hearth,
    )
    .expect("phone")
    .state;
    state = step(
        &state,
        Command::Grant {
            caller: owner.clone(),
            principal: owner.clone(),
            capability: Capability::Device {
                stream: laptop.clone(),
            },
        },
        &hearth,
    )
    .expect("laptop")
    .state;
    state = step(
        &state,
        Command::BeginAppend {
            author: owner.clone(),
            stream: phone.clone(),
        },
        &hearth,
    )
    .expect("open phone")
    .state;
    state = step(
        &state,
        Command::BeginAppend {
            author: owner.clone(),
            stream: laptop,
        },
        &hearth,
    )
    .expect("open laptop")
    .state;
    let second = step(
        &state,
        Command::BeginAppend {
            author: owner,
            stream: phone,
        },
        &hearth,
    );
    assert_eq!(second, Err(LogError::AppendInFlight));

    let (share, publisher, published) = shareable();
    let mut entries = Vec::new();
    for seq in 1..=91 {
        entries.push(entry(
            &share.database_id,
            &published,
            &publisher,
            "photos",
            seq,
            seq,
        ));
    }
    let opened = step(
        &share,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let too_big = step(
        &opened,
        Command::Append(AppendBatch {
            author: publisher,
            stream: published,
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
    let phone = stream("phone");
    let laptop = stream("laptop");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    for writer in [&phone, &laptop] {
        state = step(
            &state,
            Command::Grant {
                caller: owner.clone(),
                principal: owner.clone(),
                capability: Capability::Device {
                    stream: writer.clone(),
                },
            },
            &hearth,
        )
        .expect("grant")
        .state;
    }
    let later = entry(&state.database_id, &phone, &owner, "hearth", 1, 200);
    let earlier = entry(&state.database_id, &laptop, &owner, "hearth", 1, 100);
    state = apply(&state, &owner, &phone, vec![later], 1);
    state = apply(&state, &owner, &laptop, vec![earlier], 2);
    assert_eq!(state.entries[0].delivery_seq, 1);
    assert_eq!(state.entries[0].entry.stream, phone);
    let merged = merge_order(&state.entries);
    assert_eq!(merged[0].entry.stream, laptop);
    assert_eq!(merged[1].entry.stream, phone);
    assert_eq!(merged[0].delivery_seq, 2);
}
