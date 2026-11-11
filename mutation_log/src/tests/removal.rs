use super::*;

#[test]
fn revoke_stops_reads_and_clears_the_open_append() {
    let (mut state, publisher, _published) = shareable();
    let contributor = principal("contributor");
    let submission = writer_of(&contributor, "contrib");
    state = grant_submit(&state, &publisher, &contributor);
    state = step(
        &state,
        Command::BeginAppend {
            writer: submission.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let sample = entry(&state.database_id, &submission, "photos", 1, 1);
    assert!(can_read(&contributor, &state, &sample));
    state = step(
        &state,
        Command::Revoke {
            caller: publisher,
            principal: contributor.clone(),
            capability: Capability::Submit,
        },
        &hearth,
    )
    .expect("revoke")
    .state;
    assert!(!can_read(&contributor, &state, &sample));
    assert!(!state.inflight.contains(&submission));
    let finished = step(
        &state,
        Command::FinishAppend { writer: submission },
        &hearth,
    );
    assert_eq!(finished, Err(LogError::AppendNotOpen));
    let stranger = step(
        &state,
        Command::Grant {
            caller: contributor,
            principal: principal("receiver"),
            capability: Capability::Receive,
        },
        &hearth,
    );
    assert_eq!(stranger, Err(LogError::Unauthorized));
}

#[test]
fn revoke_cuts_off_every_device_of_the_key() {
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
    .expect("phone")
    .state;
    state = step(
        &state,
        Command::BeginAppend {
            writer: laptop.clone(),
        },
        &hearth,
    )
    .expect("laptop")
    .state;
    state = step(
        &state,
        Command::Revoke {
            caller: owner.clone(),
            principal: owner.clone(),
            capability: Capability::Owner,
        },
        &hearth,
    )
    .expect("revoke")
    .state;
    assert!(state.inflight.is_empty());
    for writer in [phone, laptop] {
        let refused = step(&state, Command::BeginAppend { writer }, &hearth);
        assert_eq!(refused, Err(LogError::Unauthorized));
    }
    let sample = entry(
        &state.database_id,
        &writer_of(&owner, "phone"),
        "notes",
        1,
        1,
    );
    assert!(!can_read(&owner, &state, &sample));
}

#[test]
fn incorporation_must_match_a_pending_submission() {
    let (state, _publisher, writer) = shareable();
    let mut forged = entry(&state.database_id, &writer, "photos", 1, 1);
    forged.incorporated_from = Some(Incorporation {
        writer: writer_of(&principal("contributor"), "missing"),
        writer_seq: 1,
    });
    let opened = step(
        &state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            writer,
            entries: vec![forged],
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::UnknownSubmission));
    assert!(state.entries.is_empty());
}

#[test]
fn a_receive_grant_cannot_append() {
    let (mut state, publisher, _published) = shareable();
    let receiver = principal("receiver");
    state = grant_receive(&state, &publisher, &receiver);
    let writer = writer_of(&receiver, "phone");
    let refused = step(&state, Command::BeginAppend { writer }, &hearth);
    assert_eq!(refused, Err(LogError::Unauthorized));
}

#[test]
fn writer_seq_max_is_rejected() {
    let (mut state, _publisher, writer) = shareable();
    state.tips.insert(writer.clone(), u64::MAX - 1);
    let entry = entry(&state.database_id, &writer, "photos", u64::MAX, 1);
    let opened = step(
        &state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            writer,
            entries: vec![entry],
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::SequenceExhausted));
    assert!(state.entries.is_empty());
}
