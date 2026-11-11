use super::*;

#[test]
fn revoke_stops_reads_and_clears_the_open_append() {
    let (mut state, publisher, _published) = shareable();
    let contributor = principal("contributor");
    let submission = stream("contrib");
    state = step(
        &state,
        Command::Grant {
            caller: publisher.clone(),
            principal: contributor.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("grant")
    .state;
    state = step(
        &state,
        Command::BeginAppend {
            author: contributor.clone(),
            stream: submission.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    assert!(can_read(&contributor, &state, &submission));
    state = step(
        &state,
        Command::Revoke {
            caller: publisher,
            principal: contributor.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("revoke")
    .state;
    assert!(!can_read(&contributor, &state, &submission));
    assert!(!state.inflight.contains_key(&submission));
    let finished = step(
        &state,
        Command::FinishAppend {
            caller: contributor.clone(),
            stream: submission,
        },
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
fn incorporation_must_match_a_pending_submission() {
    let (state, publisher, published) = shareable();
    let mut forged = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    forged.incorporated_from = Some(Incorporation {
        submission_stream: stream("missing"),
        writer_seq: 1,
        contributor: principal("contributor"),
    });
    let opened = step(
        &state,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            author: publisher,
            stream: published,
            entries: vec![forged],
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::UnknownSubmission));
    assert!(state.entries.is_empty());
}

#[test]
fn writer_seq_max_is_rejected() {
    let (mut state, publisher, published) = shareable();
    state
        .streams
        .get_mut(&published)
        .expect("publisher stream")
        .last_writer_seq = u64::MAX - 1;
    let entry = entry(
        &state.database_id,
        &published,
        &publisher,
        "photos",
        u64::MAX,
        1,
    );
    let opened = step(
        &state,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            author: publisher,
            stream: published,
            entries: vec![entry],
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::SequenceExhausted));
    assert!(state.entries.is_empty());
}
