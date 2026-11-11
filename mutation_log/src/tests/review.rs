use super::*;

#[test]
fn mixed_batch_cannot_store_a_forged_copy() {
    let (state, publisher, published, _person, submission) = copied_submission();
    let forged = copy_of(&state, &publisher, &published, &submission, 2, b"FORGED");
    let mut real = copy_of(&state, &publisher, &published, &submission, 3, b"two");
    real.incorporated_from.as_mut().expect("from").writer_seq = 2;
    let stored = try_append(&state, &publisher, &published, vec![forged, real]);
    assert_eq!(stored.as_ref().err(), Some(&LogError::UnknownSubmission));
    assert!(state
        .entries
        .iter()
        .all(|row| row.entry.payload != b"FORGED"));
}

#[test]
fn exact_incorporation_repeat_is_duplicate_and_a_changed_payload_is_not() {
    let (state, publisher, published, _person, submission) = copied_submission();
    let again = copy_of(&state, &publisher, &published, &submission, 2, b"one");
    let repeat = try_append(&state, &publisher, &published, vec![again]).expect("repeat");
    assert_eq!(
        repeat.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
    assert_eq!(repeat.state.entries.len(), state.entries.len());

    let forged = copy_of(&state, &publisher, &published, &submission, 2, b"FORGED");
    let changed = try_append(&state, &publisher, &published, vec![forged]);
    assert_eq!(changed.as_ref().err(), Some(&LogError::UnknownSubmission));
}

#[test]
fn mixed_incorporated_and_new_entries_are_rejected() {
    let (state, publisher, published, _person, submission) = copied_submission();
    let again = copy_of(&state, &publisher, &published, &submission, 2, b"one");
    let mut second = copy_of(&state, &publisher, &published, &submission, 3, b"two");
    second.incorporated_from.as_mut().expect("from").writer_seq = 2;
    let mixed = try_append(&state, &publisher, &published, vec![again, second]);
    assert_eq!(mixed.as_ref().err(), Some(&LogError::MixedIncorporation));
    assert_eq!(state.entries.len(), 3);
}

#[test]
fn repeated_writer_seq_with_different_content_is_conflict() {
    let (state, publisher, published) = shareable();
    let first = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    let state = apply(&state, &publisher, &published, vec![first], 1);
    let mut other = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    other.payload = b"other".to_vec();
    let conflict = try_append(&state, &publisher, &published, vec![other]);
    assert_eq!(conflict.as_ref().err(), Some(&LogError::Conflict));
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].entry.payload, vec![1]);
}

#[test]
fn reappend_of_a_trimmed_submission_is_rejected() {
    let (mut state, publisher, published, person, submission) = copied_submission();
    state = ack(&state, &publisher, 3);
    state = ack(&state, &person, 2);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 4,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    state = step(
        &state,
        Command::Trim {
            caller: publisher.clone(),
            now_ms: 4,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert!(state
        .entries
        .iter()
        .all(|row| row.entry.stream != submission));
    let again = copy_of(&state, &publisher, &published, &submission, 2, b"one");
    let refused = try_append(&state, &publisher, &published, vec![again]);
    assert_eq!(refused.as_ref().err(), Some(&LogError::UnknownSubmission));
}

fn copied_submission() -> (LogState, PrincipalId, StreamId, PrincipalId, StreamId) {
    let (mut state, publisher, published) = shareable();
    let person = principal("contributor");
    let submission = stream("contrib");
    state = step(
        &state,
        Command::Grant {
            caller: publisher.clone(),
            principal: person.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("grant")
    .state;
    let mut first = entry(&state.database_id, &submission, &person, "photos", 1, 1);
    first.payload = b"one".to_vec();
    let mut second = entry(&state.database_id, &submission, &person, "photos", 2, 2);
    second.payload = b"two".to_vec();
    state = apply(&state, &person, &submission, vec![first, second], 2);
    let copied = copy_of(&state, &publisher, &published, &submission, 1, b"one");
    state = apply(&state, &publisher, &published, vec![copied], 3);
    (state, publisher, published, person, submission)
}

fn copy_of(
    state: &LogState,
    publisher: &PrincipalId,
    published: &StreamId,
    submission: &StreamId,
    seq: u64,
    payload: &[u8],
) -> Entry {
    let mut entry = entry(&state.database_id, published, publisher, "photos", seq, seq);
    entry.payload = payload.to_vec();
    entry.incorporated_from = Some(Incorporation {
        submission_stream: submission.clone(),
        writer_seq: 1,
        contributor: principal("contributor"),
    });
    entry
}

fn try_append(
    state: &LogState,
    author: &PrincipalId,
    writer: &StreamId,
    entries: Vec<Entry>,
) -> Result<StepResult, LogError> {
    let begun = step(
        state,
        Command::BeginAppend {
            author: author.clone(),
            stream: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    step(
        &begun,
        Command::Append(AppendBatch {
            author: author.clone(),
            stream: writer.clone(),
            entries,
            accepted_at_ms: 9,
        }),
        &hearth,
    )
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
