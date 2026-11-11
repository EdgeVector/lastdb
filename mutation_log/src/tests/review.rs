use super::*;

#[test]
fn mixed_batch_cannot_store_a_forged_copy() {
    let (state, _publisher, published, _person, submission) = copied_submission();
    let forged = copy_of(&state, &published, &submission, 2, 1, b"FORGED");
    let real = copy_of(&state, &published, &submission, 3, 2, b"two");
    let stored = try_append(&state, &published, vec![forged, real]);
    assert_eq!(stored.as_ref().err(), Some(&LogError::Conflict));
    assert!(state
        .entries
        .iter()
        .all(|row| row.entry.payload != b"FORGED"));
}

#[test]
fn exact_incorporation_repeat_is_duplicate_and_a_changed_payload_is_not() {
    let (state, _publisher, published, _person, submission) = copied_submission();
    let again = copy_of(&state, &published, &submission, 2, 1, b"one");
    let repeat = try_append(&state, &published, vec![again]).expect("repeat");
    assert_eq!(
        repeat.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
    assert_eq!(repeat.state.entries.len(), state.entries.len());

    let forged = copy_of(&state, &published, &submission, 2, 1, b"FORGED");
    let changed = try_append(&state, &published, vec![forged]);
    assert_eq!(changed.as_ref().err(), Some(&LogError::Conflict));
}

#[test]
fn mixed_incorporated_and_new_entries_are_rejected() {
    let (state, _publisher, published, _person, submission) = copied_submission();
    let again = copy_of(&state, &published, &submission, 2, 1, b"one");
    let second = copy_of(&state, &published, &submission, 3, 2, b"two");
    let mixed = try_append(&state, &published, vec![again, second]);
    assert_eq!(mixed.as_ref().err(), Some(&LogError::MixedIncorporation));
    assert_eq!(state.entries.len(), 3);
}

#[test]
fn repeated_writer_seq_with_different_content_is_conflict() {
    let (state, _publisher, published) = shareable();
    let first = entry(&state.database_id, &published, "photos", 1, 1);
    let state = apply(&state, &published, vec![first], 1);
    let mut other = entry(&state.database_id, &published, "photos", 1, 1);
    other.payload = b"other".to_vec();
    let conflict = try_append(&state, &published, vec![other]);
    assert_eq!(conflict.as_ref().err(), Some(&LogError::Conflict));
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].entry.payload, vec![1]);
}

#[test]
fn pending_submission_survives_trim_and_can_be_reappended() {
    let (mut state, publisher, published, _person, submission) = copied_submission();
    state = ack(&state, &published, 3);
    state = ack(&state, &submission, 2);
    state = snapshot(&state, &publisher, 4);
    state = trim(&state, &publisher, 4);
    assert!(state
        .entries
        .iter()
        .any(|row| row.entry.writer == submission
            && row.entry.writer_seq == 2
            && row.entry.payload == b"two"));
    let again = copy_of(&state, &published, &submission, 2, 2, b"two");
    let stored = try_append(&state, &published, vec![again]).expect("reappend");
    assert!(matches!(
        stored.outcome,
        Outcome::Appended(AppendOutcome::Accepted { .. })
    ));
}

#[test]
fn trimmed_writer_seq_retry_is_duplicate() {
    let (state, publisher, published) = shareable();
    let first = entry(&state.database_id, &published, "photos", 1, 1);
    let mut state = apply(&state, &published, vec![first.clone()], 1);
    state = ack(&state, &published, 1);
    state = snapshot(&state, &publisher, 2);
    state = trim(&state, &publisher, 2);
    assert!(state.entries.is_empty());
    let repeat = try_append(&state, &published, vec![first]).expect("trimmed retry");
    assert_eq!(
        repeat.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
    let mut changed = entry(&state.database_id, &published, "photos", 1, 1);
    changed.payload = b"other".to_vec();
    let also = try_append(&state, &published, vec![changed]).expect("trimmed bytes are gone");
    assert_eq!(
        also.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
}

#[test]
fn trimmed_reappend_retry_is_duplicate() {
    let (mut state, publisher, published, _person, submission) = copied_submission();
    let stored = state
        .entries
        .iter()
        .find(|row| row.entry.writer == published)
        .expect("publisher entry")
        .entry
        .clone();
    state = ack(&state, &published, 3);
    state = ack(&state, &submission, 3);
    state = snapshot(&state, &publisher, 4);
    state = trim(&state, &publisher, 4 + MAX_RETENTION_MS);
    assert!(state.entries.is_empty());
    let repeat = try_append(&state, &published, vec![stored.clone()]).expect("exact retry");
    assert_eq!(
        repeat.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 2 })
    );
    let mut fresh = stored;
    fresh.writer_seq = 2;
    let refused = try_append(&state, &published, vec![fresh]);
    assert_eq!(refused.as_ref().err(), Some(&LogError::UnknownSubmission));
}

fn copied_submission() -> (LogState, PrincipalId, Writer, PrincipalId, Writer) {
    let (mut state, publisher, published) = shareable();
    let person = principal("contributor");
    let submission = writer_of(&person, "contrib");
    state = grant_submit(&state, &publisher, &person);
    let mut first = entry(&state.database_id, &submission, "photos", 1, 1);
    first.payload = b"one".to_vec();
    let mut second = entry(&state.database_id, &submission, "photos", 2, 2);
    second.payload = b"two".to_vec();
    state = apply(&state, &submission, vec![first, second], 2);
    let copied = copy_of(&state, &published, &submission, 1, 1, b"one");
    state = apply(&state, &published, vec![copied], 3);
    (state, publisher, published, person, submission)
}

fn copy_of(
    state: &LogState,
    publisher: &Writer,
    submission: &Writer,
    seq: u64,
    from_seq: u64,
    payload: &[u8],
) -> Entry {
    let mut entry = entry(&state.database_id, publisher, "photos", seq, seq);
    entry.payload = payload.to_vec();
    entry.incorporated_from = Some(Incorporation {
        writer: submission.clone(),
        writer_seq: from_seq,
    });
    entry
}

fn try_append(
    state: &LogState,
    writer: &Writer,
    entries: Vec<Entry>,
) -> Result<StepResult, LogError> {
    let begun = step(
        state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    step(
        &begun,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries,
            accepted_at_ms: 9,
        }),
        &hearth,
    )
}

fn snapshot(state: &LogState, caller: &PrincipalId, at_ms: u64) -> LogState {
    step(
        state,
        Command::Snapshot {
            caller: caller.clone(),
            at_ms,
            snapshot_ref: snap("review"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state
}

fn trim(state: &LogState, caller: &PrincipalId, now_ms: u64) -> LogState {
    step(
        state,
        Command::Trim {
            caller: caller.clone(),
            now_ms,
        },
        &hearth,
    )
    .expect("trim")
    .state
}
