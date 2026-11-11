use super::*;

#[test]
fn acking_the_scan_cursor_keeps_a_young_submission() {
    let (mut state, publisher, published, receiver, submission) = with_receiver();
    let secret = pending(&state, &submission, 1, b"secret");
    state = apply(&state, &submission, vec![secret], 1);
    let page = state.list(&receiver, 0, 10, 2, &hearth).expect("scan");
    assert!(page.entries.is_empty());
    assert_eq!(page.next, 1);
    state = ack_all(&state, &published, &submission, &receiver, page.next);
    state = cut(&state, &publisher, 2);
    state = trim_log(&state, &publisher, 2);
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].entry.payload, b"secret");
    let copy = reappend(&state, &published, &submission, 1, 1, b"secret");
    let stored = append_entries(&state, &published, vec![copy]).expect("reappend");
    assert!(matches!(
        stored.outcome,
        Outcome::Appended(AppendOutcome::Accepted { .. })
    ));
}

#[test]
fn expired_submission_is_not_a_duplicate() {
    let (mut state, publisher, published, receiver, submission) = with_receiver();
    let secret = pending(&state, &submission, 1, b"secret");
    state = apply(&state, &submission, vec![secret.clone()], 0);
    state = ack_all(&state, &published, &submission, &receiver, 1);
    state = cut(&state, &publisher, 1);
    state = trim_log(&state, &publisher, MAX_RETENTION_MS);
    assert!(state.entries.is_empty());
    let retry = append_entries(&state, &submission, vec![secret]);
    assert_eq!(retry.err(), Some(expired(&submission, 1)));
    let copy = reappend(&state, &published, &submission, 1, 1, b"secret");
    let again = append_entries(&state, &published, vec![copy]);
    assert_eq!(again.err(), Some(expired(&submission, 1)));
}

#[test]
fn a_pending_submission_does_not_pin_its_neighbors() {
    let (mut state, publisher, published, _person, submission) = contributors();
    let first = official(&state, &published, 1, b"one");
    let secret = pending(&state, &submission, 1, b"secret");
    let third = official(&state, &published, 2, b"three");
    state = apply(&state, &published, vec![first], 1);
    state = apply(&state, &submission, vec![secret], 1);
    state = apply(&state, &published, vec![third], 1);
    state = ack(&state, &published, 3);
    state = ack(&state, &submission, 3);
    state = cut(&state, &publisher, 2);
    state = trim_log(&state, &publisher, 2);
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries[0].delivery_seq, 2);
    assert_eq!(state.entries[0].entry.payload, b"secret");
    assert_eq!(state.trimmed_through, 3);
    let receiver = principal("receiver");
    state = grant_receive(&state, &publisher, &receiver);
    let head = state
        .head_for(&receiver, &hearth)
        .expect("head")
        .expect("log head");
    assert_eq!(
        head.cursor,
        state.head.as_ref().expect("log head").delivery_seq
    );
    assert!(head.stamp.is_none());
    assert!(state.list(&receiver, head.cursor, 10, 2, &hearth).is_ok());
    assert!(state
        .ack(&writer_of(&receiver, "phone"), head.cursor)
        .is_ok());
}

#[test]
fn a_rejected_submission_can_be_trimmed() {
    let (mut state, publisher, published, _person, submission) = contributors();
    let secret = pending(&state, &submission, 1, b"secret");
    state = apply(&state, &submission, vec![secret.clone()], 1);
    state = step(
        &state,
        Command::Reject {
            caller: publisher.clone(),
            writer: submission.clone(),
            writer_seq: 1,
        },
        &hearth,
    )
    .expect("reject")
    .state;
    state = ack(&state, &published, 1);
    state = ack(&state, &submission, 1);
    state = cut(&state, &publisher, 2);
    state = trim_log(&state, &publisher, 2);
    assert!(state.entries.is_empty());
    let retry = append_entries(&state, &submission, vec![secret]);
    assert_eq!(retry.err(), Some(rejected(&submission, 1)));
    let copy = reappend(&state, &published, &submission, 1, 1, b"secret");
    let again = append_entries(&state, &published, vec![copy]);
    assert_eq!(again.err(), Some(rejected(&submission, 1)));
}

#[test]
fn marks_leave_after_max_retention() {
    let (mut state, publisher, published, _person, submission) = contributors();
    let secret = pending(&state, &submission, 1, b"secret");
    state = apply(&state, &submission, vec![secret], 0);
    state = step(
        &state,
        Command::Reject {
            caller: publisher.clone(),
            writer: submission.clone(),
            writer_seq: 1,
        },
        &hearth,
    )
    .expect("reject")
    .state;
    state = ack(&state, &published, 1);
    state = ack(&state, &submission, 1);
    state = cut(&state, &publisher, 1);
    state = trim_log(&state, &publisher, 1);
    assert!(state.rejected.contains_key(&(submission, 1)));
    state = trim_log(&state, &publisher, MAX_RETENTION_MS);
    assert!(state.rejected.is_empty());

    let (mut aged, publisher, published, _person, submission) = contributors();
    let secret = pending(&aged, &submission, 1, b"secret");
    aged = apply(&aged, &submission, vec![secret], 0);
    aged = ack(&aged, &published, 1);
    aged = ack(&aged, &submission, 1);
    aged = cut(&aged, &publisher, 1);
    aged = trim_log(&aged, &publisher, MAX_RETENTION_MS);
    assert!(aged.expired_submissions.contains_key(&(submission, 1)));
    aged = trim_log(&aged, &publisher, MAX_RETENTION_MS + MAX_RETENTION_MS);
    assert!(aged.expired_submissions.is_empty());
}

fn contributors() -> (LogState, PrincipalId, Writer, PrincipalId, Writer) {
    let (state, publisher, published) = shareable();
    let person = principal("contributor");
    let submission = writer_of(&person, "contrib");
    let state = grant_submit(&state, &publisher, &person);
    (state, publisher, published, person, submission)
}

fn with_receiver() -> (LogState, PrincipalId, Writer, PrincipalId, Writer) {
    let (state, publisher, published, _person, submission) = contributors();
    let receiver = principal("receiver");
    let state = grant_receive(&state, &publisher, &receiver);
    (state, publisher, published, receiver, submission)
}

fn ack_all(
    state: &LogState,
    published: &Writer,
    submission: &Writer,
    receiver: &PrincipalId,
    through: u64,
) -> LogState {
    let state = ack(state, published, through);
    let state = ack(&state, submission, through);
    ack(&state, &writer_of(receiver, "phone"), through)
}

fn expired(writer: &Writer, writer_seq: u64) -> LogError {
    LogError::SubmissionExpired {
        writer: writer.clone(),
        writer_seq,
    }
}

fn rejected(writer: &Writer, writer_seq: u64) -> LogError {
    LogError::SubmissionRejected {
        writer: writer.clone(),
        writer_seq,
    }
}

fn pending(state: &LogState, writer: &Writer, seq: u64, payload: &[u8]) -> Entry {
    let mut entry = entry(&state.database_id, writer, "photos", seq, seq);
    entry.payload = payload.to_vec();
    entry
}

fn official(state: &LogState, writer: &Writer, seq: u64, payload: &[u8]) -> Entry {
    pending(state, writer, seq, payload)
}

fn reappend(
    state: &LogState,
    publisher: &Writer,
    submission: &Writer,
    seq: u64,
    from_seq: u64,
    payload: &[u8],
) -> Entry {
    let mut entry = official(state, publisher, seq, payload);
    entry.incorporated_from = Some(Incorporation {
        writer: submission.clone(),
        writer_seq: from_seq,
    });
    entry
}

fn cut(state: &LogState, caller: &PrincipalId, at_ms: u64) -> LogState {
    step(
        state,
        Command::Snapshot {
            caller: caller.clone(),
            at_ms,
            snapshot_ref: snap("pending"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state
}

fn trim_log(state: &LogState, caller: &PrincipalId, now_ms: u64) -> LogState {
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

fn append_entries(
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
