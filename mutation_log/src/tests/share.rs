use super::*;

fn granted_share() -> (LogState, PrincipalId, Writer, PrincipalId, Writer) {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let other = principal("other");
    state = grant_receive(&state, &publisher, &receiver);
    state = grant_submit(&state, &publisher, &contributor);
    state = grant_submit(&state, &publisher, &other);
    let submission = writer_of(&contributor, "contrib");
    (state, publisher, published, receiver, submission)
}

#[test]
fn receiver_cannot_read_another_contributors_submission() {
    let (state, _publisher, _published, receiver, submission) = granted_share();
    let other = principal("other");
    let submitted = entry(&state.database_id, &submission, "photos", 1, 5);
    let state = apply(&state, &submission, vec![submitted.clone()], 5);
    assert!(!can_read(&receiver, &state, &submitted, &hearth));
    assert!(!can_read(&other, &state, &submitted, &hearth));
    assert!(can_read(&submission.key, &state, &submitted, &hearth));
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());
    assert!(filter_readable(&state.entries, &other, &state, &hearth).is_empty());
    assert_eq!(
        filter_readable(&state.entries, &submission.key, &state, &hearth).len(),
        1
    );
}

#[test]
fn receiver_cannot_read_another_key_or_database() {
    let (state, _publisher, published, receiver, _submission) = granted_share();
    let other = principal("other");
    let other_writer = writer_of(&other, "other");
    let own = entry(&state.database_id, &other_writer, "photos", 1, 4);
    let state = apply(&state, &other_writer, vec![own], 4);
    let published_entry = entry(&state.database_id, &published, "photos", 1, 6);
    let state = apply(&state, &published, vec![published_entry], 6);

    let mut mixed = state.entries.clone();
    let foreign_db = database("personal");
    let mut foreign = entry(&foreign_db, &published, "photos", 1, 7);
    foreign.database_id = foreign_db;
    mixed.push(StoredEntry {
        delivery_seq: 99,
        accepted_at_ms: 7,
        entry: foreign,
    });
    let visible = filter_readable(&mixed, &receiver, &state, &hearth);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.database_id, state.database_id);
    assert_eq!(visible[0].entry.writer.key, published.key);
    assert!(!can_read(
        &receiver,
        &state,
        &state.entries[0].entry,
        &hearth
    ));
    assert!(can_read(
        &receiver,
        &state,
        &state.entries[1].entry,
        &hearth
    ));
}

#[test]
fn receiver_sees_a_contribution_only_after_the_publisher_reappends_it() {
    let (state, publisher, published, receiver, submission) = granted_share();
    let submitted = entry(&state.database_id, &submission, "photos", 1, 8);
    let state = apply(&state, &submission, vec![submitted], 8);
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());

    let mut incorporated = entry(&state.database_id, &published, "photos", 1, 9);
    incorporated.payload = vec![1];
    incorporated.incorporated_from = Some(Incorporation {
        writer: submission.clone(),
        writer_seq: 1,
    });
    let state = apply(&state, &published, vec![incorporated], 9);
    let visible = filter_readable(&state.entries, &receiver, &state, &hearth);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.writer.key, publisher);
    assert_eq!(
        visible[0]
            .entry
            .incorporated_from
            .as_ref()
            .map(|from| &from.writer.key),
        Some(&submission.key)
    );
}

#[test]
fn a_submit_grant_does_not_publish() {
    let (state, _publisher, published, receiver, submission) = granted_share();
    let pending = entry(&state.database_id, &submission, "photos", 1, 1);
    let state = apply(&state, &submission, vec![pending], 1);
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());
    let mut forged = entry(&state.database_id, &submission, "photos", 2, 2);
    forged.incorporated_from = Some(Incorporation {
        writer: published,
        writer_seq: 1,
    });
    let opened = step(
        &state,
        Command::BeginAppend {
            writer: submission.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            writer: submission,
            entries: vec![forged],
            accepted_at_ms: 2,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::InvalidEntry));
}

#[test]
fn reappend_from_another_publisher_device_is_duplicate() {
    let (mut state, publisher, phone, _receiver, submission) = granted_share();
    let laptop = writer_of(&publisher, "laptop");
    let pending = entry(&state.database_id, &submission, "photos", 1, 1);
    pending_payload(&mut state, &submission, pending);
    let mut copied = entry(&state.database_id, &phone, "photos", 1, 2);
    copied.payload = vec![1];
    copied.incorporated_from = Some(Incorporation {
        writer: submission.clone(),
        writer_seq: 1,
    });
    state = apply(&state, &phone, vec![copied.clone()], 2);
    copied.writer = laptop.clone();
    let begun = step(
        &state,
        Command::BeginAppend {
            writer: laptop.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let repeat = step(
        &begun,
        Command::Append(AppendBatch {
            writer: laptop,
            entries: vec![copied],
            accepted_at_ms: 3,
        }),
        &hearth,
    )
    .expect("second device");
    assert_eq!(
        repeat.outcome,
        Outcome::Appended(AppendOutcome::Duplicate { next_seq: 1 })
    );
    assert_eq!(repeat.state.entries.len(), state.entries.len());
    assert_eq!(repeat.state.incorporated.len(), 1);
}

fn pending_payload(state: &mut LogState, writer: &Writer, entry: Entry) {
    *state = apply(state, writer, vec![entry], 1);
}
