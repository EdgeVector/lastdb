use super::*;
use crate::SyncEvent;

#[test]
fn lost_append_response_retry_then_a_new_mutation_lands() {
    let phone = principal("phone");
    let phone_stream = stream("phone");
    let mut log = personal_log();
    log.grant(
        &phone,
        phone.clone(),
        Capability::Device {
            stream: phone_stream.clone(),
        },
    )
    .expect("grant");
    let opened = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        device(phone, phone_stream),
    )
    .expect("state");
    let state = SyncState::resume(&opened, &log).expect("resume");
    drive(
        &state,
        &mut log,
        append_event("notes", b"landed", 1),
        &hearth,
    )
    .expect("stored");
    let (retried, retry) = drive(
        &state,
        &mut log,
        append_event("notes", b"landed", 1),
        &hearth,
    )
    .expect("retry");
    assert!(matches!(retry, Outcome::Duplicate { .. }));
    let (next, created) = drive(
        &retried,
        &mut log,
        append_event("notes", b"after", 2),
        &hearth,
    )
    .expect("new mutation");
    assert!(matches!(created, Outcome::Appended { .. }));
    let (_next, seen) = pull(&mut log, &next, 3);
    assert_eq!(payloads(&seen), vec![b"landed".to_vec(), b"after".to_vec()]);
}

#[test]
fn lost_reappend_response_does_not_drop_the_next_contribution() {
    let (mut log, publisher, published) = share_log();
    let contributor = principal("contributor");
    let submission = stream("contrib");
    log.grant(
        &publisher,
        contributor.clone(),
        Capability::Submit {
            stream: submission.clone(),
        },
    )
    .expect("submit");
    let kind = share_kind(&publisher, &published);
    let publisher_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Publisher {
            principal: publisher.clone(),
        },
    )
    .expect("publisher");
    let contributor_state = SyncState::open(
        database("family"),
        kind,
        SyncRole::Contributor {
            principal: contributor,
            stream: submission.clone(),
        },
    )
    .expect("contributor");
    let publisher_state = append(
        &mut log,
        &publisher_state,
        append_event("photos", b"album", 1),
    );
    let _contributor_state = append(
        &mut log,
        &contributor_state,
        append_event("photos", b"pending", 2),
    );
    let (publisher_state, inbox) = pull(&mut log, &publisher_state, 3);
    let pending = submission_entry(&inbox, &submission);
    drive(
        &publisher_state,
        &mut log,
        SyncEvent::Reappend {
            entry: pending.clone(),
            accepted_at_ms: 4,
        },
        &hearth,
    )
    .expect("stored reappend");
    let (retried, retry) = drive(
        &publisher_state,
        &mut log,
        SyncEvent::Reappend {
            entry: pending,
            accepted_at_ms: 4,
        },
        &hearth,
    )
    .expect("retry reappend");
    assert!(matches!(retry, Outcome::Duplicate { .. }));
    let (_next, created) = drive(
        &retried,
        &mut log,
        append_event("photos", b"later", 5),
        &hearth,
    )
    .expect("later contribution");
    assert!(matches!(created, Outcome::Appended { .. }));
    let receiver = principal("receiver");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("receive");
    let receiver_state = SyncState::open(
        database("family"),
        share_kind(&publisher, &published),
        SyncRole::Receiver {
            principal: receiver,
        },
    )
    .expect("receiver");
    let (_receiver, seen) = pull(&mut log, &receiver_state, 6);
    assert_eq!(
        payloads(&seen),
        vec![b"album".to_vec(), b"pending".to_vec(), b"later".to_vec()]
    );
    assert_eq!(copies_of(&seen, b"pending"), 1);
}

#[test]
fn retried_reappend_publishes_one_copy() {
    let (mut log, publisher, published) = share_log();
    let contributor = principal("contributor");
    let submission = stream("contrib");
    let receiver = principal("receiver");
    log.grant(
        &publisher,
        contributor.clone(),
        Capability::Submit {
            stream: submission.clone(),
        },
    )
    .expect("submit");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("receive");
    let kind = share_kind(&publisher, &published);
    let publisher_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Publisher {
            principal: publisher.clone(),
        },
    )
    .expect("publisher");
    let contributor_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Contributor {
            principal: contributor,
            stream: submission.clone(),
        },
    )
    .expect("contributor");
    let receiver_state = SyncState::open(
        database("family"),
        kind,
        SyncRole::Receiver {
            principal: receiver,
        },
    )
    .expect("receiver");
    let publisher_state = append(
        &mut log,
        &publisher_state,
        append_event("photos", b"album", 1),
    );
    let _contributor_state = append(
        &mut log,
        &contributor_state,
        append_event("photos", b"pending", 2),
    );
    let (publisher_state, inbox) = pull(&mut log, &publisher_state, 3);
    let pending = submission_entry(&inbox, &submission);
    let (publisher_state, first) = drive(
        &publisher_state,
        &mut log,
        SyncEvent::Reappend {
            entry: pending.clone(),
            accepted_at_ms: 4,
        },
        &hearth,
    )
    .expect("first reappend");
    assert!(matches!(first, Outcome::Appended { .. }));
    let (publisher_state, retry) = drive(
        &publisher_state,
        &mut log,
        SyncEvent::Reappend {
            entry: pending,
            accepted_at_ms: 5,
        },
        &hearth,
    )
    .expect("retry after the sequence advanced");
    assert!(matches!(retry, Outcome::Duplicate { .. }));
    let (_publisher_state, created) = drive(
        &publisher_state,
        &mut log,
        append_event("photos", b"later", 6),
        &hearth,
    )
    .expect("mutation after the idempotent retry");
    assert!(matches!(created, Outcome::Appended { .. }));
    let (_receiver, seen) = pull(&mut log, &receiver_state, 7);
    assert_eq!(copies_of(&seen, b"pending"), 1);
    assert!(payloads(&seen).contains(&b"later".to_vec()));
}

#[test]
fn rolled_back_pull_is_not_stuck_on_ack_regress() {
    let phone = principal("phone");
    let phone_stream = stream("phone");
    let mut log = personal_log();
    log.grant(
        &phone,
        phone.clone(),
        Capability::Device {
            stream: phone_stream.clone(),
        },
    )
    .expect("grant");
    let mut writer = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        device(phone.clone(), phone_stream),
    )
    .expect("state");
    for index in 0..91 {
        writer = append(
            &mut log,
            &writer,
            append_event("notes", &[index], index as u64),
        );
    }
    let cursor_zero = writer;
    let (page, _) = pull(&mut log, &cursor_zero, 1);
    let (_tip, _) = pull(&mut log, &page, 2);
    let rolled = drive(
        &cursor_zero,
        &mut log,
        SyncEvent::Pull { now_ms: 3 },
        &hearth,
    );
    assert!(rolled.is_ok());
}

fn submission_entry(entries: &[StoredEntry], stream: &StreamId) -> mutation_log::Entry {
    entries
        .iter()
        .find(|stored| &stored.entry.stream == stream)
        .expect("submission")
        .entry
        .clone()
}

pub(super) fn payloads(entries: &[StoredEntry]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .map(|stored| stored.entry.payload.clone())
        .collect()
}

fn copies_of(entries: &[StoredEntry], payload: &[u8]) -> usize {
    entries
        .iter()
        .filter(|stored| stored.entry.payload == payload)
        .count()
}
