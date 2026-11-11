use super::*;
use crate::SyncError;
use mutation_log::{
    step, AppendBatch, Command, Entry, LogError, LogState, Writer, MAX_RETENTION_MS,
};

#[test]
fn lost_submit_grant_keeps_the_delivery_cursor() {
    let (mut log, publisher) = share_log();
    let person = principal("both");
    log.grant(&publisher, person.clone(), Capability::Receive)
        .expect("receive");
    log.grant(&publisher, person.clone(), Capability::Submit)
        .expect("submit");
    let kind = share_kind(&publisher);
    let publisher_state = SyncState::open(
        database("family"),
        kind.clone(),
        publisher_role(publisher.clone(), "phone"),
    )
    .expect("publisher");
    let mut reader = SyncState::open(
        database("family"),
        kind,
        contributor_role(person.clone(), "phone"),
    )
    .expect("reader");
    let publisher_state = append(
        &mut log,
        &publisher_state,
        append_event("photos", b"album", 1),
    );
    reader = append(&mut log, &reader, append_event("photos", b"pending", 2));
    let _publisher_state = append(
        &mut log,
        &publisher_state,
        append_event("photos", b"second", 3),
    );
    let (reader, _) = pull(&mut log, &reader, 4);
    let saved = reader.cursor;
    log.revoke(&publisher, &person, &Capability::Submit)
        .expect("revoke submit");
    let (reader, seen) = pull(&mut log, &reader, 5);
    assert_eq!(reader.cursor, saved);
    assert!(seen.is_empty());
    let fresh = log.list(&person, 0, 90, 5).expect("fresh read");
    assert_eq!(
        payloads(&fresh.entries),
        vec![b"album".to_vec(), b"second".to_vec()]
    );
    let resumed = SyncState::resume(&reader, &log);
    assert_eq!(resumed, Err(SyncError::Log(LogError::Unauthorized)));
}

#[test]
fn resumed_writer_continues_after_the_stored_tip() {
    let phone = principal("phone");
    let mut log = personal_log();
    log.grant(&phone, phone.clone(), Capability::Owner)
        .expect("grant");
    let opened = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        owner_role(phone.clone(), "phone"),
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
    let rejoined = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        owner_role(phone, "phone"),
    )
    .expect("rejoin");
    assert_eq!(rejoined.next_seq, 1);
    let resumed = SyncState::resume(&rejoined, &log).expect("resume");
    assert_eq!(resumed.next_seq, 2);
    let (next, created) = drive(
        &resumed,
        &mut log,
        append_event("notes", b"after", 2),
        &hearth,
    )
    .expect("continued");
    assert!(matches!(created, Outcome::Appended { .. }));
    let (_next, seen) = pull(&mut log, &next, 3);
    assert_eq!(payloads(&seen), vec![b"landed".to_vec(), b"after".to_vec()]);
}

#[test]
fn expired_cursor_restarts_from_the_snapshot() {
    let publisher = principal("publisher");
    let published = writer(&publisher, "phone");
    let receiver = principal("receiver");
    let mut state = LogState::open(database("family"), share_kind(&publisher));
    state = step(
        &state,
        Command::Grant {
            caller: publisher.clone(),
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("grant")
    .state;
    let old = mutation_log::Entry {
        database_id: database("family"),
        writer: published.clone(),
        writer_seq: 1,
        authored_at_ms: 1,
        namespace: namespace("photos"),
        incorporated_from: None,
        payload: b"old".to_vec(),
    };
    state = append_batch(&state, &published, vec![old], 0);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 1,
            snapshot_ref: mutation_log::SnapshotRef::new("family-snap").expect("ref"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    let covered: Vec<Vec<u8>> = state
        .entries
        .iter()
        .map(|stored| stored.entry.payload.clone())
        .collect();
    assert_eq!(covered, vec![b"old".to_vec()]);
    let fresh = mutation_log::Entry {
        database_id: database("family"),
        writer: published.clone(),
        writer_seq: 2,
        authored_at_ms: 2,
        namespace: namespace("photos"),
        incorporated_from: None,
        payload: b"fresh".to_vec(),
    };
    state = append_batch(&state, &published, vec![fresh], MAX_RETENTION_MS);
    state = step(
        &state,
        Command::Trim {
            caller: publisher.clone(),
            now_ms: MAX_RETENTION_MS,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    assert_eq!(state.entries.len(), 1);
    let mut log = open_log(state);
    let reader = SyncState::open(
        database("family"),
        share_kind(&publisher),
        SyncRole::Receiver {
            principal: receiver,
            device: device("reader"),
        },
    )
    .expect("receiver");
    let (reader, outcome) = drive(
        &reader,
        &mut log,
        SyncEvent::Pull {
            now_ms: MAX_RETENTION_MS,
        },
        &hearth,
    )
    .expect("pull");
    match outcome {
        Outcome::Pulled {
            entries,
            cursor,
            reset,
        } => {
            assert_eq!(payloads(&entries), vec![b"fresh".to_vec()]);
            let snapshot = reset.expect("snapshot covers the trimmed entry");
            assert_eq!(snapshot.through, 1);
            assert_eq!(snapshot.snapshot_ref.as_str(), "family-snap");
            assert_eq!(reader.cursor, cursor);
            assert!(cursor > snapshot.through);
        }
        Outcome::Appended { .. } | Outcome::Duplicate { .. } => panic!("pull stored an entry"),
    }
}

fn append_batch(
    state: &LogState,
    writer: &Writer,
    entries: Vec<Entry>,
    accepted_at_ms: u64,
) -> LogState {
    let begun = step(
        state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let appended = step(
        &begun,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries,
            accepted_at_ms,
        }),
        &hearth,
    )
    .expect("append")
    .state;
    step(
        &appended,
        Command::FinishAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state
}
