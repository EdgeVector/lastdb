use super::retry::payloads;
use super::*;

#[test]
fn lost_submit_grant_restarts_the_cursor_in_the_publisher_space() {
    let (mut log, publisher, published) = share_log();
    let person = principal("both");
    let submission = stream("both");
    log.grant(&publisher, person.clone(), Capability::Receive)
        .expect("receive");
    log.grant(
        &publisher,
        person.clone(),
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
    let mut reader = SyncState::open(
        database("family"),
        kind,
        SyncRole::Contributor {
            principal: person.clone(),
            stream: submission,
        },
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
    assert!(matches!(reader.cursor, crate::ReadCursor::Delivery(_)));
    log.revoke(
        &publisher,
        &person,
        &Capability::Submit {
            stream: stream("both"),
        },
    )
    .expect("revoke submit");
    let (reader, seen) = pull(&mut log, &reader, 5);
    assert!(matches!(
        reader.cursor,
        crate::ReadCursor::Stream { stream, .. } if stream == published
    ));
    assert_eq!(payloads(&seen), vec![b"album".to_vec(), b"second".to_vec()]);
}

#[test]
fn resumed_writer_continues_after_the_stored_tip() {
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
    let state = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        device(phone.clone(), phone_stream.clone()),
    )
    .expect("state");
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
        device(phone, phone_stream),
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
