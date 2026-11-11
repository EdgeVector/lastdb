use super::*;
use crate::SyncError;
use mutation_log::LogError;

#[test]
fn append_before_resume_is_refused() {
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
        device(phone.clone(), phone_stream),
    )
    .expect("state");
    let refused = drive(
        &state,
        &mut log,
        append_event("notes", b"early", 1),
        &hearth,
    );
    assert_eq!(refused, Err(SyncError::NeedsResume));
    assert!(log.head(&phone).expect("head").is_none());
}

#[test]
fn two_writers_on_one_stream_conflict_instead_of_dropping() {
    let (mut log, publisher, published) = share_log();
    let kind = share_kind(&publisher, &published);
    let open = || {
        SyncState::open(
            database("family"),
            kind.clone(),
            SyncRole::Publisher {
                principal: publisher.clone(),
            },
        )
        .expect("publisher")
    };
    let left = SyncState::resume(&open(), &log).expect("left");
    let left = append(&mut log, &left, append_event("photos", b"kept", 1));
    let right = SyncState::resume(&open(), &log).expect("right");
    assert_eq!(left.next_seq, 2);
    assert_eq!(right.next_seq, 2);
    let (_left, stored) = drive(
        &left,
        &mut log,
        append_event("photos", b"alpha", 2),
        &hearth,
    )
    .expect("first writer");
    assert!(matches!(stored, Outcome::Appended { .. }));
    let conflict = drive(
        &right,
        &mut log,
        append_event("photos", b"beta", 3),
        &hearth,
    );
    assert_eq!(conflict, Err(SyncError::Log(LogError::Conflict)));
    let (right, retry) = drive(
        &right,
        &mut log,
        append_event("photos", b"alpha", 2),
        &hearth,
    )
    .expect("identical retry");
    assert!(matches!(retry, Outcome::Duplicate { .. }));
    let (_right, seen) = pull(&mut log, &right, 4);
    assert_eq!(
        payloads_of(&seen),
        vec![b"kept".to_vec(), b"alpha".to_vec()]
    );
}

fn payloads_of(entries: &[mutation_log::StoredEntry]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .map(|stored| stored.entry.payload.clone())
        .collect()
}
