use super::*;
use crate::SyncError;
use mutation_log::LogError;

#[test]
fn append_before_resume_is_refused() {
    let phone = principal("phone");
    let mut log = personal_log();
    log.grant(&phone, phone.clone(), Capability::Owner)
        .expect("grant");
    let state = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        owner_role(phone.clone(), "phone"),
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
fn same_writer_conflict_keeps_the_first_payload() {
    let (mut log, publisher) = share_log();
    let kind = share_kind(&publisher);
    let open = || {
        SyncState::open(
            database("family"),
            kind.clone(),
            publisher_role(publisher.clone(), "phone"),
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
    assert_eq!(payloads(&seen), vec![b"kept".to_vec(), b"alpha".to_vec()]);
}

#[test]
fn two_devices_of_one_key_append_without_conflict() {
    let owner = principal("owner");
    let mut log = personal_log();
    log.grant(&owner, owner.clone(), Capability::Owner)
        .expect("owner");
    let phone = SyncState::resume(
        &SyncState::open(
            database("home"),
            DatabaseKind::Personal,
            owner_role(owner.clone(), "phone"),
        )
        .expect("phone"),
        &log,
    )
    .expect("resume phone");
    let laptop = SyncState::resume(
        &SyncState::open(
            database("home"),
            DatabaseKind::Personal,
            owner_role(owner.clone(), "laptop"),
        )
        .expect("laptop"),
        &log,
    )
    .expect("resume laptop");
    let (phone, phone_written) = drive(
        &phone,
        &mut log,
        append_event("notes", b"phone", 1),
        &hearth,
    )
    .expect("phone");
    let (_laptop, laptop_written) = drive(
        &laptop,
        &mut log,
        append_event("notes", b"laptop", 2),
        &hearth,
    )
    .expect("laptop");
    assert!(matches!(phone_written, Outcome::Appended { .. }));
    assert!(matches!(laptop_written, Outcome::Appended { .. }));
    let (_phone, seen) = pull(&mut log, &phone, 3);
    assert_eq!(payloads(&seen), vec![b"phone".to_vec(), b"laptop".to_vec()]);
}
