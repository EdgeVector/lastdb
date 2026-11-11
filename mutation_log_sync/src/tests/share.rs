use super::*;
use crate::{SyncError, SyncEvent};
use mutation_log::LogError;

#[test]
fn share_round_trip_hides_a_submission_until_the_publisher_reappends() {
    let (mut log, publisher) = share_log();
    let receiver = principal("receiver");
    let other = principal("other");
    let contributor = principal("contributor");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("receiver");
    log.grant(&publisher, other.clone(), Capability::Receive)
        .expect("other");
    log.grant(&publisher, contributor.clone(), Capability::Submit)
        .expect("contributor");
    let kind = share_kind(&publisher);
    let publisher_state = SyncState::open(
        database("family"),
        kind.clone(),
        publisher_role(publisher.clone(), "phone"),
    )
    .expect("publisher");
    let receiver_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Receiver {
            principal: receiver,
        },
    )
    .expect("receiver");
    let other_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Receiver { principal: other },
    )
    .expect("other");
    let contributor_state = SyncState::open(
        database("family"),
        kind,
        contributor_role(contributor.clone(), "phone"),
    )
    .expect("contributor");

    let publisher_state = append(
        &mut log,
        &publisher_state,
        append_event("photos", b"album", 1),
    );
    let (_receiver_state, seen) = pull(&mut log, &receiver_state, 2);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].entry.writer.key, publisher);

    let _contributor_state = append(
        &mut log,
        &contributor_state,
        append_event("photos", b"pending", 3),
    );
    let (other_state, hidden) = pull(&mut log, &other_state, 4);
    assert_eq!(hidden.len(), 1);
    assert!(hidden
        .iter()
        .all(|stored| stored.entry.writer.key == publisher));

    let (publisher_state, inbox) = pull(&mut log, &publisher_state, 5);
    let pending = submission_entry(&inbox, &contributor);
    let (_publisher_state, copied) = drive(
        &publisher_state,
        &mut log,
        SyncEvent::Reappend {
            entry: pending,
            accepted_at_ms: 6,
        },
        &hearth,
    )
    .expect("reappend");
    assert!(matches!(copied, Outcome::Appended { .. }));

    let (_other_state, visible) = pull(&mut log, &other_state, 7);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.writer.key, publisher);
    assert_eq!(visible[0].entry.payload, b"pending");
    assert!(visible[0].entry.incorporated_from.is_some());
    assert!(visible
        .iter()
        .all(|stored| stored.entry.writer.key != contributor));
}

#[test]
fn reappend_from_another_publisher_device_is_duplicate() {
    let (mut log, publisher) = share_log();
    let contributor = principal("contributor");
    let receiver = principal("receiver");
    log.grant(&publisher, contributor.clone(), Capability::Submit)
        .expect("submit");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("receive");
    let kind = share_kind(&publisher);
    let contributor_state = SyncState::open(
        database("family"),
        kind.clone(),
        contributor_role(contributor.clone(), "phone"),
    )
    .expect("contributor");
    let phone = SyncState::open(
        database("family"),
        kind.clone(),
        publisher_role(publisher.clone(), "phone"),
    )
    .expect("phone");
    let laptop = SyncState::open(
        database("family"),
        kind.clone(),
        publisher_role(publisher.clone(), "laptop"),
    )
    .expect("laptop");
    let receiver_state = SyncState::open(
        database("family"),
        kind,
        SyncRole::Receiver {
            principal: receiver,
        },
    )
    .expect("receiver");
    let _contributor_state = append(
        &mut log,
        &contributor_state,
        append_event("photos", b"pending", 1),
    );
    let (phone, inbox) = pull(&mut log, &phone, 2);
    let phone = SyncState::resume(&phone, &log).expect("phone resume");
    let pending = submission_entry(&inbox, &contributor);
    let (phone, first) = drive(
        &phone,
        &mut log,
        SyncEvent::Reappend {
            entry: pending.clone(),
            accepted_at_ms: 3,
        },
        &hearth,
    )
    .expect("phone copies");
    assert!(matches!(first, Outcome::Appended { .. }));
    let laptop = SyncState::resume(&laptop, &log).expect("laptop resume");
    let (_laptop, second) = drive(
        &laptop,
        &mut log,
        SyncEvent::Reappend {
            entry: pending,
            accepted_at_ms: 4,
        },
        &hearth,
    )
    .expect("laptop retry");
    assert!(matches!(second, Outcome::Duplicate { .. }));
    let (_receiver, seen) = pull(&mut log, &receiver_state, 5);
    assert_eq!(copies_of(&seen, b"pending"), 1);
    let _ = phone;
}

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (mut log, publisher) = share_log();
    let opened = SyncState::open(
        database("family"),
        share_kind(&publisher),
        publisher_role(publisher.clone(), "phone"),
    )
    .expect("publisher");
    let state = SyncState::resume(&opened, &log).expect("resume");
    let refused = drive(
        &state,
        &mut log,
        append_event("hearth", b"private", 1),
        &hearth,
    );
    assert_eq!(refused, Err(SyncError::Log(LogError::AppPrivateNamespace)));
    assert!(log.head(&publisher).expect("head").is_none());
}

fn submission_entry(entries: &[StoredEntry], key: &PrincipalId) -> mutation_log::Entry {
    entries
        .iter()
        .find(|stored| &stored.entry.writer.key == key && stored.entry.incorporated_from.is_none())
        .expect("submission")
        .entry
        .clone()
}

fn copies_of(entries: &[StoredEntry], payload: &[u8]) -> usize {
    entries
        .iter()
        .filter(|stored| stored.entry.payload == payload)
        .count()
}
