use super::*;
use crate::{SyncError, SyncEvent};
use mutation_log::LogError;

#[test]
fn share_round_trip_hides_a_submission_until_the_publisher_reappends() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    let other = principal("other");
    let contributor = principal("contributor");
    let submission = stream("contrib");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("receiver");
    log.grant(&publisher, other.clone(), Capability::Receive)
        .expect("other");
    log.grant(
        &publisher,
        contributor.clone(),
        Capability::Submit {
            stream: submission.clone(),
        },
    )
    .expect("contributor");
    let kind = share_kind(&publisher, &published);
    let publisher_state = SyncState::open(
        database("family"),
        kind.clone(),
        SyncRole::Publisher {
            principal: publisher.clone(),
        },
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
    let (_receiver_state, seen) = pull(&mut log, &receiver_state, 2);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].entry.stream, published);

    let _contributor_state = append(
        &mut log,
        &contributor_state,
        append_event("photos", b"pending", 3),
    );
    let (other_state, hidden) = pull(&mut log, &other_state, 4);
    assert_eq!(hidden.len(), 1);
    assert!(hidden.iter().all(|stored| stored.entry.stream == published));

    let (publisher_state, inbox) = pull(&mut log, &publisher_state, 5);
    let pending = inbox
        .iter()
        .find(|stored| stored.entry.stream == submission)
        .expect("publisher reads the submission")
        .entry
        .clone();
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
    assert_eq!(visible[0].entry.stream, published);
    assert_eq!(visible[0].entry.payload, b"pending");
    assert!(visible[0].entry.incorporated_from.is_some());
    assert!(visible
        .iter()
        .all(|stored| stored.entry.stream != submission));
}

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (mut log, publisher, published) = share_log();
    let state = SyncState::open(
        database("family"),
        share_kind(&publisher, &published),
        SyncRole::Publisher {
            principal: publisher.clone(),
        },
    )
    .expect("publisher");
    let refused = drive(
        &state,
        &mut log,
        append_event("hearth", b"private", 1),
        &hearth,
    );
    assert_eq!(refused, Err(SyncError::Log(LogError::AppPrivateNamespace)));
    assert!(log.head(&publisher).expect("head").is_none());
}
