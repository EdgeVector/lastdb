use super::*;
use crate::SyncError;
use mutation_log::LogError;

#[test]
fn personal_rejects_a_receiver_and_shareable_rejects_an_owner() {
    let owner = principal("owner");
    let personal = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        SyncRole::Receiver { principal: owner },
    );
    assert_eq!(
        personal,
        Err(SyncError::Log(LogError::PersonalNotShareable))
    );

    let publisher = principal("publisher");
    let shareable = SyncState::open(
        database("family"),
        share_kind(&publisher),
        owner_role(principal("phone"), "phone"),
    );
    assert_eq!(
        shareable,
        Err(SyncError::Log(LogError::DeviceGrantNotShareable))
    );
}

#[test]
fn a_receiver_cannot_append() {
    let (mut log, publisher) = share_log();
    let receiver = principal("receiver");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("grant");
    let state = SyncState::open(
        database("family"),
        share_kind(&publisher),
        SyncRole::Receiver {
            principal: receiver,
        },
    )
    .expect("receiver");
    let refused = drive(
        &state,
        &mut log,
        append_event("photos", b"nope", 1),
        &hearth,
    );
    assert_eq!(refused, Err(SyncError::ReadOnly));
    assert!(log.head(&publisher).expect("head").is_none());
}
