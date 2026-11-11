use super::*;
use crate::SyncError;
use mutation_log::LogError;

#[test]
fn personal_rejects_a_receiver_and_shareable_rejects_a_device() {
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
    let published = stream("publisher");
    let shareable = SyncState::open(
        database("family"),
        share_kind(&publisher, &published),
        device(principal("phone"), stream("phone")),
    );
    assert_eq!(
        shareable,
        Err(SyncError::Log(LogError::DeviceGrantNotShareable))
    );
}

#[test]
fn a_receiver_cannot_append() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .expect("grant");
    let state = SyncState::open(
        database("family"),
        share_kind(&publisher, &published),
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
