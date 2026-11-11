use super::*;

#[test]
fn two_devices_round_trip_one_personal_log() {
    let owner = principal("owner");
    let mut log = personal_log();
    log.grant(&owner, owner.clone(), Capability::Owner)
        .expect("owner");
    let phone_state = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        owner_role(owner.clone(), "phone"),
    )
    .expect("phone state");
    let laptop_state = SyncState::open(
        database("home"),
        DatabaseKind::Personal,
        owner_role(owner, "laptop"),
    )
    .expect("laptop state");

    let phone_state = append(
        &mut log,
        &phone_state,
        append_event("hearth", b"from-phone", 10),
    );
    let (laptop_state, seen) = pull(&mut log, &laptop_state, 11);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].entry.namespace.as_str(), "hearth");
    assert_eq!(seen[0].entry.payload, b"from-phone");

    let _laptop_state = append(
        &mut log,
        &laptop_state,
        append_event("notes", b"from-laptop", 12),
    );
    let (_phone_state, seen) = pull(&mut log, &phone_state, 13);
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].entry.payload, b"from-laptop");
}
