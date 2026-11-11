use super::*;

#[test]
fn revoke_drops_every_device_ack_of_the_key() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let phone = writer_of(&receiver, "phone");
    let laptop = writer_of(&receiver, "laptop");
    state = grant_receive(&state, &publisher, &receiver);
    let note = entry(&state.database_id, &published, "photos", 1, 1);
    state = apply(&state, &published, vec![note], 1);
    state = ack(&state, &published, 1);
    state = ack(&state, &phone, 1);
    state = ack(&state, &laptop, 0);
    assert_eq!(state.acks.len(), 3);
    state = step(
        &state,
        Command::Revoke {
            caller: publisher,
            principal: receiver,
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("revoke")
    .state;
    assert_eq!(state.acks.len(), 1);
    assert!(state.acks.contains_key(&published));
}

#[test]
fn can_read_and_list_agree_on_app_private() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    state = grant_receive(&state, &publisher, &receiver);
    let private = entry(&state.database_id, &published, "hearth", 1, 1);
    assert!(!can_read(&receiver, &state, &private, &hearth));
    state.entries.push(StoredEntry {
        delivery_seq: 1,
        accepted_at_ms: 1,
        entry: private,
    });
    state.head = Some(Head {
        delivery_seq: 1,
        stamp: state.entries[0].entry.stamp(),
    });
    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("list");
    assert!(page.entries.is_empty());
    assert!(!can_read(
        &receiver,
        &state,
        &state.entries[0].entry,
        &hearth
    ));
    let publisher_page = state
        .list(&publisher, 0, 10, 1, &hearth)
        .expect("publisher");
    assert!(publisher_page.entries.is_empty());
    assert!(!can_read(
        &publisher,
        &state,
        &state.entries[0].entry,
        &hearth
    ));
}
