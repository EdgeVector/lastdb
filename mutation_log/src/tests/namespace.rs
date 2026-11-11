use super::*;

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (state, publisher, published) = shareable();
    let private = entry(&state.database_id, &published, &publisher, "hearth", 1, 1);
    let opened = step(
        &state,
        Command::BeginAppend {
            author: publisher.clone(),
            stream: published.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            author: publisher,
            stream: published,
            entries: vec![private],
            accepted_at_ms: 1,
        }),
        &hearth,
    );
    assert_eq!(refused, Err(LogError::AppPrivateNamespace));
    assert!(state.entries.is_empty());
    assert!(state.head.is_none());
}

#[test]
fn read_filter_hides_a_stored_app_private_entry_from_a_receiver() {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    state = step(
        &state,
        Command::Grant {
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("grant")
    .state;
    state.entries.push(StoredEntry {
        delivery_seq: 1,
        accepted_at_ms: 1,
        entry: entry(&state.database_id, &published, &publisher, "hearth", 1, 1),
    });
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());
    assert!(filter_readable(&state.entries, &publisher, &state, &hearth).is_empty());
    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("list");
    assert!(page.entries.is_empty());
}

#[test]
fn personal_database_stores_an_app_private_namespace() {
    let owner = principal("owner");
    let phone = stream("phone");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = step(
        &state,
        Command::Grant {
            principal: owner.clone(),
            capability: Capability::Device {
                stream: phone.clone(),
            },
        },
        &hearth,
    )
    .expect("grant")
    .state;
    let private = entry(&state.database_id, &phone, &owner, "hearth", 1, 3);
    state = apply(&state, &owner, &phone, vec![private], 3);
    let visible = filter_readable(&state.entries, &owner, &state, &hearth);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.namespace.as_str(), "hearth");
}

#[test]
fn personal_database_cannot_be_granted_to_a_receiver() {
    let state = LogState::open(database("personal"), DatabaseKind::Personal);
    let receiver = principal("receiver");
    let refused = step(
        &state,
        Command::Grant {
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    );
    assert_eq!(refused, Err(LogError::PersonalNotShareable));
    let submit = step(
        &state,
        Command::Grant {
            principal: receiver,
            capability: Capability::Submit {
                stream: stream("inbox"),
            },
        },
        &hearth,
    );
    assert_eq!(submit, Err(LogError::PersonalNotShareable));
    assert!(state.grants.is_empty());
    assert!(!can_read(&principal("receiver"), &state, &stream("inbox")));

    let (share, _publisher, _published) = shareable();
    let device = step(
        &share,
        Command::Grant {
            principal: principal("phone"),
            capability: Capability::Device {
                stream: stream("phone"),
            },
        },
        &hearth,
    );
    assert_eq!(device, Err(LogError::DeviceGrantNotShareable));
}

#[test]
fn share_snapshot_omits_submissions_and_app_private_entries() {
    let (mut state, publisher, published) = shareable();
    let contributor = principal("contributor");
    let submission = stream("contrib");
    state = step(
        &state,
        Command::Grant {
            principal: contributor.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("grant")
    .state;
    let official = entry(&state.database_id, &published, &publisher, "photos", 1, 1);
    state = apply(&state, &publisher, &published, vec![official], 1);
    let pending = entry(
        &state.database_id,
        &submission,
        &contributor,
        "photos",
        1,
        2,
    );
    state = apply(&state, &contributor, &submission, vec![pending], 2);
    state.entries.push(StoredEntry {
        delivery_seq: 2,
        accepted_at_ms: 3,
        entry: entry(&state.database_id, &published, &publisher, "hearth", 2, 3),
    });
    state = step(&state, Command::Snapshot { at_ms: 4 }, &hearth)
        .expect("snapshot")
        .state;
    let basis = state.snapshot_basis(&hearth);
    assert_eq!(basis.len(), 1);
    assert_eq!(basis[0].entry.namespace.as_str(), "photos");
    assert_eq!(basis[0].entry.stream, published);
}
