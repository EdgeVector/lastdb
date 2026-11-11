use super::*;

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (state, _publisher, writer) = shareable();
    let private = entry(&state.database_id, &writer, "hearth", 1, 1);
    let opened = step(
        &state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let refused = step(
        &opened,
        Command::Append(AppendBatch {
            writer,
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
    state = grant_receive(&state, &publisher, &receiver);
    state.entries.push(StoredEntry {
        delivery_seq: 1,
        accepted_at_ms: 1,
        entry: entry(&state.database_id, &published, "hearth", 1, 1),
    });
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());
    assert!(filter_readable(&state.entries, &publisher, &state, &hearth).is_empty());
    let page = state.list(&receiver, 0, 10, 1, &hearth).expect("list");
    assert!(page.entries.is_empty());
    assert_eq!(page.next, 1);
}

#[test]
fn personal_database_stores_an_app_private_namespace() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let mut state = LogState::open(database("personal"), DatabaseKind::Personal);
    state = grant_owner(&state, &owner, &owner);
    let private = entry(&state.database_id, &phone, "hearth", 1, 3);
    state = apply(&state, &phone, vec![private], 3);
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
            caller: receiver.clone(),
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    );
    assert_eq!(refused, Err(LogError::PersonalNotShareable));
    let submit = step(
        &state,
        Command::Grant {
            caller: receiver.clone(),
            principal: receiver.clone(),
            capability: Capability::Submit,
        },
        &hearth,
    );
    assert_eq!(submit, Err(LogError::PersonalNotShareable));
    assert!(state.grants.is_empty());
    let sample = entry(
        &state.database_id,
        &writer_of(&receiver, "phone"),
        "notes",
        1,
        1,
    );
    assert!(!can_read(&receiver, &state, &sample, &hearth));

    let (share, publisher, _published) = shareable();
    let device_grant = step(
        &share,
        Command::Grant {
            caller: publisher,
            principal: principal("phone"),
            capability: Capability::Owner,
        },
        &hearth,
    );
    assert_eq!(device_grant, Err(LogError::DeviceGrantNotShareable));
}

#[test]
fn share_snapshot_omits_submissions_and_app_private_entries() {
    let (mut state, publisher, published) = shareable();
    let contributor = principal("contributor");
    let submission = writer_of(&contributor, "contrib");
    state = grant_submit(&state, &publisher, &contributor);
    let official = entry(&state.database_id, &published, "photos", 1, 1);
    state = apply(&state, &published, vec![official], 1);
    let pending = entry(&state.database_id, &submission, "photos", 1, 2);
    state = apply(&state, &submission, vec![pending], 2);
    state.entries.push(StoredEntry {
        delivery_seq: 2,
        accepted_at_ms: 3,
        entry: entry(&state.database_id, &published, "hearth", 2, 3),
    });
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher,
            at_ms: 4,
            snapshot_ref: snap("basis"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    let basis = state.snapshot_basis(&hearth);
    assert_eq!(basis.len(), 1);
    assert_eq!(basis[0].entry.namespace.as_str(), "photos");
    assert_eq!(basis[0].entry.writer.key, published.key);
}

fn store(
    state: &LogState,
    writer: &Writer,
    namespace_name: &str,
    policy: &impl NamespacePolicy,
) -> Result<LogState, LogError> {
    let begun = step(
        state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        policy,
    )?;
    let appended = step(
        &begun.state,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries: vec![entry(&state.database_id, writer, namespace_name, 1, 1)],
            accepted_at_ms: 1,
        }),
        policy,
    )?;
    Ok(step(
        &appended.state,
        Command::FinishAppend {
            writer: writer.clone(),
        },
        policy,
    )?
    .state)
}

#[test]
fn shareable_database_accepts_each_listed_namespace() {
    assert_eq!(SHAREABLE_NAMESPACES, &["lastdb-files", "social"]);
    for name in SHAREABLE_NAMESPACES {
        let (state, _publisher, writer) = shareable();
        let stored = store(&state, &writer, name, &classify_shareable).expect("listed namespace");
        assert_eq!(stored.entries.len(), 1);
        assert_eq!(stored.entries[0].entry.namespace.as_str(), *name);
    }
}

#[test]
fn shareable_database_rejects_social_graph() {
    let (state, _publisher, writer) = shareable();
    let refused = store(&state, &writer, "social-graph", &classify_shareable);
    assert_eq!(refused, Err(LogError::AppPrivateNamespace));
    assert!(state.entries.is_empty());
}

#[test]
fn shareable_database_rejects_hearth_photometa() {
    let (state, _publisher, writer) = shareable();
    let refused = store(&state, &writer, "hearth", &classify_shareable);
    assert_eq!(refused, Err(LogError::AppPrivateNamespace));
    assert!(state.entries.is_empty());
}

#[test]
fn personal_database_accepts_an_unlisted_namespace() {
    let owner = principal("owner");
    let phone = writer_of(&owner, "phone");
    let state = grant_owner(
        &LogState::open(database("personal"), DatabaseKind::Personal),
        &owner,
        &owner,
    );
    let stored = store(&state, &phone, "hearth", &classify_shareable).expect("personal namespace");
    assert_eq!(stored.entries.len(), 1);
    assert_eq!(stored.entries[0].entry.namespace.as_str(), "hearth");
}
