use super::*;

fn granted_share() -> (LogState, PrincipalId, StreamId, PrincipalId, PrincipalId) {
    let (mut state, publisher, published) = shareable();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let other = principal("other");
    state = step(
        &state,
        Command::Grant {
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("receiver")
    .state;
    state = step(
        &state,
        Command::Grant {
            principal: contributor.clone(),
            capability: Capability::Submit {
                stream: stream("contrib"),
            },
        },
        &hearth,
    )
    .expect("contributor")
    .state;
    state = step(
        &state,
        Command::Grant {
            principal: other,
            capability: Capability::Submit {
                stream: stream("other-contrib"),
            },
        },
        &hearth,
    )
    .expect("other")
    .state;
    (state, publisher, published, receiver, contributor)
}

#[test]
fn receiver_cannot_read_another_contributors_submission() {
    let (state, _publisher, _published, receiver, contributor) = granted_share();
    let other = principal("other");
    let submission = stream("contrib");
    let submitted = entry(
        &state.database_id,
        &submission,
        &contributor,
        "photos",
        1,
        5,
    );
    let state = apply(&state, &contributor, &submission, vec![submitted], 5);
    assert!(!can_read(&receiver, &state, &submission));
    assert!(!can_read(&other, &state, &submission));
    assert!(can_read(&contributor, &state, &submission));
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());
    assert!(filter_readable(&state.entries, &other, &state, &hearth).is_empty());
    assert_eq!(
        filter_readable(&state.entries, &contributor, &state, &hearth).len(),
        1
    );
}

#[test]
fn receiver_cannot_read_another_stream_or_database() {
    let (state, publisher, published, receiver, _contributor) = granted_share();
    let other_stream = stream("other-contrib");
    let other = principal("other");
    let foreign_db = database("personal");
    let own = entry(&state.database_id, &other_stream, &other, "photos", 1, 4);
    let state = apply(&state, &other, &other_stream, vec![own], 4);
    let published_entry = entry(&state.database_id, &published, &publisher, "photos", 1, 6);
    let state = apply(&state, &publisher, &published, vec![published_entry], 6);

    let mut mixed = state.entries.clone();
    let mut foreign = entry(&foreign_db, &published, &publisher, "photos", 1, 7);
    foreign.database_id = foreign_db;
    mixed.push(StoredEntry {
        delivery_seq: 99,
        accepted_at_ms: 7,
        entry: foreign,
    });
    let visible = filter_readable(&mixed, &receiver, &state, &hearth);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.database_id, state.database_id);
    assert_eq!(visible[0].entry.stream, published);
    assert!(!can_read(&receiver, &state, &other_stream));
    assert!(can_read(&receiver, &state, &published));
}

#[test]
fn receiver_sees_a_contribution_only_after_the_publisher_reappends_it() {
    let (state, publisher, published, receiver, contributor) = granted_share();
    let submission = stream("contrib");
    let submitted = entry(
        &state.database_id,
        &submission,
        &contributor,
        "photos",
        1,
        8,
    );
    let state = apply(&state, &contributor, &submission, vec![submitted], 8);
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth).is_empty());

    let mut incorporated = entry(&state.database_id, &published, &publisher, "photos", 1, 9);
    incorporated.incorporated_from = Some(Incorporation {
        submission_stream: submission,
        writer_seq: 1,
        contributor: contributor.clone(),
    });
    let state = apply(&state, &publisher, &published, vec![incorporated], 9);
    let visible = filter_readable(&state.entries, &receiver, &state, &hearth);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].entry.stream, published);
    assert_eq!(visible[0].entry.author, publisher);
    assert_eq!(
        visible[0]
            .entry
            .incorporated_from
            .as_ref()
            .map(|from| &from.contributor),
        Some(&contributor)
    );
    assert!(filter_readable(&state.entries, &receiver, &state, &hearth)
        .iter()
        .all(|stored| stored.entry.stream == published));
}
