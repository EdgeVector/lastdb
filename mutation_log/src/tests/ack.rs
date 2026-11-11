use super::*;

#[test]
fn space_change_does_not_keep_a_stale_ack() {
    let (mut state, publisher, published, person, submission) = dual_grant();
    for seq in 1..=10 {
        let submitted = entry(&state.database_id, &submission, &person, "photos", seq, seq);
        state = apply(&state, &person, &submission, vec![submitted], seq);
    }
    for seq in 1..=3 {
        let published_entry = entry(
            &state.database_id,
            &published,
            &publisher,
            "photos",
            seq,
            10 + seq,
        );
        state = apply(
            &state,
            &publisher,
            &published,
            vec![published_entry],
            10 + seq,
        );
    }
    assert_eq!(state.head.as_ref().map(|head| head.delivery_seq), Some(13));
    assert_eq!(
        state
            .streams
            .get(&published)
            .expect("stream")
            .last_writer_seq,
        3
    );
    state = ack(&state, &person, 13);
    state = step(
        &state,
        Command::Revoke {
            caller: publisher.clone(),
            principal: person.clone(),
            capability: Capability::Submit { stream: submission },
        },
        &hearth,
    )
    .expect("revoke")
    .state;
    for seq in 4..=6 {
        let published_entry = entry(
            &state.database_id,
            &published,
            &publisher,
            "photos",
            seq,
            13 + seq,
        );
        state = apply(
            &state,
            &publisher,
            &published,
            vec![published_entry],
            13 + seq,
        );
    }
    state = ack(&state, &person, 2);
    state = ack(&state, &publisher, 16);
    state = step(
        &state,
        Command::Snapshot {
            caller: publisher.clone(),
            at_ms: 30,
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    let floor = state.effective_floor(30, &hearth);
    assert!(floor < 16, "stale ack advanced the floor to {floor}");
    state = step(
        &state,
        Command::Trim {
            caller: publisher,
            now_ms: 30,
        },
        &hearth,
    )
    .expect("trim")
    .state;
    let page = state
        .list(&person, 2, 10, 30, &hearth)
        .expect("publisher entries 4-6 stay readable");
    let seqs: Vec<u64> = page
        .entries
        .iter()
        .map(|row| row.entry.writer_seq)
        .collect();
    assert!(
        seqs.contains(&4) && seqs.contains(&5) && seqs.contains(&6),
        "{seqs:?}"
    );
}

fn dual_grant() -> (LogState, PrincipalId, StreamId, PrincipalId, StreamId) {
    let (mut state, publisher, published) = shareable();
    let person = principal("both");
    let submission = stream("both");
    state = step(
        &state,
        Command::Grant {
            caller: publisher.clone(),
            principal: person.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("receive")
    .state;
    state = step(
        &state,
        Command::Grant {
            caller: publisher.clone(),
            principal: person.clone(),
            capability: Capability::Submit {
                stream: submission.clone(),
            },
        },
        &hearth,
    )
    .expect("submit")
    .state;
    (state, publisher, published, person, submission)
}

fn ack(state: &LogState, consumer: &PrincipalId, through: u64) -> LogState {
    step(
        state,
        Command::Ack {
            consumer: consumer.clone(),
            through,
        },
        &hearth,
    )
    .expect("ack")
    .state
}
