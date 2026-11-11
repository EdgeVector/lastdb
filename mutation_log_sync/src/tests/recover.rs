use mutation_log::{step, AppendBatch, Command, Entry, LogState, MutationLog, Namespace, Snapshot};

use super::{database, drive, hearth, open_log, owner_role, payloads, principal, writer, MemLog};
use crate::{Outcome, SyncEvent, SyncState};

#[test]
fn expired_pull_delivers_the_snapshot_and_resident_entries_once() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    state = put(&state, &phone, b"one", 1, 10);
    state = put(&state, &phone, b"two", 2, 11);
    state = cut(&state, &owner, 12);
    state = put(&state, &phone, b"three", 3, 13);
    state = ack_writer(&state, &phone, 1);
    state = trim_at(&state, &owner, 14);
    let mut log = open_log(state);
    let before = log.list(&owner, 1, 90, 14).expect("stored");
    assert_eq!(
        payloads(&before.entries),
        vec![b"two".to_vec(), b"three".to_vec()]
    );
    let reader = owner_state(&owner, "laptop");
    let (_advanced, first, snapshot) = pull_reset(&mut log, &reader, 14);
    assert_eq!(payloads(&first), vec![b"two".to_vec(), b"three".to_vec()]);
    assert_eq!(snapshot.expect("reset").through_delivery_seq, 2);
    let still = log.list(&owner, 1, 90, 14).expect("phone ack stays");
    assert_eq!(
        payloads(&still.entries),
        vec![b"two".to_vec(), b"three".to_vec()]
    );
    let (_again, second, reset) = pull_reset(&mut log, &reader, 14);
    assert!(second.is_empty());
    assert!(reset.is_none());
}

#[test]
fn laptop_recovers_everything_after_the_phone_acks() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let laptop = writer(&owner, "laptop");
    let mut state = personal(&owner);
    state = put(&state, &phone, b"phone", 1, 1);
    state = put(&state, &phone, b"laptop-needs-this", 2, 2);
    state = ack_writer(&state, &laptop, 0);
    state = ack_writer(&state, &phone, 2);
    state = cut(&state, &owner, 3);
    state = trim_at(&state, &owner, 4);
    assert_eq!(state.entries.len(), 2);
    let mut log = open_log(state);
    let reader = owner_state(&owner, "laptop");
    let (advanced, seen) = super::pull(&mut log, &reader, 5);
    assert_eq!(
        payloads(&seen),
        vec![b"phone".to_vec(), b"laptop-needs-this".to_vec()]
    );
    let (_again, empty) = super::pull(&mut log, &advanced, 6);
    assert!(empty.is_empty());
}

#[test]
fn unregistered_laptop_resets_to_the_snapshot() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    state = put(&state, &phone, b"phone", 1, 1);
    state = put(&state, &phone, b"laptop-needs-this", 2, 2);
    state = ack_writer(&state, &phone, 2);
    state = cut(&state, &owner, 3);
    state = trim_at(&state, &owner, 4);
    assert!(state.entries.is_empty());
    let mut log = open_log(state);
    let reader = owner_state(&owner, "laptop");
    let (_advanced, seen, reset) = pull_reset(&mut log, &reader, 5);
    assert!(seen.is_empty());
    assert_eq!(
        reset
            .expect("snapshot covers both entries")
            .through_delivery_seq,
        2
    );
    let (_again, second, another) = pull_reset(&mut log, &reader, 6);
    assert!(second.is_empty());
    assert!(another.is_none());
}

fn personal(owner: &mutation_log::PrincipalId) -> LogState {
    let state = LogState::open(database("home"), mutation_log::DatabaseKind::Personal);
    step(
        &state,
        Command::Grant {
            caller: owner.clone(),
            principal: owner.clone(),
            capability: mutation_log::Capability::Owner,
        },
        &hearth,
    )
    .expect("owner")
    .state
}

fn owner_state(owner: &mutation_log::PrincipalId, device_name: &str) -> SyncState {
    SyncState::open(
        database("home"),
        mutation_log::DatabaseKind::Personal,
        owner_role(owner.clone(), device_name),
    )
    .expect("owner")
}

fn put(
    state: &LogState,
    writer: &mutation_log::Writer,
    payload: &[u8],
    seq: u64,
    at_ms: u64,
) -> LogState {
    let entry = Entry {
        database_id: state.database_id.clone(),
        writer: writer.clone(),
        writer_seq: seq,
        authored_at_ms: at_ms,
        namespace: Namespace::new("notes").expect("namespace"),
        incorporated_from: None,
        payload: payload.to_vec(),
    };
    let begun = step(
        state,
        Command::BeginAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let appended = step(
        &begun,
        Command::Append(AppendBatch {
            writer: writer.clone(),
            entries: vec![entry],
            accepted_at_ms: at_ms,
        }),
        &hearth,
    )
    .expect("append")
    .state;
    step(
        &appended,
        Command::FinishAppend {
            writer: writer.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state
}

fn ack_writer(state: &LogState, reader: &mutation_log::Writer, through: u64) -> LogState {
    step(
        state,
        Command::Ack {
            reader: reader.clone(),
            through,
        },
        &hearth,
    )
    .expect("ack")
    .state
}

fn cut(state: &LogState, caller: &mutation_log::PrincipalId, at_ms: u64) -> LogState {
    step(
        state,
        Command::Snapshot {
            caller: caller.clone(),
            at_ms,
        },
        &hearth,
    )
    .expect("snapshot")
    .state
}

fn trim_at(state: &LogState, caller: &mutation_log::PrincipalId, now_ms: u64) -> LogState {
    step(
        state,
        Command::Trim {
            caller: caller.clone(),
            now_ms,
        },
        &hearth,
    )
    .expect("trim")
    .state
}

fn pull_reset(
    log: &mut MemLog,
    state: &SyncState,
    now_ms: u64,
) -> (SyncState, Vec<mutation_log::StoredEntry>, Option<Snapshot>) {
    let (next, outcome) = drive(state, log, SyncEvent::Pull { now_ms }, &hearth).expect("pull");
    match outcome {
        Outcome::Pulled {
            entries,
            cursor,
            reset,
        } => {
            assert_eq!(next.cursor, cursor);
            (next, entries, reset)
        }
        Outcome::Appended { .. } | Outcome::Duplicate { .. } => panic!("pull stored an entry"),
    }
}
