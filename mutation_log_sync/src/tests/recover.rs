use std::collections::BTreeMap;

use mutation_log::{
    step, AppendBatch, Command, Entry, LogState, MutationLog, Namespace, Reset, SnapshotRef,
    StoredEntry,
};

use super::{database, drive, hearth, open_log, owner_role, payloads, principal, writer, MemLog};
use crate::{Outcome, SyncEvent, SyncState};

type Image = BTreeMap<u64, Vec<u8>>;

#[test]
fn expired_pull_delivers_snapshot_content_and_the_suffix() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    let mut snaps = BTreeMap::new();
    state = put(&state, &phone, b"one", 1, 10);
    state = put(&state, &phone, b"two", 2, 11);
    state = cut(&state, &owner, 12, "home-1", &mut snaps);
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
    let (advanced, entries, reset) = pull_reset(&mut log, &reader, 14);
    let installed = install(reset.as_ref(), &snaps, &entries);
    assert_eq!(installed, image_of(&[b"one", b"two", b"three"]));
    assert_eq!(install(reset.as_ref(), &snaps, &entries), installed);
    log.ack(&advanced.read_writer(), advanced.cursor)
        .expect("ack after apply");
    let (_again, second, another) = pull_reset(&mut log, &advanced, 15);
    assert!(second.is_empty());
    assert!(another.is_none());
}

#[test]
fn crash_before_ack_redelivers_the_same_reset() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    let mut snaps = BTreeMap::new();
    state = put(&state, &phone, b"one", 1, 10);
    state = put(&state, &phone, b"two", 2, 11);
    state = cut(&state, &owner, 12, "home-1", &mut snaps);
    state = put(&state, &phone, b"three", 3, 13);
    state = ack_writer(&state, &phone, 1);
    state = trim_at(&state, &owner, 14);
    let mut log = open_log(state);
    let reader = owner_state(&owner, "laptop");
    let (_advanced, first_entries, first_reset) = pull_reset(&mut log, &reader, 14);
    let once = install(first_reset.as_ref(), &snaps, &first_entries);
    let twice = install(first_reset.as_ref(), &snaps, &first_entries);
    assert_eq!(once, twice);
    let (_ignored, second_entries, second_reset) = pull_reset(&mut log, &reader, 15);
    assert_eq!(second_reset, first_reset);
    assert_eq!(payloads(&second_entries), payloads(&first_entries));
}

#[test]
fn first_pull_registers_zero_and_keeps_the_entry() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    state = put(&state, &phone, b"kept", 1, 1);
    state = ack_writer(&state, &phone, 1);
    state = cut(&state, &owner, 2, "home-kept", &mut BTreeMap::new());
    let mut log = open_log(state);
    let laptop = owner_state(&owner, "laptop");
    let (_next, entries, reset) = pull_reset(&mut log, &laptop, 3);
    assert!(reset.is_none());
    assert_eq!(payloads(&entries), vec![b"kept".to_vec()]);
}

#[test]
fn unregistered_laptop_downloads_the_snapshot() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    let mut snaps = BTreeMap::new();
    state = put(&state, &phone, b"phone", 1, 1);
    state = put(&state, &phone, b"laptop-needs-this", 2, 2);
    state = ack_writer(&state, &phone, 2);
    state = cut(&state, &owner, 3, "home-2", &mut snaps);
    state = trim_at(&state, &owner, 4);
    assert!(state.entries.is_empty());
    let mut log = open_log(state);
    let reader = owner_state(&owner, "laptop");
    let (_advanced, seen, reset) = pull_reset(&mut log, &reader, 5);
    assert!(seen.is_empty());
    let reset = reset.expect("snapshot covers both entries");
    assert_eq!(reset.through, 2);
    assert_eq!(
        snaps.get(reset.snapshot_ref.as_str()).expect("bytes"),
        &image_of(&[b"phone", b"laptop-needs-this"])
    );
}

#[test]
fn reinstall_behind_the_stored_ack_resets() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut state = personal(&owner);
    let mut snaps = BTreeMap::new();
    state = put(&state, &phone, b"one", 1, 1);
    state = put(&state, &phone, b"two", 2, 2);
    state = cut(&state, &owner, 3, "home-3", &mut snaps);
    state = ack_writer(&state, &phone, 2);
    state = trim_at(&state, &owner, 4);
    let mut log = open_log(state);
    let wiped = owner_state(&owner, "phone");
    let (_next, seen, reset) = pull_reset(&mut log, &wiped, 5);
    assert!(seen.is_empty());
    let reset = reset.expect("local cursor is behind the ack");
    assert_eq!(
        snaps.get(reset.snapshot_ref.as_str()).expect("bytes"),
        &image_of(&[b"one", b"two"])
    );
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
    .expect("reader")
}

fn put(
    state: &LogState,
    phone: &mutation_log::Writer,
    payload: &[u8],
    seq: u64,
    at: u64,
) -> LogState {
    let begun = step(
        state,
        Command::BeginAppend {
            writer: phone.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let appended = step(
        &begun,
        Command::Append(AppendBatch {
            writer: phone.clone(),
            entries: vec![note(phone, payload, seq, at)],
            accepted_at_ms: at,
        }),
        &hearth,
    )
    .expect("append")
    .state;
    step(
        &appended,
        Command::FinishAppend {
            writer: phone.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state
}

fn note(phone: &mutation_log::Writer, payload: &[u8], seq: u64, at: u64) -> Entry {
    Entry {
        database_id: database("home"),
        writer: phone.clone(),
        writer_seq: seq,
        authored_at_ms: at,
        namespace: Namespace::new("notes").expect("namespace"),
        incorporated_from: None,
        payload: payload.to_vec(),
    }
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

fn cut(
    state: &LogState,
    caller: &mutation_log::PrincipalId,
    at_ms: u64,
    name: &str,
    snaps: &mut BTreeMap<String, Image>,
) -> LogState {
    let next = step(
        state,
        Command::Snapshot {
            caller: caller.clone(),
            at_ms,
            snapshot_ref: SnapshotRef::new(name).expect("ref"),
        },
        &hearth,
    )
    .expect("snapshot")
    .state;
    let through = next.snapshot.as_ref().expect("cut").through_delivery_seq;
    snaps.insert(name.to_owned(), image_through(state, through));
    next
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
) -> (SyncState, Vec<StoredEntry>, Option<Reset>) {
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

fn image_through(state: &LogState, through: u64) -> Image {
    state
        .entries
        .iter()
        .filter(|stored| stored.delivery_seq <= through)
        .map(|stored| (stored.delivery_seq, stored.entry.payload.clone()))
        .collect()
}

fn image_of(payloads: &[&[u8]]) -> Image {
    payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| (index as u64 + 1, payload.to_vec()))
        .collect()
}

fn install(
    reset: Option<&Reset>,
    snaps: &BTreeMap<String, Image>,
    entries: &[StoredEntry],
) -> Image {
    let mut image = reset
        .map(|reset| snaps.get(reset.snapshot_ref.as_str()).expect("ref").clone())
        .unwrap_or_default();
    for stored in entries {
        image.insert(stored.delivery_seq, stored.entry.payload.clone());
    }
    image
}
