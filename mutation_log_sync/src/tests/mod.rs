use mutation_log::{
    Capability, DatabaseId, DatabaseKind, DeviceId, LogState, Namespace, NamespaceKind,
    PrincipalId, StoredEntry, Writer,
};
use mutation_log_mem::{InMemoryMutationLog, MutationLog};

use crate::{drive, Outcome, SyncEvent, SyncRole, SyncState};

mod personal;
mod recover;
mod retry;
mod role;
mod share;
mod space;
mod writer;

type Policy = fn(&Namespace) -> NamespaceKind;
type MemLog = InMemoryMutationLog<Policy>;

fn hearth(namespace: &Namespace) -> NamespaceKind {
    if namespace.as_str() == "hearth" {
        NamespaceKind::AppPrivate
    } else {
        NamespaceKind::Official
    }
}

fn database(name: &str) -> DatabaseId {
    DatabaseId::new(name).expect("database id")
}

fn device(name: &str) -> DeviceId {
    DeviceId::new(name).expect("device id")
}

fn principal(name: &str) -> PrincipalId {
    PrincipalId::new(name).expect("principal id")
}

fn namespace(name: &str) -> Namespace {
    Namespace::new(name).expect("namespace")
}

fn writer(key: &PrincipalId, device_name: &str) -> Writer {
    Writer {
        key: key.clone(),
        device: device(device_name),
    }
}

fn open_log(state: LogState) -> MemLog {
    InMemoryMutationLog::new(state, hearth as Policy)
}

fn personal_log() -> MemLog {
    open_log(LogState::open(database("home"), DatabaseKind::Personal))
}

fn share_kind(publisher: &PrincipalId) -> DatabaseKind {
    DatabaseKind::Shareable {
        publisher: publisher.clone(),
    }
}

fn share_log() -> (MemLog, PrincipalId) {
    let publisher = principal("publisher");
    (
        open_log(LogState::open(database("family"), share_kind(&publisher))),
        publisher,
    )
}

fn owner_role(owner: PrincipalId, device_name: &str) -> SyncRole {
    SyncRole::Owner {
        principal: owner,
        device: device(device_name),
    }
}

fn publisher_role(publisher: PrincipalId, device_name: &str) -> SyncRole {
    SyncRole::Publisher {
        principal: publisher,
        device: device(device_name),
    }
}

fn contributor_role(contributor: PrincipalId, device_name: &str) -> SyncRole {
    SyncRole::Contributor {
        principal: contributor,
        device: device(device_name),
    }
}

fn append_event(namespace_name: &str, payload: &[u8], at_ms: u64) -> SyncEvent {
    SyncEvent::Append {
        namespace: namespace(namespace_name),
        payload: payload.to_vec(),
        authored_at_ms: at_ms,
        accepted_at_ms: at_ms,
    }
}

fn append(log: &mut MemLog, state: &SyncState, event: SyncEvent) -> SyncState {
    let state = SyncState::resume(state, log).expect("resume");
    let (next, outcome) = drive(&state, log, event, &hearth).expect("append");
    assert!(matches!(outcome, Outcome::Appended { .. }));
    next
}

fn pull(log: &mut MemLog, state: &SyncState, now_ms: u64) -> (SyncState, Vec<StoredEntry>) {
    let (next, outcome) = drive(state, log, SyncEvent::Pull { now_ms }, &hearth).expect("pull");
    match outcome {
        Outcome::Pulled {
            entries,
            cursor,
            reset,
        } => {
            assert!(reset.is_none());
            log.ack(&next.read_writer(), cursor)
                .expect("ack after apply");
            assert_eq!(next.cursor, cursor);
            (next, entries)
        }
        Outcome::Appended { .. } | Outcome::Duplicate { .. } => panic!("pull stored an entry"),
    }
}

pub(super) fn payloads(entries: &[StoredEntry]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .map(|stored| stored.entry.payload.clone())
        .collect()
}
