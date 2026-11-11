use mutation_log::{
    Capability, DatabaseId, DatabaseKind, LogState, Namespace, NamespaceKind, PrincipalId,
    StoredEntry, StreamId,
};
use mutation_log_mem::{InMemoryMutationLog, MutationLog};

use crate::{drive, Outcome, SyncEvent, SyncRole, SyncState};

mod path;
mod personal;
mod retry;
mod role;
mod share;
mod space;

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

fn stream(name: &str) -> StreamId {
    StreamId::new(name).expect("stream id")
}

fn principal(name: &str) -> PrincipalId {
    PrincipalId::new(name).expect("principal id")
}

fn namespace(name: &str) -> Namespace {
    Namespace::new(name).expect("namespace")
}

fn open_log(state: LogState) -> MemLog {
    mutation_log_mem::MutationLogConfig::from_value(Some("on"))
        .open(state, hearth as Policy)
        .expect("enabled config installs the log")
}

fn personal_log() -> MemLog {
    open_log(LogState::open(database("home"), DatabaseKind::Personal))
}

fn share_kind(publisher: &PrincipalId, published: &StreamId) -> DatabaseKind {
    DatabaseKind::Shareable {
        publisher: publisher.clone(),
        publisher_stream: published.clone(),
    }
}

fn share_log() -> (MemLog, PrincipalId, StreamId) {
    let publisher = principal("publisher");
    let published = stream("publisher");
    let kind = share_kind(&publisher, &published);
    (
        open_log(LogState::open(database("family"), kind)),
        publisher,
        published,
    )
}

fn device(owner: PrincipalId, writer: StreamId) -> SyncRole {
    SyncRole::Device {
        principal: owner,
        stream: writer,
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
    let (next, outcome) = drive(state, log, event, &hearth).expect("append");
    assert!(matches!(outcome, Outcome::Appended { .. }));
    next
}

fn pull(log: &mut MemLog, state: &SyncState, now_ms: u64) -> (SyncState, Vec<StoredEntry>) {
    let (next, outcome) = drive(state, log, SyncEvent::Pull { now_ms }, &hearth).expect("pull");
    match outcome {
        Outcome::Pulled { entries, cursor } => {
            assert_eq!(next.cursor, cursor);
            (next, entries)
        }
        Outcome::Appended { .. } | Outcome::Duplicate { .. } => panic!("pull stored an entry"),
    }
}
