pub use super::*;

mod ack;
mod append;
mod cursor;
mod device;
mod namespace;
mod removal;
mod retain;
mod review;
mod share;

pub fn database(name: &str) -> DatabaseId {
    DatabaseId::new(name).expect("database id")
}

pub fn device(name: &str) -> DeviceId {
    DeviceId::new(name).expect("device id")
}

pub fn principal(name: &str) -> PrincipalId {
    PrincipalId::new(name).expect("principal id")
}

pub fn namespace(name: &str) -> Namespace {
    Namespace::new(name).expect("namespace")
}

pub fn writer_of(key: &PrincipalId, device_name: &str) -> Writer {
    Writer {
        key: key.clone(),
        device: device(device_name),
    }
}

pub fn hearth(namespace: &Namespace) -> NamespaceKind {
    if namespace.as_str() == "hearth" {
        NamespaceKind::AppPrivate
    } else {
        NamespaceKind::Official
    }
}

pub fn entry(
    database: &DatabaseId,
    writer: &Writer,
    namespace_name: &str,
    seq: u64,
    at: u64,
) -> Entry {
    Entry {
        database_id: database.clone(),
        writer: writer.clone(),
        writer_seq: seq,
        authored_at_ms: at,
        namespace: namespace(namespace_name),
        incorporated_from: None,
        payload: vec![seq as u8],
    }
}

pub fn apply(
    state: &LogState,
    writer: &Writer,
    entries: Vec<Entry>,
    accepted_at_ms: u64,
) -> LogState {
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
            entries,
            accepted_at_ms,
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

pub fn shareable() -> (LogState, PrincipalId, Writer) {
    let publisher = principal("publisher");
    let writer = writer_of(&publisher, "publisher");
    let state = LogState::open(
        database("social-family"),
        DatabaseKind::Shareable {
            publisher: publisher.clone(),
        },
    );
    (state, publisher, writer)
}

pub fn grant_owner(state: &LogState, caller: &PrincipalId, owner: &PrincipalId) -> LogState {
    step(
        state,
        Command::Grant {
            caller: caller.clone(),
            principal: owner.clone(),
            capability: Capability::Owner,
        },
        &hearth,
    )
    .expect("owner")
    .state
}

pub fn grant_receive(
    state: &LogState,
    publisher: &PrincipalId,
    receiver: &PrincipalId,
) -> LogState {
    step(
        state,
        Command::Grant {
            caller: publisher.clone(),
            principal: receiver.clone(),
            capability: Capability::Receive,
        },
        &hearth,
    )
    .expect("receive")
    .state
}

pub fn grant_submit(
    state: &LogState,
    publisher: &PrincipalId,
    contributor: &PrincipalId,
) -> LogState {
    step(
        state,
        Command::Grant {
            caller: publisher.clone(),
            principal: contributor.clone(),
            capability: Capability::Submit,
        },
        &hearth,
    )
    .expect("submit")
    .state
}

pub fn ack(state: &LogState, reader: &Writer, through: u64) -> LogState {
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
