pub use super::*;

mod append;
mod namespace;
mod retain;
mod share;

pub fn database(name: &str) -> DatabaseId {
    DatabaseId::new(name).expect("database id")
}

pub fn stream(name: &str) -> StreamId {
    StreamId::new(name).expect("stream id")
}

pub fn principal(name: &str) -> PrincipalId {
    PrincipalId::new(name).expect("principal id")
}

pub fn namespace(name: &str) -> Namespace {
    Namespace::new(name).expect("namespace")
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
    writer: &StreamId,
    author: &PrincipalId,
    namespace_name: &str,
    seq: u64,
    at: u64,
) -> Entry {
    Entry {
        database_id: database.clone(),
        stream: writer.clone(),
        writer_seq: seq,
        authored_at_ms: at,
        author: author.clone(),
        namespace: namespace(namespace_name),
        incorporated_from: None,
        payload: vec![seq as u8],
    }
}

pub fn apply(
    state: &LogState,
    author: &PrincipalId,
    writer: &StreamId,
    entries: Vec<Entry>,
    accepted_at_ms: u64,
) -> LogState {
    let begun = step(
        state,
        Command::BeginAppend {
            author: author.clone(),
            stream: writer.clone(),
        },
        &hearth,
    )
    .expect("begin")
    .state;
    let appended = step(
        &begun,
        Command::Append(AppendBatch {
            author: author.clone(),
            stream: writer.clone(),
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
            stream: writer.clone(),
        },
        &hearth,
    )
    .expect("finish")
    .state
}

pub fn shareable() -> (LogState, PrincipalId, StreamId) {
    let publisher = principal("publisher");
    let published = stream("publisher");
    let state = LogState::open(
        database("social-family"),
        DatabaseKind::Shareable {
            publisher: publisher.clone(),
            publisher_stream: published.clone(),
        },
    );
    (state, publisher, published)
}
