use mutation_log::{
    AppendBatch, AppendOutcome, Capability, DatabaseId, DatabaseKind, Entry, Incorporation,
    LogError, LogState, Namespace, NamespaceKind, Outcome, PrincipalId, StreamId,
    APPEND_BATCH_LIMIT,
};

use crate::memory::require_outcome;
use crate::{InMemoryMutationLog, MutationLog, MutationLogConfig};

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

fn enabled(state: LogState) -> MemLog {
    MutationLogConfig::from_value(Some("on"))
        .open(state, hearth as Policy)
        .expect("enabled config installs the adapter")
}

fn share_log() -> (MemLog, PrincipalId, StreamId) {
    let publisher = principal("publisher");
    let published = stream("publisher");
    let state = LogState::open(
        database("social-family"),
        DatabaseKind::Shareable {
            publisher: publisher.clone(),
            publisher_stream: published.clone(),
        },
    );
    (enabled(state), publisher, published)
}

fn entry(
    database_id: &DatabaseId,
    writer: &StreamId,
    author: &PrincipalId,
    namespace_name: &str,
    seq: u64,
) -> Entry {
    Entry {
        database_id: database_id.clone(),
        stream: writer.clone(),
        writer_seq: seq,
        authored_at_ms: seq,
        author: author.clone(),
        namespace: namespace(namespace_name),
        incorporated_from: None,
        payload: vec![1],
    }
}

fn batch(author: &PrincipalId, writer: &StreamId, namespace_name: &str, count: u64) -> AppendBatch {
    let database_id = database("social-family");
    let entries = (1..=count)
        .map(|seq| entry(&database_id, writer, author, namespace_name, seq))
        .collect();
    AppendBatch {
        author: author.clone(),
        stream: writer.clone(),
        entries,
        accepted_at_ms: 1,
    }
}

fn page(log: &dyn MutationLog, reader: &PrincipalId) -> Vec<Entry> {
    log.list(reader, 0, 90, 1)
        .expect("list")
        .entries
        .into_iter()
        .map(|stored| stored.entry)
        .collect()
}

#[test]
fn mismatched_step_outcome_is_not_an_invalid_entry() {
    let mismatched = require_outcome(Outcome::Granted, |outcome| match outcome {
        Outcome::Appended(value) => Some(value),
        _ => None,
    });
    assert_eq!(mismatched, Err(LogError::UnexpectedOutcome));
}

#[test]
fn config_defaults_off() {
    let state = LogState::open(database("personal"), DatabaseKind::Personal);
    assert!(!MutationLogConfig::default().is_enabled());
    assert!(MutationLogConfig::default()
        .open(state, hearth as Policy)
        .is_none());
    for raw in [None, Some(""), Some("0"), Some("false"), Some("no")] {
        assert!(!MutationLogConfig::from_value(raw).is_enabled());
    }
    let again = LogState::open(database("personal"), DatabaseKind::Personal);
    assert!(MutationLogConfig::from_value(Some("1"))
        .open(again, hearth as Policy)
        .is_some());
    assert_eq!(MutationLogConfig::FLAG, "LASTDB_MUTATION_LOG");
}

#[test]
fn failed_append_releases_the_stream() {
    let (mut log, publisher, published) = share_log();
    let port: &mut dyn MutationLog = &mut log;
    let too_many = u64::try_from(APPEND_BATCH_LIMIT).expect("limit fits") + 1;
    let refused = port.append(batch(&publisher, &published, "photos", too_many));
    assert_eq!(refused, Err(LogError::BatchTooLarge));
    let accepted = port
        .append(batch(&publisher, &published, "photos", 1))
        .expect("the failed batch released the stream");
    assert_eq!(
        accepted,
        AppendOutcome::Accepted {
            delivery_seqs: vec![1]
        }
    );
}

#[test]
fn receiver_reads_only_the_publisher_stream() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let other = principal("other");
    let submission = stream("contribution");
    let other_stream = stream("other-contribution");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .unwrap();
    log.grant(
        &publisher,
        contributor.clone(),
        Capability::Submit {
            stream: submission.clone(),
        },
    )
    .unwrap();
    log.grant(
        &publisher,
        other.clone(),
        Capability::Submit {
            stream: other_stream,
        },
    )
    .unwrap();
    log.append(batch(&publisher, &published, "photos", 1))
        .unwrap();
    log.append(batch(&contributor, &submission, "photos", 1))
        .unwrap();
    let seen = page(&log, &receiver);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].stream, published);
    let own = page(&log, &contributor);
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].stream, submission);
    assert!(page(&log, &other).is_empty());
    assert_eq!(page(&log, &publisher).len(), 2);
}

#[test]
fn submission_is_visible_to_a_receiver_only_after_reappend() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = stream("contribution");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .unwrap();
    log.grant(
        &publisher,
        contributor.clone(),
        Capability::Submit {
            stream: submission.clone(),
        },
    )
    .unwrap();
    log.append(batch(&contributor, &submission, "photos", 1))
        .unwrap();
    assert!(page(&log, &receiver).is_empty());
    let mut copied = entry(
        &database("social-family"),
        &published,
        &publisher,
        "photos",
        1,
    );
    copied.incorporated_from = Some(Incorporation {
        submission_stream: submission,
        writer_seq: 1,
        contributor,
    });
    log.append(AppendBatch {
        author: publisher,
        stream: published.clone(),
        entries: vec![copied],
        accepted_at_ms: 2,
    })
    .unwrap();
    let seen = page(&log, &receiver);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].stream, published);
    assert!(seen[0].incorporated_from.is_some());
}

#[test]
fn shareable_database_rejects_a_device_grant() {
    let (mut log, publisher, _) = share_log();
    let refused = log.grant(
        &publisher,
        principal("phone"),
        Capability::Device {
            stream: stream("phone"),
        },
    );
    assert_eq!(refused, Err(LogError::DeviceGrantNotShareable));
}

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (mut log, publisher, published) = share_log();
    let refused = log.append(batch(&publisher, &published, "hearth", 1));
    assert_eq!(refused, Err(LogError::AppPrivateNamespace));
    assert!(page(&log, &publisher).is_empty());
}

#[test]
fn personal_database_stores_app_private_and_rejects_a_receiver() {
    let owner = principal("owner");
    let phone = stream("phone");
    let mut log = enabled(LogState::open(database("personal"), DatabaseKind::Personal));
    let receiver = principal("receiver");
    assert_eq!(
        log.grant(&owner, receiver.clone(), Capability::Receive),
        Err(LogError::PersonalNotShareable)
    );
    assert_eq!(
        log.grant(
            &owner,
            receiver,
            Capability::Submit {
                stream: stream("nope"),
            },
        ),
        Err(LogError::PersonalNotShareable)
    );
    log.grant(
        &owner,
        owner.clone(),
        Capability::Device {
            stream: phone.clone(),
        },
    )
    .unwrap();
    let mut personal = batch(&owner, &phone, "hearth", 1);
    personal.entries[0].database_id = database("personal");
    log.append(personal).unwrap();
    let seen = page(&log, &owner);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].namespace.as_str(), "hearth");
}

#[test]
fn grants_are_not_capped_at_twelve() {
    let (mut log, publisher, _) = share_log();
    for index in 0..13 {
        let name = format!("c{index}");
        let person = principal(&name);
        let writer = stream(&name);
        log.grant(
            &publisher,
            person.clone(),
            Capability::Submit {
                stream: writer.clone(),
            },
        )
        .unwrap();
        log.append(batch(&person, &writer, "photos", 1)).unwrap();
    }
    let publisher = principal("publisher");
    assert_eq!(page(&log, &publisher).len(), 13);
}
