use mutation_log::{
    AppendBatch, AppendOutcome, Capability, DatabaseId, DatabaseKind, DeviceId, Entry,
    Incorporation, LogError, LogState, Namespace, NamespaceKind, Outcome, PrincipalId, Writer,
    APPEND_BATCH_LIMIT,
};

use crate::memory::require_outcome;
use crate::{InMemoryMutationLog, MutationLog};

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

fn principal(name: &str) -> PrincipalId {
    PrincipalId::new(name).expect("principal id")
}

fn namespace(name: &str) -> Namespace {
    Namespace::new(name).expect("namespace")
}

fn writer(key: &PrincipalId, device_name: &str) -> Writer {
    Writer {
        key: key.clone(),
        device: DeviceId::new(device_name).expect("device id"),
    }
}

fn enabled(state: LogState) -> MemLog {
    InMemoryMutationLog::new(state, hearth as Policy)
}

fn share_log() -> (MemLog, PrincipalId, Writer) {
    let publisher = principal("publisher");
    let published = writer(&publisher, "publisher");
    let state = LogState::open(
        database("social-family"),
        DatabaseKind::Shareable {
            publisher: publisher.clone(),
        },
    );
    (enabled(state), publisher, published)
}

fn entry(database_id: &DatabaseId, writer: &Writer, namespace_name: &str, seq: u64) -> Entry {
    Entry {
        database_id: database_id.clone(),
        writer: writer.clone(),
        writer_seq: seq,
        authored_at_ms: seq,
        namespace: namespace(namespace_name),
        incorporated_from: None,
        payload: vec![1],
    }
}

fn batch(writer: &Writer, namespace_name: &str, count: u64) -> AppendBatch {
    let database_id = database("social-family");
    let entries = (1..=count)
        .map(|seq| entry(&database_id, writer, namespace_name, seq))
        .collect();
    AppendBatch {
        writer: writer.clone(),
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
fn failed_append_releases_the_writer() {
    let (mut log, _publisher, published) = share_log();
    let port: &mut dyn MutationLog = &mut log;
    let too_many = u64::try_from(APPEND_BATCH_LIMIT).expect("limit fits") + 1;
    let refused = port.append(batch(&published, "photos", too_many));
    assert_eq!(refused, Err(LogError::BatchTooLarge));
    let accepted = port
        .append(batch(&published, "photos", 1))
        .expect("the failed batch released the writer");
    assert_eq!(
        accepted,
        AppendOutcome::Accepted {
            delivery_seqs: vec![1]
        }
    );
}

#[test]
fn receiver_reads_only_publisher_key_entries() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let other = principal("other");
    let submission = writer(&contributor, "contrib");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .unwrap();
    log.grant(&publisher, contributor.clone(), Capability::Submit)
        .unwrap();
    log.grant(&publisher, other.clone(), Capability::Submit)
        .unwrap();
    log.append(batch(&published, "photos", 1)).unwrap();
    log.append(batch(&submission, "photos", 1)).unwrap();
    let seen = page(&log, &receiver);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].writer.key, publisher);
    let own = page(&log, &contributor);
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].writer, submission);
    assert!(page(&log, &other).is_empty());
    assert_eq!(page(&log, &publisher).len(), 2);
}

#[test]
fn submission_is_visible_to_a_receiver_only_after_reappend() {
    let (mut log, publisher, published) = share_log();
    let receiver = principal("receiver");
    let contributor = principal("contributor");
    let submission = writer(&contributor, "contrib");
    log.grant(&publisher, receiver.clone(), Capability::Receive)
        .unwrap();
    log.grant(&publisher, contributor, Capability::Submit)
        .unwrap();
    log.append(batch(&submission, "photos", 1)).unwrap();
    assert!(page(&log, &receiver).is_empty());
    let mut copied = entry(&database("social-family"), &published, "photos", 1);
    copied.incorporated_from = Some(Incorporation {
        writer: submission,
        writer_seq: 1,
    });
    copied.payload = vec![1];
    log.append(AppendBatch {
        writer: published,
        entries: vec![copied],
        accepted_at_ms: 2,
    })
    .unwrap();
    let seen = page(&log, &receiver);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].writer.key, publisher);
    assert!(seen[0].incorporated_from.is_some());
}

#[test]
fn shareable_database_rejects_an_owner_grant() {
    let (mut log, publisher, _) = share_log();
    let refused = log.grant(&publisher, principal("phone"), Capability::Owner);
    assert_eq!(refused, Err(LogError::DeviceGrantNotShareable));
}

#[test]
fn shareable_append_rejects_an_app_private_namespace() {
    let (mut log, publisher, published) = share_log();
    let refused = log.append(batch(&published, "hearth", 1));
    assert_eq!(refused, Err(LogError::AppPrivateNamespace));
    assert!(page(&log, &publisher).is_empty());
}

#[test]
fn personal_database_stores_app_private_and_rejects_a_receiver() {
    let owner = principal("owner");
    let phone = writer(&owner, "phone");
    let mut log = enabled(LogState::open(database("personal"), DatabaseKind::Personal));
    let receiver = principal("receiver");
    assert_eq!(
        log.grant(&owner, receiver.clone(), Capability::Receive),
        Err(LogError::PersonalNotShareable)
    );
    assert_eq!(
        log.grant(&owner, receiver, Capability::Submit),
        Err(LogError::PersonalNotShareable)
    );
    log.grant(&owner, owner.clone(), Capability::Owner).unwrap();
    let mut personal = batch(&phone, "hearth", 1);
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
        let writer = writer(&person, "device");
        log.grant(&publisher, person, Capability::Submit).unwrap();
        log.append(batch(&writer, "photos", 1)).unwrap();
    }
    let publisher = principal("publisher");
    assert_eq!(page(&log, &publisher).len(), 13);
}
