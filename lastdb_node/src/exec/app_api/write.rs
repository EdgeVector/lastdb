//! `POST /v1/write`: a list of conditional mutations, committed all or nothing.
//!
//! A `create` writes only when no record holds the key. A `replace` and a
//! `delete` name the revision the caller read and write only when the record
//! is still at it. A mutation whose condition does not hold refuses the list
//! and names that mutation.

mod delete_alternates;

use std::collections::{BTreeMap, HashMap};

use fold_db::access::AccessContext;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, FileWrite, KeyValue, Mutation, MutationType};
use serde::Deserialize;
use serde_json::{json, Value};

use super::refusal::{conflict_error, name_conflict, WrittenKey, ABSENT};
use super::{bad, fail, ok, require_exact_name, require_write};
use crate::exec::mutation::commit_mutations;
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

pub(super) type Alternates = BTreeMap<String, Option<KeyValue>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody {
    mutations: Vec<WriteItem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteItem {
    #[serde(default)]
    pub(super) database: Option<String>,
    pub(super) schema: String,
    pub(super) op: Op,
    pub(super) key: KeyValue,
    #[serde(default)]
    pub(super) record: Option<HashMap<String, Value>>,
    #[serde(default)]
    pub(super) alternates: Alternates,
    #[serde(default)]
    pub(super) revision: Option<String>,
    /// Absent keeps a replace's file. JSON null clears it. An object links a path.
    #[serde(default, deserialize_with = "file_input")]
    pub(super) file: FileInput,
}

#[derive(Default)]
pub(super) enum FileInput {
    #[default]
    Absent,
    Clear,
    Link(FileLink),
}

fn file_input<'de, D>(deserializer: D) -> Result<FileInput, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<FileLink>::deserialize(deserializer)? {
        None => FileInput::Clear,
        Some(link) => FileInput::Link(link),
    })
}

/// A link to a file on disk that the caller can read. No bytes pass through
/// the request, and LastDB keeps a reference in place of a copy.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileLink {
    path: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Op {
    Create,
    Replace,
    Delete,
}

pub(in crate::exec) async fn write_route(
    req: &UdsRequest,
    _ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match write(req, host).await {
        Ok(body) => ok(&body),
        Err(error) => fail(&error),
    }
}

async fn write(req: &UdsRequest, host: &Host) -> Result<Value, HostError> {
    let items = parse(req)?;
    run(host, items).await
}

pub(super) async fn run(host: &Host, mut items: Vec<WriteItem>) -> Result<Value, HostError> {
    for (index, item) in items.iter().enumerate() {
        require_write(&item.schema).map_err(|error| error.at(index))?;
    }
    let database = items.first().and_then(|item| item.database.clone());
    let _shared_lock = super::write_signed::lock_shared_write(host, database.as_deref()).await;
    let (mut revisions, signed) = super::database::write_in(database.as_deref(), host, async {
        let shared = super::write_signed::is_shared_target(host, database.as_deref());
        for (index, item) in items.iter_mut().enumerate() {
            require_exact_name(host, &item.schema).map_err(|error| error.at(index))?;
            if let Some(canonical) =
                resolve_schema_name(host, &item.schema).map_err(|error| error.at(index))?
            {
                item.schema = canonical;
            }
            if shared {
                let has_file = !matches!(item.file, FileInput::Absent);
                super::write_signed::check_shared_item(index, has_file, &item.key)?;
            }
        }
        let store = host.records();
        let signed = if shared {
            super::write_signed::plan(
                &store,
                items.iter().map(|item| super::write_signed::SignedInput {
                    schema: item.schema.as_str(),
                    key: &item.key,
                    op: mutation_type(item.op),
                    record: item.record.as_ref(),
                    alternates: &item.alternates,
                }),
            )
        } else {
            Vec::new()
        };
        let located = located_keys(&items);
        let mut mutations = prepare(host, items).await?;
        if shared {
            let writer =
                mutation_log::NodeKey::from_bytes(host.keypair.public_key_bytes()).to_hex();
            for mutation in &mut mutations {
                mutation.writer = Some(writer.clone());
            }
        }
        let receipt = commit_mutations(host, mutations)
            .await
            .map_err(|error| name_conflict(error, &located))?;
        let revisions: Vec<Value> = receipt
            .mutation_ids
            .iter()
            .map(|id| json!(receipt.record_revisions.get(id)))
            .collect();
        Ok((revisions, signed))
    })
    .await?;
    // Owned databases, leased received databases, and the personal database:
    // mirror onto the signed log while holding `_shared_lock`.
    super::write_signed::mirror_committed(host, database.as_deref(), &signed, &mut revisions)
        .await?;
    Ok(json!({ "written": revisions.len(), "revisions": revisions }))
}

async fn prepare(host: &Host, items: Vec<WriteItem>) -> Result<Vec<Mutation>, HostError> {
    let mut mutations = Vec::with_capacity(items.len());
    for (index, mut item) in items.into_iter().enumerate() {
        // A delete names every declared alternate. The node reads the live
        // record's addresses. When no record is there, a shape-valid stand-in
        // lets the commit report that absence with the other conditions.
        let alternates = if item.op == Op::Delete {
            match delete_alternates::stored_alternates(host, &item).await {
                Ok(found) => found,
                Err(error) if error.reason == Some(ABSENT) => {
                    return Err(conflict_error(index, ABSENT));
                }
                Err(error) => return Err(error.at(index)),
            }
        } else {
            std::mem::take(&mut item.alternates)
        };
        let file = std::mem::replace(&mut item.file, FileInput::Absent);
        let mut mutation = build(item, alternates);
        match file {
            FileInput::Link(link) => super::write_file::link_file(
                host,
                &mut mutation,
                link.path,
                link.name,
                link.media_type,
            )
            .await
            .map_err(|error| error.at(index))?,
            FileInput::Clear => mutation.file_write = Some(FileWrite::Clear),
            FileInput::Absent => {}
        }
        mutations.push(mutation);
    }
    Ok(mutations)
}

fn located_keys(items: &[WriteItem]) -> Vec<WrittenKey> {
    items
        .iter()
        .map(|item| WrittenKey {
            schema: item.schema.clone(),
            key: item.key.to_storage_key(),
        })
        .collect()
}

fn parse(req: &UdsRequest) -> Result<Vec<WriteItem>, HostError> {
    let parsed: WriteBody = serde_json::from_slice(&req.body)
        .map_err(|error| bad(format!("invalid /v1/write body: {error}")))?;
    if parsed.mutations.is_empty() {
        return Err(bad("mutations must hold at least one mutation"));
    }
    for index in 0..parsed.mutations.len() {
        if parsed.mutations[index].database != parsed.mutations[0].database {
            return Err(bad("every mutation in one write names the same database").at(index));
        }
        let item = &parsed.mutations[index];
        let (record, revision) = (item.record.is_some(), item.revision.is_some());
        let shape = match item.op {
            Op::Create if matches!(item.file, FileInput::Clear) => {
                Err("a create names a file by path or omits it")
            }
            Op::Create if record && !revision => Ok(()),
            Op::Create => Err("a create carries a record and no revision"),
            Op::Replace if record && revision => Ok(()),
            Op::Replace => Err("a replace carries a record and the revision it replaces"),
            Op::Delete if !matches!(item.file, FileInput::Absent) => {
                Err("a delete carries no file")
            }
            Op::Delete if !record && revision && item.alternates.is_empty() => Ok(()),
            Op::Delete => Err("a delete carries the revision it deletes and no record"),
        };
        if let Err(message) = shape {
            return Err(bad(message).at(index));
        }
    }
    Ok(parsed.mutations)
}

fn mutation_type(op: Op) -> MutationType {
    match op {
        Op::Create => MutationType::Create,
        Op::Replace => MutationType::Replace,
        Op::Delete => MutationType::Delete,
    }
}

fn build(item: WriteItem, alternates: Alternates) -> Mutation {
    let expected = item.revision.map_or(CasExpectation::Absent, |revision| {
        CasExpectation::Revision { revision }
    });
    let target = RecordTarget {
        schema: item.schema,
        key: item.key,
        alternates,
    };
    Mutation::with_uuid(
        String::new(),
        target,
        item.record.unwrap_or_default(),
        mutation_type(item.op),
        expected,
    )
}
