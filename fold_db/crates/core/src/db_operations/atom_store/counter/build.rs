//! Turn the plans of one planner run into the mutations of one batch.
//!
//! The order inside the batch: bodies, counters, the blob counters, tip records
//! and other rows, and then every delete. A batch is atomic, so the order only
//! fixes which write wins when two operations name one key.
//!
//! The blob counters change in this batch too. A created atom counter that lists
//! a blob raises the blob counter, a deleted one lowers it, and a tip record that
//! names a blob in its metadata raises it, a replaced or removed one lowers it.

use super::commit::DeletedAtom;
use super::plan::{AtomPlan, SuspectKind};
use super::read::Offer;
use super::report::CounterEvent;
use super::{AtomCounter, TipCommit};
use crate::atom::atom_key_codec;
use crate::db_operations::atom_store::blob_counter::BlobDeltas;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// The mutations of the batch, and what the run did.
pub(super) async fn build_mutations(
    store: &AtomStore,
    scope: Option<&str>,
    tips: &BTreeMap<String, Option<PerKeyRecord>>,
    old_tips: &HashMap<String, PerKeyRecord>,
    (rows, deletes): (Vec<(String, Value)>, Vec<Vec<u8>>),
    plans: Vec<(String, AtomPlan)>,
    mut offers: HashMap<String, Offer>,
) -> Result<(Vec<KvMutation>, TipCommit), SchemaError> {
    let mut commit = TipCommit::default();
    let mut puts = Vec::new();
    let mut removals = Vec::new();
    let mut blob_deltas = BlobDeltas::default();
    for (atom, plan) in plans {
        let event = match plan {
            AtomPlan::Unchanged => None,
            AtomPlan::SetCounter(counter) => {
                puts.push(counter_put(scope, &atom, &counter)?);
                None
            }
            AtomPlan::Create(counter) => {
                let offer = offers
                    .remove(&atom)
                    .expect("a created atom has an offered body");
                puts.extend(create_puts(store, scope, &atom, &counter, &offer).await?);
                blob_deltas.atom_counter_created(&counter.blobs);
                commit.created.push(offer.atom);
                Some(CounterEvent::Created)
            }
            AtomPlan::Delete { blobs } => {
                removals.extend(delete_removals(scope, &atom));
                blob_deltas.atom_counter_deleted(&blobs);
                commit.deleted.push(DeletedAtom {
                    uuid: atom.clone(),
                    blobs,
                });
                Some(CounterEvent::Deleted)
            }
            AtomPlan::Suspect(counter, kind) => {
                puts.push(counter_put(scope, &atom, &counter)?);
                if kind == SuspectKind::CounterAbsent {
                    // The new counter lists blobs that the lost counter may
                    // have counted. Counting them again can only leak.
                    blob_deltas.atom_counter_created(&counter.blobs);
                }
                Some(match kind {
                    SuspectKind::Underflow => CounterEvent::Underflow,
                    SuspectKind::CounterAbsent => CounterEvent::CounterAbsent,
                })
            }
            AtomPlan::Dangling => Some(CounterEvent::Dangling),
        };
        if let Some(event) = event {
            store.counter_report().note(&atom, &event);
            commit.note_fault(&event);
        }
    }
    // A tip record that carries file metadata names a blob. The blob counters of
    // the old and the new record change in the same batch as the record.
    for (key, new) in tips {
        blob_deltas.tip_replaced(old_tips.get(key), new.as_ref());
    }
    let mut mutations = puts;
    mutations.extend(store.blob_counter_mutations(blob_deltas).await?);
    for (key, new) in tips {
        match new {
            Some(record) => mutations.push(KvMutation::put(
                key.as_bytes().to_vec(),
                to_bytes(record, "tip record")?,
            )),
            None => removals.push(KvMutation::delete(key.as_bytes().to_vec())),
        }
    }
    for (key, value) in rows {
        mutations.push(KvMutation::put(
            key.into_bytes(),
            to_bytes(&value, "batch row")?,
        ));
    }
    mutations.extend(deletes.into_iter().map(KvMutation::delete));
    mutations.extend(removals);
    Ok((mutations, commit))
}

fn counter_put(
    scope: Option<&str>,
    atom: &str,
    counter: &AtomCounter,
) -> Result<KvMutation, SchemaError> {
    let key = build_storage_key(scope, &atom_key_codec::counter_key(atom));
    Ok(KvMutation::put(
        key.into_bytes(),
        to_bytes(counter, "atom counter")?,
    ))
}

/// Body and counter of an atom that is new in the scope.
async fn create_puts(
    store: &AtomStore,
    scope: Option<&str>,
    atom: &str,
    counter: &AtomCounter,
    offer: &Offer,
) -> Result<Vec<KvMutation>, SchemaError> {
    crate::atom::enforce_atom_content_limit(offer.atom.source_schema_name(), offer.atom.content())?;
    let body = store
        .encode_atom_bytes(&offer.atom, offer.bundle_molecule.as_deref())
        .await?;
    let mut puts = vec![KvMutation::put(
        build_storage_key(scope, &atom_key_codec::body_key(atom)).into_bytes(),
        body,
    )];
    puts.push(counter_put(scope, atom, counter)?);
    Ok(puts)
}

/// Body and counter of an atom whose count reached zero.
fn delete_removals(scope: Option<&str>, atom: &str) -> Vec<KvMutation> {
    vec![
        KvMutation::delete(
            build_storage_key(scope, &atom_key_codec::counter_key(atom)).into_bytes(),
        ),
        KvMutation::delete(build_storage_key(scope, &atom_key_codec::body_key(atom)).into_bytes()),
    ]
}

fn to_bytes<T: serde::Serialize>(value: &T, what: &str) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(format!("serialize {what}: {error}")))
}
