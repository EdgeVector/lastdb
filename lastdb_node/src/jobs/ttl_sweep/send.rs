//! Send one batch of expired keys to the erase, and drop the keys of a purge
//! that are already gone.

use fold_db::schema::types::MutationIds;
use std::collections::HashMap;

use fold_db::schema::types::operations::Mutation;
use fold_db::schema::types::KeyValue;

use super::verb::SweepVerb;
use crate::host::Host;

/// Send `keys`. A purge refuses a batch that names a key already gone, and a
/// client, a peer or another purger can remove a key after the re-check, or
/// while the batch waits. After a refused purge the sweep finds the absent
/// keys, counts them as done, drops them, and sends the rest.
///
/// Each refusal that names an absent key makes the batch smaller by at least
/// one key, so the loop ends after at most one round for each key of the batch.
/// A refusal that names no absent key is a real error and goes back to the
/// caller. A key that another purger removed is never an error of the sweep.
/// Returns the count sent and the count dropped as absent.
pub(super) async fn send_with_missing_retry(
    host: &Host,
    schema_name: &str,
    mut keys: Vec<KeyValue>,
    (verb, write_cutoff_nanos): (SweepVerb, Option<u64>),
) -> Result<(usize, usize), String> {
    let mut missing = 0;
    loop {
        let error = match purge_keys(host, schema_name, &keys, verb, write_cutoff_nanos).await {
            Ok(()) => return Ok((keys.len(), missing)),
            Err(error) => error,
        };
        if verb != SweepVerb::Purge {
            return Err(error);
        }
        let absent = host
            .db
            .mutation_manager()
            .absent_erase_targets(schema_name, &keys)
            .await
            .map_err(|check| format!("{error}; the absent-key check failed: {check}"))?;
        if absent.is_empty() {
            return Err(error);
        }
        missing += absent.len();
        keys.retain(|key| !absent.contains(key));
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = schema_name,
            absent = absent.len(),
            kept = keys.len(),
            "ttl sweep drops keys that are already gone and sends the rest of the batch"
        );
        if keys.is_empty() {
            return Ok((0, missing));
        }
    }
}

async fn purge_keys(
    host: &Host,
    schema_name: &str,
    keys: &[KeyValue],
    verb: SweepVerb,
    write_cutoff_nanos: Option<u64>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let query = fold_db::schema::types::Query::new_with_filter(
        schema_name.to_string(),
        Vec::new(),
        Some(
            fold_db::schema::types::field::HashRangeFilter::HashRangeKeys(
                keys.iter()
                    .map(|key| {
                        (
                            key.hash.clone().unwrap_or_default(),
                            key.range.clone().unwrap_or_default(),
                        )
                    })
                    .collect(),
            ),
        ),
    );
    let mut rows = host
        .db
        .query_executor()
        .query(query)
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() != keys.len() {
        return Err("erasure target disappeared before preparation".into());
    }
    let mutations: Vec<Mutation> = keys
        .iter()
        .map(|key| {
            let record = rows.remove(key).expect("every erasure key was read");
            let target = fold_db::record_replacement::RecordTarget {
                schema: schema_name.to_string(),
                key: key.clone(),
                alternates: record.alternates,
            };
            let mut mutation = Mutation::new(
                uuid::Uuid::new_v4().to_string(),
                target,
                HashMap::new(),
                host.public_key(),
                verb.mutation_type(),
                fold_db::record_replacement::RecordExpectation::Revision {
                    revision: record.revision,
                },
            );
            mutation.synchronous = verb.waits_for_lane().then_some(true);
            mutation = mutation.with_erase_caller(verb.caller());
            if let Some(nanos) = write_cutoff_nanos {
                mutation = mutation.with_erase_written_before_nanos(nanos);
            }
            mutation
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations(mutations)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
