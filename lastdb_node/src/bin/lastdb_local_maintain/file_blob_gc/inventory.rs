//! Complete physical source inventory, followed by exact old blob selection.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use fold_db::db_operations::AtomStore;
use fold_db::storage::laststore::{offline_physical_collection_pair, COMPACT_ALLOWLIST};
use fold_db::storage::traits::PhysicalScanPage;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct BlobHeader {
    blob_ref: String,
    content_sha256: String,
    stored_at: Option<String>,
    bytes_b64: String,
    size: u64,
}

#[derive(Default)]
struct Facts {
    counts: model::Counts,
    rows: Vec<model::BlobRow>,
    digests: BTreeMap<String, String>,
}

pub(super) async fn build(
    home: &Path,
    opened: &HomeStore,
    started_at: &str,
) -> Result<model::Plan, String> {
    let (e2e, _) = lastdb_node::offline_home::load_e2e_keys(home)?;
    let decoder = AtomStore::for_offline_read(
        Arc::clone(&opened.store),
        e2e.encryption_key(),
        e2e.encryption_key(),
    )
    .await
    .map_err(err)?;
    let cloud_gate = crate::reap::cloud_gate::check(home, opened)
        .await
        .map_err(err)?;
    let published = cloud_gate
        .published_maps
        .get("personal")
        .ok_or("file blob proof has no personal writer map")?;
    let snapshot_map = snapshot_fence::require(opened, published).await?;
    let mut roots = cloud_roots::read(opened, &decoder, &snapshot_map).await?;
    let names = namespace_names(opened).await?;
    let raw_store = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let crypto = crate::home::load_home_crypto(home).ok_or("home has no at-rest identity key")?;
    let mut facts = Facts::default();
    facts.counts.journal_roots = roots.records;
    facts.counts.confirmed_personal_roots_omitted = roots.omitted;
    for name in &names {
        read_collection(name, &raw_store, &crypto, &decoder, &mut roots, &mut facts).await?;
    }
    if !roots.required_atoms.is_empty() {
        return Err("a durable cloud root names an unavailable atom source".into());
    }
    if namespace_names(opened).await? != names {
        return Err("physical namespace inventory changed during the read".into());
    }
    let candidates = choose(&mut facts, &roots, started_at)?;
    Ok(model::Plan {
        format: model::FORMAT,
        home: std::fs::canonicalize(home).map_err(err)?,
        store_root: std::fs::canonicalize(&opened.store_root).map_err(err)?,
        started_at: started_at.into(),
        namespace_digests: facts.digests,
        cloud_gate,
        counts: facts.counts,
        candidates,
        retirement_state_sha256: model::retirement_state(&opened.store_root)?,
        pre_blob_snapshot_writer_map: snapshot_map,
        prerequisites: vec![
            "required external gate: authoritative normal snapshot after key reap and before this stop".into(),
            "required external gate: local and peer writers quiet through the complete maintenance window".into(),
            "required external gate: fresh normal snapshot committed before writers resume after file blob reclaim".into(),
        ],
    })
}

async fn read_collection(
    name: &str,
    raw_store: &Arc<laststore::LastStore>,
    crypto: &Arc<dyn fold_db::crypto::CryptoProvider>,
    decoder: &AtomStore,
    roots: &mut cloud_roots::Roots,
    facts: &mut Facts,
) -> Result<(), String> {
    let (raw, seam) =
        offline_physical_collection_pair(Arc::clone(raw_store), name, Arc::clone(crypto));
    let mut walker = crate::reap::walk::Walker::new(raw);
    let mut digest = Sha256::new();
    while let Some(page) = walker.next_page().await.map_err(err)? {
        hash_page(&page, &mut digest, &mut facts.counts);
        refuse_pending_roots(name, &page)?;
        let selected = page
            .rows
            .iter()
            .filter_map(|(key, _)| match source_kind(name, key) {
                Ok(Some(kind)) => Some(Ok((key.clone(), kind))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values = seam
            .get_many(selected.iter().map(|(key, _)| key.clone()).collect())
            .await
            .map_err(err)?;
        if values.len() != selected.len() {
            return Err("physical source batch count differs".into());
        }
        let mut atom_rows = Vec::new();
        let mut identities = Vec::new();
        for ((key, kind), value) in selected.into_iter().zip(values) {
            let value = value.ok_or("a physical source is hidden from the decrypted reader")?;
            match kind {
                Kind::Atom(identity) => {
                    identities.push(identity);
                    atom_rows.push(value);
                }
                Kind::Blob(reference) => {
                    let raw = page
                        .rows
                        .iter()
                        .find(|row| row.0 == key)
                        .ok_or("blob source vanished from its physical page")?;
                    facts
                        .rows
                        .push(blob_row(name, &key, &raw.1, &value, reference)?);
                }
            }
        }
        let decoded = decoder
            .decode_stored_atom_batch(&atom_rows)
            .await
            .map_err(err)?;
        for ((scope, uuid), atom) in identities.into_iter().zip(decoded) {
            if atom.uuid() != uuid {
                return Err("atom body differs from its physical key".into());
            }
            roots.required_atoms.remove(&(scope.clone(), uuid.clone()));
            pointers::retain(atom.content(), atom.metadata(), &mut roots.blobs)?;
            facts.counts.atoms_read += 1;
            *facts.counts.atom_scopes.entry(scope).or_default() += 1;
        }
    }
    facts
        .digests
        .insert(name.to_string(), fold_db::hex::hex_lower(digest.finalize()));
    Ok(())
}

enum Kind {
    Atom((String, String)),
    Blob(String),
}

fn source_kind(namespace: &str, key: &[u8]) -> Result<Option<Kind>, String> {
    if let Some(identity) = pointers::atom_identity(key)? {
        return Ok(Some(Kind::Atom(identity)));
    }
    if namespace == "atoms" {
        return Err("atoms collection has an unsupported physical source key".into());
    }
    if namespace == "cas_blobs" {
        let reference = std::str::from_utf8(key).map_err(err)?.to_string();
        pointers::valid_blob_ref(&reference)?;
        return Ok(Some(Kind::Blob(reference)));
    }
    Ok(pointers::resident_blob_ref(key)?.map(Kind::Blob))
}

fn refuse_pending_roots(namespace: &str, page: &PhysicalScanPage) -> Result<(), String> {
    if !page.rows.is_empty()
        && matches!(
            namespace,
            "sync_outbox" | "sync_capture_reexport" | "sync_upload_quarantine"
        )
    {
        return Err(
            "a durable pending or quarantined cloud source prevents file blob reclaim".into(),
        );
    }
    Ok(())
}

async fn namespace_names(opened: &HomeStore) -> Result<Vec<String>, String> {
    let mut names = opened.base.list_namespaces().await.map_err(err)?;
    names.sort();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("duplicate physical namespace".into());
    }
    Ok(names)
}

fn hash_page(page: &PhysicalScanPage, digest: &mut Sha256, counts: &mut model::Counts) {
    for (key, value) in &page.rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
        counts.physical_rows += 1;
    }
}

fn blob_row(
    namespace: &str,
    key: &[u8],
    raw: &[u8],
    plain: &[u8],
    reference: String,
) -> Result<model::BlobRow, String> {
    let stored_at = if namespace == "cas_blobs" {
        let header: BlobHeader = serde_json::from_slice(plain).map_err(err)?;
        if header.blob_ref != reference || format!("sha256:{}", header.content_sha256) != reference
        {
            return Err("file blob body identity differs from its physical key".into());
        }
        if header.bytes_b64.is_empty() && header.size != 0 {
            return Err("nonempty file blob has no stored bytes".into());
        }
        header.stored_at
    } else {
        let body: serde_json::Value = serde_json::from_slice(plain).map_err(err)?;
        let object = body
            .as_object()
            .ok_or("resident file blob has no object body")?;
        object
            .get("stored_at")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or("resident file blob has an invalid date".to_string())
            })
            .transpose()?
    };
    if let Some(date) = &stored_at {
        DateTime::parse_from_rfc3339(date).map_err(err)?;
    }
    Ok(model::BlobRow {
        collection: namespace.into(),
        key_b64: STANDARD.encode(key),
        blob_ref: reference,
        raw_sha256: model::digest(raw),
        raw_bytes: raw.len() as u64,
        stored_at,
    })
}

fn choose(
    facts: &mut Facts,
    roots: &cloud_roots::Roots,
    started_at: &str,
) -> Result<Vec<model::BlobRow>, String> {
    let started = DateTime::parse_from_rfc3339(started_at)
        .map_err(err)?
        .with_timezone(&Utc);
    if started > Utc::now() {
        return Err("file blob plan date is in the future".into());
    }
    let before = started - Duration::seconds(model::GRACE_SECONDS);
    facts.counts.file_blobs_read = facts.rows.len() as u64;
    let mut candidates = Vec::new();
    for row in facts.rows.drain(..) {
        if roots.blobs.contains(&row.blob_ref) {
            facts.counts.file_blobs_referenced += 1;
        } else if let Some(date) = &row.stored_at {
            if DateTime::parse_from_rfc3339(date)
                .map_err(err)?
                .with_timezone(&Utc)
                >= before
            {
                facts.counts.file_blobs_recent += 1;
            } else {
                if !COMPACT_ALLOWLIST.contains(&row.collection.as_str())
                    || row.collection == "atoms"
                {
                    return Err("file blob candidate is outside mutable compact collections".into());
                }
                facts.counts.candidate_stored_bytes += row.raw_bytes;
                candidates.push(row);
            }
        } else {
            facts.counts.file_blobs_undated += 1;
        }
    }
    candidates.sort_by(|a, b| (&a.collection, &a.key_b64).cmp(&(&b.collection, &b.key_b64)));
    if candidates
        .windows(2)
        .any(|pair| pair[0].collection == pair[1].collection && pair[0].key_b64 == pair[1].key_b64)
    {
        return Err("duplicate physical file blob candidate".into());
    }
    facts.counts.candidate_rows = candidates.len() as u64;
    Ok(candidates)
}
