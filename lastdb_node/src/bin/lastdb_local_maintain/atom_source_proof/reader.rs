//! Every physical namespace, exact batch reads, and immutable completion gates.

use super::*;
use fold_db::atom::{atom_key_codec, molecule_key_codec as codec};
use fold_db::storage::laststore::offline_physical_collection_pair;
use fold_db::storage::traits::KvStore;

pub(super) async fn namespace_names(opened: &HomeStore) -> Result<Vec<String>, String> {
    let mut names = opened.base.list_namespaces().await.map_err(err)?;
    names.sort();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("duplicate physical namespace".into());
    }
    Ok(names)
}

pub(super) fn atom_identity(key: &[u8]) -> Result<Option<(String, String)>, String> {
    let key = std::str::from_utf8(key).map_err(err)?;
    let Some((scope, bare)) = sources::peel_kind(key, sources::KINDS)? else {
        return Ok(None);
    };
    if !bare.starts_with("atom:") && !bare.starts_with("atom\0") {
        return Ok(None);
    }
    let uuid = atom_key_codec::uuid_of(bare)
        .filter(|id| !id.is_empty())
        .ok_or("invalid physical atom key")?;
    Ok(Some((scope.into(), uuid.into())))
}

pub(super) async fn collection(
    name: &str,
    physical: &Arc<laststore::LastStore>,
    crypto: &Arc<dyn fold_db::crypto::CryptoProvider>,
    targets: &BTreeSet<String>,
    links: &mut sources::Links,
    collected: &mut Collected,
) -> Result<(), String> {
    let (raw, seam) =
        offline_physical_collection_pair(Arc::clone(physical), name, Arc::clone(crypto));
    let mut walker = crate::reap::walk::Walker::new(Arc::clone(&raw));
    let mut hash = Sha256::new();
    let mut rows = 0;
    while let Some(page) = walker.next_page().await.map_err(err)? {
        let mut selected = Vec::new();
        for (key, value) in &page.rows {
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
            rows += 1;
            let identity = if name == "lineage_reverse" {
                None
            } else {
                atom_identity(key)?
            };
            match identity {
                Some((scope, uuid)) => {
                    if targets.contains(&uuid) {
                        selected.push((key.clone(), value.clone(), Some((scope, uuid))));
                    }
                }
                None if name == "atoms" => {
                    return Err("atoms collection has an unsupported physical key".into())
                }
                None if matches!(name, "cas_blobs" | "sync_pin_log") => {}
                None => selected.push((key.clone(), value.clone(), None)),
            }
        }
        read_selected(
            name,
            &*raw,
            &*seam,
            page.row_handle
                .map(|handle| (handle.shard, handle.group_id)),
            selected,
            targets,
            links,
            collected,
        )
        .await?;
    }
    collected
        .facts
        .namespace_digests
        .insert(name.into(), fold_db::hex::hex_lower(hash.finalize()));
    collected.facts.namespace_rows.insert(name.into(), rows);
    Ok(())
}

type Selected = (Vec<u8>, Vec<u8>, Option<(String, String)>);

async fn read_selected(
    name: &str,
    raw: &dyn KvStore,
    seam: &dyn KvStore,
    handle: Option<(u16, Option<u32>)>,
    rows: Vec<Selected>,
    targets: &BTreeSet<String>,
    links: &mut sources::Links,
    collected: &mut Collected,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let (shard, group_id) = handle.ok_or("physical source page has no row handle")?;
    let keys: Vec<_> = rows.iter().map(|row| row.0.clone()).collect();
    let (stored, plain) = tokio::join!(raw.get_many(keys.clone()), seam.get_many(keys));
    let stored = stored.map_err(err)?;
    let plain = plain.map_err(err)?;
    if stored.len() != rows.len() || plain.len() != rows.len() {
        return Err("physical source batch count differs".into());
    }
    for (((key, raw, identity), stored), plain) in rows.into_iter().zip(stored).zip(plain) {
        if stored.as_ref() != Some(&raw) {
            return Err("physical source bytes differ from the keyed read".into());
        }
        let plain = plain.ok_or("a physical source is hidden from the decrypted reader")?;
        if let Some((scope, uuid)) = identity {
            if !collected.body_keys.insert((name.into(), key.clone())) {
                return Err("duplicate target body key across physical handles".into());
            }
            collected.facts.found_target_ids.insert(uuid.clone());
            collected.facts.count("target_physical_bodies");
            if !scope.is_empty() || name != "atoms" {
                collected.facts.hold(
                    targets,
                    &uuid,
                    model::hold(name, &key, &scope, "other_scope_or_collection_body", None),
                );
            }
            collected.bodies.push(TargetBody {
                collection: name.into(),
                shard,
                group_id,
                key,
                scope,
                uuid,
                raw,
                plain,
            });
        } else {
            sources::observe(name, &key, &plain, targets, links, &mut collected.facts)?;
        }
    }
    Ok(())
}

pub(super) async fn completion(opened: &HomeStore, facts: &Facts) -> Result<(), String> {
    let keys = [
        codec::ATOM_REF_V2_COMPLETE_KEY,
        codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
    ];
    let main = opened.store.open_namespace("main").await.map_err(err)?;
    let values = main
        .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
        .await
        .map_err(err)?;
    if values.len() != keys.len() {
        return Err("compact reference completion batch differs".into());
    }
    for (key, value) in keys.into_iter().zip(values) {
        if value.as_deref() != Some(b"true".as_slice())
            || facts
                .personal_completion_markers
                .get(key)
                .map(String::as_str)
                != Some(&digest(b"true"))
        {
            return Err("personal compact reference absence requires exact physical and keyed completion markers".into());
        }
    }
    Ok(())
}
