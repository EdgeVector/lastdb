//! Authoritative source types and independent active/pending reference holds.

use super::*;
use fold_db::atom::{molecule_key_codec as codec, AtomEntry, MutationEvent};
use fold_db::db_operations::atom_store::reap_keys::{
    offline_atom_ref_root, offline_history_atom_roots, reap_tip_sources, OfflineAtomRefRoot,
};
use fold_db::db_operations::atom_store::TipVersionBackref;
use fold_db::kind_partition::{anchored, form_twin};
use serde_json::Value;

pub(super) const KINDS: &[&str] = &[
    "atom:",
    "atom\0",
    "mk:",
    "mh:",
    "mgr:v1:",
    "mgd:v1:",
    "mgp:v1:",
    "tv:",
    "tv\0",
    "tvr:",
    "tvr-meta:",
    "history:",
    "history\0",
    "conflict:",
    "conflict\0",
    "ref:",
    "aref:",
    "aref\0",
    "protein:",
    "molprot:",
    "fldprot:",
    "pfq:",
    "mord:",
    "mord\0",
    "moc:",
    "moc\0",
    "mcc:",
    "rdel:",
    "aloc:",
    "aloc\0",
    "bref:",
    "mref:",
    "amigr:",
    "dellog:",
    "dellog\0",
    "cas_blob:",
    "gcatoms-meta:",
    "gcatoms-probe-ref\0",
    "gcatoms-probe-tv-skip:",
];

pub(super) fn peel_kind<'a>(
    key: &'a str,
    kinds: &[&str],
) -> Result<Option<(&'a str, &'a str)>, String> {
    let at = kinds
        .iter()
        .filter_map(|kind| {
            key.match_indices(kind)
                .find(|(at, _)| *at == 0 || key.as_bytes().get(at - 1) == Some(&b':'))
                .map(|(at, _)| at)
        })
        .min();
    let Some(at) = at else {
        return Ok(None);
    };
    let scope = if at == 0 { "" } else { &key[..at - 1] };
    if scope.contains('\0') {
        return Err("source has an invalid storage scope".into());
    }
    Ok(Some((scope, &key[at..])))
}

#[derive(Default)]
pub(super) struct Links {
    versions: BTreeSet<(String, String)>,
    previous: BTreeSet<(String, String, model::Hold)>,
}

impl Links {
    fn entry(
        &mut self,
        targets: &BTreeSet<String>,
        entry: AtomEntry,
        hold: model::Hold,
        facts: &mut Facts,
    ) -> Result<(), String> {
        if entry.atom_uuid.is_empty() {
            return Err("source entry has an empty atom identity".into());
        }
        facts.hold(targets, &entry.atom_uuid, hold.clone());
        if !entry.prev_tip_id.is_empty() {
            self.previous
                .insert((hold.scope.clone(), entry.prev_tip_id, hold));
        }
        Ok(())
    }
    pub(super) fn finish(
        self,
        targets: &BTreeSet<String>,
        facts: &mut Facts,
    ) -> Result<(), String> {
        for (scope, link, mut hold) in self.previous {
            if self.versions.contains(&(scope, link.clone())) {
                continue;
            }
            if link.len() == 64
                && link
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                hold.kind = "legacy_previous_atom".into();
                facts.hold(targets, &link, hold);
            } else {
                return Err(
                    "a source has an unavailable or unsupported previous tip version".into(),
                );
            }
        }
        Ok(())
    }
}

pub(super) fn observe(
    collection: &str,
    key: &[u8],
    plain: &[u8],
    targets: &BTreeSet<String>,
    links: &mut Links,
    facts: &mut Facts,
) -> Result<(), String> {
    if auxiliary::lineage(collection, key, plain, targets, facts)? {
        return Ok(());
    }
    let text = std::str::from_utf8(key).map_err(err)?;
    let (scope, bare) = peel_kind(text, KINDS)?.unwrap_or(("", text));
    if bare.starts_with("aref:") || bare.starts_with("aref\0") {
        return references(collection, key, scope, bare, plain, targets, facts);
    }
    if bare.starts_with("mk:") || bare.starts_with("mgr:v1:") || bare.starts_with("mgd:v1:") {
        facts.count("tip_source_rows");
        for source in reap_tip_sources(bare, plain).map_err(err)? {
            let hold = model::hold(
                collection,
                key,
                scope,
                "current_or_shadow_tip",
                Some(&source.molecule_uuid),
            );
            links.entry(targets, source.entry, hold, facts)?;
        }
        return Ok(());
    }
    if let Some(version) = bare
        .strip_prefix("tv:")
        .or_else(|| bare.strip_prefix("tv\0"))
    {
        if version.is_empty() {
            return Err("empty authoritative tip version".into());
        }
        links.versions.insert((scope.into(), version.into()));
        facts.count("version_rows");
        let entry: AtomEntry = serde_json::from_slice(plain).map_err(err)?;
        return links.entry(
            targets,
            entry,
            model::hold(collection, key, scope, "authoritative_version", None),
            facts,
        );
    }
    if bare.starts_with(codec::TIP_VERSION_BACKREF_PREFIX) {
        let backref: TipVersionBackref = serde_json::from_slice(plain).map_err(err)?;
        if bare != codec::tip_version_backref_key(&backref.atom_uuid, &backref.version_id)
            || backref.atom_uuid.is_empty()
        {
            return Err("version backref key differs from its value".into());
        }
        facts.count("version_backref_rows");
        facts.hold(
            targets,
            &backref.atom_uuid,
            model::hold(
                collection,
                key,
                scope,
                "version_backref",
                Some(&backref.molecule_uuid),
            ),
        );
        return Ok(());
    }
    if bare.starts_with("history:") || bare.starts_with("history\0") {
        return history(collection, key, scope, bare, plain, targets, facts);
    }
    if bare.starts_with("conflict:") || bare.starts_with("conflict\0") {
        return conflict(collection, key, scope, bare, plain, targets, facts);
    }
    ownership::row(collection, key, scope, bare, plain, targets, facts)
}

fn references(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    match offline_atom_ref_root(bare, plain).map_err(err)? {
        OfflineAtomRefRoot::Active { atom_uuid, kind } => {
            facts.count(kind);
            facts.hold(
                targets,
                &atom_uuid,
                model::hold(collection, key, scope, kind, None),
            );
        }
        OfflineAtomRefRoot::Pending { atom_uuid } => {
            facts.count("pending_ref_rows");
            facts.hold(
                targets,
                &atom_uuid,
                model::hold(collection, key, scope, "pending_ref", None),
            );
        }
        OfflineAtomRefRoot::CatalogTransition {
            atom_uuids,
            transitions,
        } => {
            if transitions != 0 {
                return Err(
                    "unfinished database catalog transition prevents target atom proof".into(),
                );
            }
            facts.count("catalog_transition_rows");
            for uuid in atom_uuids {
                facts.hold(
                    targets,
                    &uuid,
                    model::hold(collection, key, scope, "catalog_transition", None),
                );
            }
        }
        OfflineAtomRefRoot::Completion { .. } if scope.is_empty() => {
            if facts
                .personal_completion_markers
                .insert(
                    form_twin(bare)
                        .filter(|_| bare.starts_with("aref\0"))
                        .unwrap_or_else(|| bare.into()),
                    digest(plain),
                )
                .is_some()
            {
                return Err("duplicate personal completion marker".into());
            }
        }
        _ => {}
    }
    Ok(())
}

fn history(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    let event: MutationEvent = serde_json::from_slice(plain).map_err(err)?;
    let prefix = codec::history_molecule_prefix(&event.molecule_uuid);
    if !bare.starts_with(&prefix) && !form_twin(&prefix).is_some_and(|twin| bare.starts_with(&twin))
    {
        return Err("history source key differs from its molecule".into());
    }
    facts.count("history_rows");
    for uuid in offline_history_atom_roots(std::str::from_utf8(key).map_err(err)?, &event) {
        if uuid.is_empty() {
            return Err("history has an empty atom identity".into());
        }
        facts.hold(
            targets,
            &uuid,
            model::hold(
                collection,
                key,
                scope,
                "history_new_old_or_loser",
                Some(&event.molecule_uuid),
            ),
        );
    }
    Ok(())
}

fn conflict(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    let conflict: fold_db::SyncConflict = serde_json::from_slice(plain).map_err(err)?;
    let expected = anchored("conflict", &conflict.id);
    if bare != expected && form_twin(&expected).as_deref() != Some(bare) {
        return Err("conflict source key differs from its value".into());
    }
    facts.count("conflict_rows");
    for uuid in [&conflict.winner_atom, &conflict.loser_atom] {
        if uuid.is_empty() {
            return Err("conflict has an empty atom identity".into());
        }
        facts.hold(
            targets,
            uuid,
            model::hold(
                collection,
                key,
                scope,
                "conflict_winner_or_loser",
                Some(&conflict.molecule_uuid),
            ),
        );
    }
    Ok(())
}

pub(super) fn json(plain: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(plain).map_err(err)
}
