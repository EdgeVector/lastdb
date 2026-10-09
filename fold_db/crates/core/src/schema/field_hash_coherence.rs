//! Local field-identity coherence: detect multi-key siblings, bind proteins.
//!
//! A **field identity** is `H(name, description, type, version)` — minted by
//! Schema Service when it has one, and by this node otherwise (see
//! [`DeclarativeSchemaDefinition::ensure_field_hashes`]) so coherence never
//! depends on being online.
//!
//! Field identity alone is **not** enough to bind. On a real catalog it is far
//! too coarse: `created_at` / `"RFC 3339 timestamp"` / `String` is byte-identical
//! across dozens of unrelated schemas, and every one of them keyed on `slug`.
//! Binding on identity alone would fold a `Sop` write onto a `Concept` tip at
//! the same slug. So a bind additionally requires the two schemas to be
//! **multi-key siblings of the same product** ([`are_multi_key_siblings`]):
//! different key layouts over what is demonstrably the same record shape.
//!
//! | Condition | Local action |
//! |-----------|--------------|
//! | multi-key siblings + matching field identity | one protein per field; tips fold |
//! | anything else | nothing — key layouts and molecules stay untouched |
//!
//! Both key layouts always stay addressable; a protein is coherence only, never
//! a collapse (`preference-schema-expand-same-product-different-keys`).

use std::collections::{HashMap, HashSet};

use crate::db_operations::atom_store::AtomStore;
use crate::protein::ProteinMember;
use crate::schema::types::key_config::KeyConfig;
use crate::schema::types::DeclarativeSchemaDefinition;
use crate::schema::SchemaError;
use crate::security::Ed25519KeyPair;
use serde_json::Value;

/// Diagnostic breadcrumb: field identity → the protein that carries it.
///
/// Written for observability only (`lastdb inventory` classifies the `fldprot:`
/// prefix). It is deliberately **not** an authority: an earlier revision keyed
/// one protein per field identity globally, which unioned every schema family
/// sharing a common field name into a single protein. Membership resolution goes
/// through each molecule's own `molprot:` back-reference instead.
pub fn field_protein_key(field_hash: &str, protein_uuid: &str) -> String {
    format!("fldprot:{field_hash}:{protein_uuid}")
}

/// Fingerprint of a schema's key layout for equality (not product names).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyLayoutFingerprint {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

impl KeyLayoutFingerprint {
    #[must_use]
    pub fn from_key(key: Option<&KeyConfig>) -> Self {
        match key {
            None => Self {
                hash_field: None,
                range_field: None,
            },
            Some(k) => Self {
                hash_field: k.hash_field.clone(),
                range_field: k.range_field.clone(),
            },
        }
    }

    #[must_use]
    pub fn from_schema(schema: &DeclarativeSchemaDefinition) -> Self {
        Self::from_key(schema.key.as_ref())
    }

    /// A member can only derive tip coordinates when it has a hash field.
    #[must_use]
    pub fn is_keyable(&self) -> bool {
        self.hash_field.as_deref().is_some_and(|h| !h.is_empty())
    }
}

/// Share of the smaller schema's field identities that the larger one also
/// carries. Overlap (not Jaccard) so a thin key-only index still matches the
/// wide record it indexes.
#[must_use]
pub fn overlap_coefficient(a: &HashSet<&str>, b: &HashSet<&str>) -> f64 {
    let smaller = a.len().min(b.len());
    if smaller == 0 {
        return 0.0;
    }
    a.intersection(b).count() as f64 / smaller as f64
}

/// Minimum field-identity overlap for two differently-keyed schemas to count as
/// the same product.
///
/// Measured against the live catalog (1,115 schemas): every cross-key pair at or
/// above this value is a genuine multi-key sibling (`BoardCards` by `board` vs by
/// `milestone`, `Lastgit Change Request` by `cr_key` vs by `(repo, cr_id)`), and
/// no pair spans two different apps. Unrelated products that merely share
/// `repo` / `schema_version` / `created_at` fall far below it.
pub const MULTI_KEY_SIBLING_OVERLAP: f64 = 0.6;

/// Field identities carried by a schema, declared or inferred.
#[must_use]
pub fn field_identity_hashes(schema: &DeclarativeSchemaDefinition) -> HashSet<&str> {
    schema
        .field_hashes
        .values()
        .filter(|h| !h.is_empty())
        .map(String::as_str)
        .collect()
}

/// Field identities this schema **declared** — the subset of `field_hashes`
/// whose field also carries a `declaration_id`.
///
/// Presence of the declaration is the marker for a v2, declaration-scoped
/// identity. That distinction is load-bearing and cannot be skipped: v1
/// identities are network-global (`H(name, description, type, version)`), so
/// `created_at` is byte-identical across 46 unrelated schemas on the live
/// primary. Binding on a v1 identity alone would fold unrelated products
/// together. A declared identity is scoped to the act of declaring, so a match
/// means someone actually said these are the same field.
#[must_use]
pub fn declared_field_identities(schema: &DeclarativeSchemaDefinition) -> HashSet<&str> {
    schema
        .field_declarations
        .keys()
        .filter_map(|field| schema.field_hashes.get(field))
        .filter(|h| !h.is_empty())
        .map(String::as_str)
        .collect()
}

/// Whether two schemas share at least one **declared** field identity.
///
/// This is the declared path: when it holds, the schemas were told to be
/// coherent and the similarity heuristic is not consulted at all.
#[must_use]
pub fn declared_identities_match(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> bool {
    let (da, db) = (declared_field_identities(a), declared_field_identities(b));
    !da.is_empty() && !db.is_empty() && da.intersection(&db).next().is_some()
}

/// Two apps never share a product. Absent ownership is not a conflict — local
/// and test schemas routinely carry none.
#[must_use]
fn owners_conflict(a: &DeclarativeSchemaDefinition, b: &DeclarativeSchemaDefinition) -> bool {
    match (a.owner_app_id.as_deref(), b.owner_app_id.as_deref()) {
        (Some(x), Some(y)) if !x.is_empty() && !y.is_empty() => x != y,
        _ => false,
    }
}

/// Whether two schemas are the same product reachable under different keys —
/// the only relationship that makes folding one's write onto the other's tip
/// correct.
#[must_use]
pub fn are_multi_key_siblings(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> bool {
    if a.name == b.name || owners_conflict(a, b) {
        return false;
    }
    let (la, lb) = (
        KeyLayoutFingerprint::from_schema(a),
        KeyLayoutFingerprint::from_schema(b),
    );
    // Same layout means one molecule can serve both — that is field-mapper
    // territory, and collapsing it here is what corrupts same-keyed peers.
    if la == lb || !la.is_keyable() || !lb.is_keyable() {
        return false;
    }
    let (ha, hb) = (field_identity_hashes(a), field_identity_hashes(b));
    if ha.is_empty() || hb.is_empty() {
        return false;
    }

    // The declared path. When both schemas carry a declaration-scoped identity
    // and those identities match, someone *said* these two index the same
    // record — so the similarity heuristic is not consulted at all. This is the
    // whole point: coherence is declared or it does not happen, and a declared
    // pair binds even with overlap far below the threshold.
    //
    // The four structural checks above still ran and are non-overridable. They
    // are correctness invariants, not tuning: a declaration cannot license
    // binding two same-key-layout schemas, because that is field-mapper
    // territory and binding there corrupts same-keyed peers.
    if declared_identities_match(a, b) {
        return true;
    }

    // The inference path, unchanged, for the entire existing catalog — all
    // 1,141 live schemas carry locally-minted v1 identities and no
    // declarations, so their behaviour is bit-for-bit what it was.
    //
    // Self-correcting as adoption grows: two schemas from different
    // declarations compute overlap near zero naturally, so this path gets
    // safer, not more dangerous, over time.
    overlap_coefficient(&ha, &hb) >= MULTI_KEY_SIBLING_OVERLAP
}

/// One field two sibling schemas agree on.
///
/// The two local names are carried separately because matching is on
/// **identity**, not on the local field name — a schema calling the field
/// `name` binds with one calling it `title`. For every legacy pair the two are
/// equal, because a v1 identity hashes the field name and so cannot match
/// across a rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedFieldIdentity {
    /// Local field name in the first schema.
    pub a_field: String,
    /// Local field name in the second schema.
    pub b_field: String,
    pub field_hash: String,
}

/// Fields carried by both schemas under the same identity.
///
/// Matching is on identity **alone**. The previous form additionally required
/// `b.field_hashes[field] == hash` — the same *name* as well as the same
/// identity — which silently made renames unbindable. That extra condition was
/// invisible for v1 identities (which hash the name, so a rename already
/// changes the identity) and only became wrong once a declared identity could
/// outlive a local rename.
#[must_use]
pub fn shared_field_identities(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> Vec<SharedFieldIdentity> {
    // identity -> b's local field name. A well-formed schema carries an
    // identity once; if it somehow repeats, take the lexicographically first
    // name so repeated loads bind the same way rather than by hash order.
    let mut b_by_identity: HashMap<&str, &str> = HashMap::new();
    for (field, hash) in &b.field_hashes {
        if hash.is_empty() {
            continue;
        }
        b_by_identity
            .entry(hash.as_str())
            .and_modify(|existing| {
                if field.as_str() < *existing {
                    *existing = field.as_str();
                }
            })
            .or_insert(field.as_str());
    }

    let mut shared: Vec<SharedFieldIdentity> = a
        .field_hashes
        .iter()
        .filter(|(_, h)| !h.is_empty())
        .filter_map(|(field, h)| {
            b_by_identity
                .get(h.as_str())
                .map(|b_field| SharedFieldIdentity {
                    a_field: field.clone(),
                    b_field: (*b_field).to_string(),
                    field_hash: h.clone(),
                })
        })
        .collect();
    // Stable order so repeated loads bind in the same sequence.
    shared.sort_by(|x, y| x.a_field.cmp(&y.a_field));
    shared
}

/// Bind two differently-keyed field molecules into one protein.
///
/// Adopts whichever protein either molecule already belongs to, so a molecule
/// bound by an earlier load (or by a pre-cutover app) is joined rather than
/// fought over. Returns `Ok(None)` when the two are already bound to *different*
/// proteins — merging those would move tips that another member still owns.
///
/// **Already-bound fast path.** When both molecules resolve to the same protein
/// AND that protein already carries both members under these exact layouts,
/// there is nothing to establish: return `Ok(None)` before touching the store
/// again. Without it, a steady-state call still paid four more reads (two
/// `protein_of_molecule` + two `protein_get`, inside the two idempotent
/// `protein_add_member` calls) and — worse — one unconditional durable
/// `put_item` of the `fldprot:` breadcrumb.
///
/// That breadcrumb write is why the fast path matters rather than merely being
/// tidy. It is append-only and rewrites the *same key with the same value*, so
/// on a caller that runs per write (see `apply_field_hash_coherence_on_load`)
/// it accumulates one historical segment record per mutation forever, for a
/// value the module docs above already say is never read back as an authority.
/// This is the same append-only amplification `store_schema`'s
/// skip-if-unchanged was added to stop on the `schemas` plane.
pub async fn bind_cross_key_field_protein(
    store: &AtomStore,
    field_hash: &str,
    source_mol: &str,
    source_layout: &KeyLayoutFingerprint,
    target_mol: &str,
    target_layout: &KeyLayoutFingerprint,
) -> Result<Option<String>, SchemaError> {
    let src_hf = source_layout
        .hash_field
        .as_deref()
        .ok_or_else(|| SchemaError::InvalidData("source key missing hash_field".into()))?;
    let tgt_hf = target_layout
        .hash_field
        .as_deref()
        .ok_or_else(|| SchemaError::InvalidData("target key missing hash_field".into()))?;
    if source_mol.is_empty() || target_mol.is_empty() {
        return Ok(None);
    }

    let bound_src = store.protein_of_molecule(source_mol).await?;
    let bound_tgt = store.protein_of_molecule(target_mol).await?;
    let protein_uuid = match (bound_src, bound_tgt) {
        (Some(a), Some(b)) if a == b => {
            // Both already on the same protein. Confirm the membership is the
            // one we would write before skipping — a molecule can be bound to
            // this protein under a *different* layout, and that case still
            // needs the additional conformation added below.
            if let Some(protein) = store.protein_get(&a).await? {
                let src_present = protein
                    .member_for_layout(source_mol, src_hf, source_layout.range_field.as_deref())
                    .is_some();
                let tgt_present = protein
                    .member_for_layout(target_mol, tgt_hf, target_layout.range_field.as_deref())
                    .is_some();
                if src_present && tgt_present {
                    return Ok(None);
                }
            }
            a
        }
        (Some(_), Some(_)) => return Ok(None),
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => store.protein_create().await?.uuid,
    };

    store
        .protein_add_member(
            &protein_uuid,
            ProteinMember::new(source_mol, src_hf, source_layout.range_field.clone()),
        )
        .await?;
    store
        .protein_add_member(
            &protein_uuid,
            ProteinMember::new(target_mol, tgt_hf, target_layout.range_field.clone()),
        )
        .await?;

    // Breadcrumb only — never read back as an authority (see `field_protein_key`).
    store
        .raw()
        .put_item(&field_protein_key(field_hash, &protein_uuid), &field_hash)
        .await
        .map_err(|e| SchemaError::InvalidData(format!("write fldprot: {e}")))?;

    Ok(Some(protein_uuid))
}

/// Bind two sibling schemas' record molecules into one protein.
///
/// No-op when either catalog has no R yet (uncompacted). Field proteins stay.
/// Fold of a compacted write only proceeds when both record molecules are
/// members (`fold_protein_siblings_after_write`).
pub async fn bind_cross_key_record_protein(
    store: &AtomStore,
    source: &DeclarativeSchemaDefinition,
    target: &DeclarativeSchemaDefinition,
) -> Result<Option<String>, SchemaError> {
    let Some(src_r) = source.molecule_uuid.as_deref() else {
        return Ok(None);
    };
    let Some(tgt_r) = target.molecule_uuid.as_deref() else {
        return Ok(None);
    };
    if !are_multi_key_siblings(source, target) {
        return Ok(None);
    }
    bind_cross_key_field_protein(
        store,
        "record-molecule",
        src_r,
        &KeyLayoutFingerprint::from_schema(source),
        tgt_r,
        &KeyLayoutFingerprint::from_schema(target),
    )
    .await
}

/// Write content via entry molecule under a field-hash protein (sync fold).
pub async fn protein_write_field_with_sync_fold(
    store: &AtomStore,
    entry_molecule_uuid: &str,
    fields: &HashMap<String, String>,
    content: Value,
    keypair: &Ed25519KeyPair,
) -> Result<(String, String), SchemaError> {
    let outcome = store
        .protein_write_via_member(entry_molecule_uuid, fields, content, keypair)
        .await?;
    if outcome.fold_jobs_enqueued > 0 {
        let _ = store
            .protein_process_folds(outcome.fold_jobs_enqueued.saturating_mul(4).max(16))
            .await?;
    }
    Ok((outcome.protein_uuid, outcome.atom_uuid))
}
