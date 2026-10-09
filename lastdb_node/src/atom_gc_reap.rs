//! Eligibility rules for the offline atom GC reaper.
//!
//! The reaper is the destructive half of `lastdb_local_maintain atom-gc-audit`:
//! the audit inventories duplicate atom-body copies left behind by the
//! partition-prefix rekey, this decides which of them may actually be removed.
//! Schema → Molecule → Atom → file blob is untouched — only redundant *storage
//! keys* for a body that already lives somewhere else are dropped.
//!
//! ## Why the rules live here and not in the walk
//!
//! Every rule is a pure function of one group's facts, so the dangerous part of
//! a destructive pass is testable without a store. [`classify_group`] never
//! touches I/O; the CLI supplies the facts and executes the verdict.
//!
//! ## What makes a copy safe to delete
//!
//! A body is reachable through the ladder in
//! [`fold_db::db_operations::AtomStore::get_atoms_located`]:
//!
//! 1. the key built from the home's [`AtomKeyEncoding`] plus the caller's
//!    partition hint;
//! 2. the flat key `atom:{uuid}`, for a body the rekey has not reached;
//! 3. the `aloc:{uuid}` locator row, for a caller with no hint (the uuid-only
//!    `GET /api/atom/{uuid}` surface).
//!
//! So a copy may only be deleted when a *survivor* copy stays reachable by that
//! ladder for every caller shape — which is why the `PartitionPrefix` rule
//! demands a locator naming the survivor's partition before it will drop the
//! flat copy: without one, step 3 is the only path a hintless reader has, and
//! deleting the flat key would take away step 2 as well.
//!
//! ## Everything else fails closed
//!
//! Content-hash disagreement, an unreadable body, more than one candidate
//! survivor, a survivor in the wrong shape for the home's encoding — each is
//! [`ReapVerdict::Ambiguous`], which deletes nothing and is reported. The rules
//! never guess which of two disagreeing bodies is the real one.

use std::collections::BTreeSet;

/// The storage-key shape of one atom-body copy.
///
/// Mirrors `fold_db::atom::atom_key_codec`: a body key is either `atom:{uuid}`
/// or `atom:{partition}{uuid}`, where the partition is the `mk:{M}:{esc(hash)}\0`
/// prefix its owning tip lives under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomKeyShape {
    /// `atom:{uuid}` — no locality prefix.
    Flat,
    /// `atom:{partition}{uuid}` — co-located with the owning slot's tips.
    Prefixed {
        /// The partition prefix, separator included.
        partition: String,
    },
}

impl AtomKeyShape {
    /// Classify a BASE body key (`atom:…`, no `{storage_prefix}:`).
    ///
    /// The uuid is everything after the last separator, so a key that carries
    /// one is prefixed and everything before the last separator (inclusive) is
    /// the partition — the same split `atom_key_codec::uuid_of_suffix` makes.
    #[must_use]
    pub fn of_base_key(base_key: &str) -> Self {
        let Some(suffix) = fold_db::kind_partition::rest_of(base_key, "atom")
            .or_else(|| base_key.strip_prefix("atom:"))
        else {
            return Self::Flat;
        };
        match suffix.rsplit_once('\0') {
            Some((partition, _uuid)) => Self::Prefixed {
                partition: format!("{partition}\0"),
            },
            None => Self::Flat,
        }
    }

    /// The partition a prefixed key names.
    #[must_use]
    pub fn partition(&self) -> Option<&str> {
        match self {
            Self::Prefixed { partition } => Some(partition),
            Self::Flat => None,
        }
    }
}

/// The atom-body key encoding a home is written under, as resolved from its
/// durable marker rather than from this process's environment.
///
/// A copy of `fold_db::atom::AtomKeyEncoding` deliberately kept local: the
/// reaper resolves the encoding by reading the home's
/// `amigr:atom_key_encoding_v1` row offline, never by consulting
/// `LASTDB_ATOM_KEY_ENCODING` — an env var that happens to be set in the
/// operator's shell must not decide what a destructive pass deletes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeAtomKeyEncoding {
    /// Bodies are addressed `atom:{uuid}`.
    Flat,
    /// Bodies are addressed `atom:{partition}{uuid}`.
    PartitionPrefix,
}

/// One stored copy of an atom body.
#[derive(Debug, Clone)]
pub struct AtomCopy {
    /// LastStore namespace the row lives in (`atoms`, `main`, …).
    pub namespace: String,
    /// `{namespace}:{stored_key}` — the identity the rules compare copies by.
    ///
    /// Qualified rather than bare, because one key can exist in two namespaces
    /// and a survivor must be distinguishable from a duplicate that merely
    /// shares its name.
    pub key: String,
    /// The key as actually stored, for the delete.
    pub stored_key: String,
    /// The BASE key (`atom:…`) this row is addressed by.
    pub base_key: String,
    /// Flat or partition-prefixed.
    pub shape: AtomKeyShape,
    /// SHA-256 of the body's **canonical decoded form**.
    ///
    /// `None` means the body could not be read as content: absent, or opaque at
    /// rest because the home was opened without its at-rest key. Both are
    /// [`ReapVerdict::Ambiguous`] — never a delete. Hashing raw at-rest bytes
    /// would be worse than useless here: the seam seals with a fresh nonce per
    /// write, so two byte-identical bodies hash differently and two *different*
    /// bodies could never be told apart from that fact.
    pub content_sha256: Option<String>,
}

/// Everything the rules need to know about one atom uuid.
#[derive(Debug, Clone)]
pub struct AtomGroup {
    /// The atom uuid every copy in this group addresses.
    pub atom_uuid: String,
    /// Each stored copy of the body.
    pub copies: Vec<AtomCopy>,
    /// Whether any live tip / version / history / conflict / legacy-ref row
    /// names this uuid.
    pub referenced: bool,
    /// The partition named by this uuid's `aloc:` locator row, if it has one.
    pub locator_partition: Option<String>,
}

/// What the rules decided for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReapVerdict {
    /// Nothing to reclaim; every copy stays.
    Keep {
        /// Why the group was left alone.
        reason: &'static str,
    },
    /// Redundant copies may be removed, `survivor_key` stays.
    Delete {
        /// The key that must remain readable after the pass.
        survivor_key: String,
        /// Keys to remove. Never contains `survivor_key`.
        delete_keys: Vec<String>,
        /// Why the deletion is safe.
        reason: &'static str,
    },
    /// The group could be redundant but the rules cannot prove it. Deletes
    /// nothing and is surfaced in the summary.
    Ambiguous {
        /// What could not be proven.
        reason: &'static str,
    },
}

impl ReapVerdict {
    /// The keys this verdict removes.
    #[must_use]
    pub fn delete_keys(&self) -> &[String] {
        match self {
            Self::Delete { delete_keys, .. } => delete_keys,
            Self::Keep { .. } | Self::Ambiguous { .. } => &[],
        }
    }
}

/// Knobs a destructive pass may set. Both default to the safe end.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReapPolicy {
    /// Also delete bodies that no live row references at all.
    ///
    /// Off by default, and deliberately separate from duplicate reclaim: a
    /// duplicate delete is provably lossless (an identical body survives under
    /// another key), whereas an orphan delete is only as good as the reference
    /// scan that declared it unreferenced. One is arithmetic, the other is a
    /// judgement about scan completeness, so they do not share a flag.
    pub reap_unreferenced_orphans: bool,
}

/// Decide what may be removed for one atom uuid.
///
/// Pure: every input is in `group`, `encoding`, and `policy`.
#[must_use]
pub fn classify_group(
    group: &AtomGroup,
    encoding: HomeAtomKeyEncoding,
    policy: ReapPolicy,
) -> ReapVerdict {
    if group.copies.is_empty() {
        return ReapVerdict::Ambiguous {
            reason: "no-stored-copies",
        };
    }

    // An unreadable body is never evidence of anything. Checked before the
    // single-copy branch so an opaque orphan cannot be deleted on the strength
    // of a hash nobody could compute.
    if group.copies.iter().any(|c| c.content_sha256.is_none()) {
        return ReapVerdict::Ambiguous {
            reason: "missing-or-unreadable-body",
        };
    }

    if group.copies.len() == 1 {
        return classify_single_copy(group, policy);
    }

    let hashes: BTreeSet<&str> = group
        .copies
        .iter()
        .filter_map(|c| c.content_sha256.as_deref())
        .collect();
    if hashes.len() > 1 {
        return ReapVerdict::Ambiguous {
            reason: "content-hash-conflict",
        };
    }

    match encoding {
        HomeAtomKeyEncoding::PartitionPrefix => classify_duplicates_partition_prefix(group),
        HomeAtomKeyEncoding::Flat => classify_duplicates_flat(group),
    }
}

fn classify_single_copy(group: &AtomGroup, policy: ReapPolicy) -> ReapVerdict {
    if group.referenced {
        return ReapVerdict::Keep {
            reason: "referenced-single-copy",
        };
    }
    if !policy.reap_unreferenced_orphans {
        return ReapVerdict::Keep {
            reason: "unreferenced-orphan-retained-by-policy",
        };
    }
    ReapVerdict::Delete {
        // An orphan has no survivor by construction: nothing references it and
        // no other copy exists. Named explicitly so the summary cannot be read
        // as "a copy remains".
        survivor_key: String::new(),
        delete_keys: group.copies.iter().map(|c| c.key.clone()).collect(),
        reason: "unreferenced-orphan",
    }
}

/// Under `PartitionPrefix` the prefixed copy is canonical and the flat copy is
/// the migration's leftover.
fn classify_duplicates_partition_prefix(group: &AtomGroup) -> ReapVerdict {
    let prefixed: Vec<&AtomCopy> = group
        .copies
        .iter()
        .filter(|c| matches!(c.shape, AtomKeyShape::Prefixed { .. }))
        .collect();

    match prefixed.len() {
        // Only flat copies under an encoding that reads prefixed keys first:
        // there is no canonical survivor to keep, so keep everything.
        0 => ReapVerdict::Ambiguous {
            reason: "no-prefixed-copy-under-partition-prefix-encoding",
        },
        1 => {
            let survivor = prefixed[0];
            let partition = survivor
                .shape
                .partition()
                .expect("prefixed copies carry a partition");
            // The flat key is step 2 of the read ladder. Dropping it leaves a
            // hintless reader with step 3 alone, so the locator must already
            // name exactly this survivor.
            if group.locator_partition.as_deref() != Some(partition) {
                return ReapVerdict::Ambiguous {
                    reason: "locator-does-not-name-the-surviving-partition",
                };
            }
            let delete_keys: Vec<String> = group
                .copies
                .iter()
                .filter(|c| c.key != survivor.key)
                .map(|c| c.key.clone())
                .collect();
            if delete_keys.is_empty() {
                return ReapVerdict::Keep {
                    reason: "single-prefixed-copy",
                };
            }
            ReapVerdict::Delete {
                survivor_key: survivor.key.clone(),
                delete_keys,
                reason: "content-hash-agreed-duplicate-of-located-prefixed-body",
            }
        }
        // Two partitions for one uuid: which one a reader's hint names depends
        // on the tip it came from, and the locator can only vouch for one.
        _ => ReapVerdict::Ambiguous {
            reason: "multiple-prefixed-partitions",
        },
    }
}

/// Under `Flat` the flat copy is the only one any read path builds; prefixed
/// copies are residue from a migration this home did not adopt.
fn classify_duplicates_flat(group: &AtomGroup) -> ReapVerdict {
    let flat: Vec<&AtomCopy> = group
        .copies
        .iter()
        .filter(|c| matches!(c.shape, AtomKeyShape::Flat))
        .collect();

    if flat.len() != 1 {
        return ReapVerdict::Ambiguous {
            reason: "expected-exactly-one-flat-copy-under-flat-encoding",
        };
    }
    let survivor = flat[0];
    let delete_keys: Vec<String> = group
        .copies
        .iter()
        .filter(|c| c.key != survivor.key)
        .map(|c| c.key.clone())
        .collect();
    ReapVerdict::Delete {
        survivor_key: survivor.key.clone(),
        delete_keys,
        reason: "content-hash-agreed-prefixed-residue-under-flat-encoding",
    }
}

/// The molecule uuid a partition prefix names — the `M` in `mk:{M}:{hash}\0`.
///
/// Used only for attribution in the summary, so a prefix that does not parse is
/// `None` rather than an error.
#[must_use]
pub fn molecule_uuid_of_partition(partition: &str) -> Option<&str> {
    let rest = partition.strip_prefix("mk:")?;
    let (molecule, _) = rest.split_once(':')?;
    if molecule.is_empty() {
        return None;
    }
    Some(molecule)
}
