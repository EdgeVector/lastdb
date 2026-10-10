//! The one pass over every group of the `tips` collection.
//!
//! Two readers walk the groups in step. The raw reader returns the stored
//! bytes. The seam reader returns the decrypted values. An un-enveloped row is
//! skipped by the seam reader without an error, so a page where the two
//! counts differ stops the plan. For each key the pass:
//!
//! - classifies the key and counts it by class,
//! - checks the key against the rules (the matcher that writes the rules),
//! - for a doomed `mk:` row, decodes the value, checks `written_at` against the
//!   drop time of its molecule (the tripwire), and writes the exact key of the
//!   tip edge in the compact edge plane.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use fold_db::crypto::unsealed_discarded_count;
use fold_db::db_operations::atom_store::{compact_tip_edge_key, TipEdgeKey};
use fold_db::hex::hex_lower;
use fold_db::storage::traits::{KvStore, NamespacedStore, PhysicalScanPage};
use serde::{Deserialize, Serialize};

use super::keys::{classify, mol_key, token_shaped_segments, Class, MolKey};
use super::rules::{key_hash, RuleSet};
use super::tripwire::{Tripwire, TripwireStats};
use super::walk::{engine_id, pages_agree, walk_both};
use super::ReapError;

/// Keys and bytes of one key class.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClassStat {
    pub keys: u64,
    pub bytes: u64,
    pub dead_keys: u64,
    pub dead_bytes: u64,
}

/// Keys and bytes under one molecule token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenStat {
    pub keys: u64,
    pub bytes: u64,
    /// One spelling seen in a key.
    pub spelling: String,
    /// True when the token is a 43 or 64 character molecule digest.
    pub digest_shaped: bool,
}

/// What the pass counted.
#[derive(Debug, Default)]
pub(crate) struct TipsReport {
    pub raw_keys: u64,
    pub decoded_keys: u64,
    pub groups: u64,
    pub largest_group_keys: u64,
    pub unsealed_discarded: u64,
    pub keys_only_count: Option<u64>,
    pub matched_keys: u64,
    pub matched_bytes: u64,
    pub by_class: BTreeMap<&'static str, ClassStat>,
    pub other_heads: BTreeMap<String, u64>,
    pub tokens: HashMap<MolKey, TokenStat>,
    pub doomed_mk: u64,
    pub edge_keys: u64,
    pub tips_without_v2_edge: u64,
    pub tv_chain_heads: u64,
    pub tombstoned_doomed: u64,
    pub scoped_dead_hits: u64,
    pub unhandled_needle_hits: u64,
    pub needle_samples: Vec<String>,
    pub protein_rows_in_tips: u64,
    pub edge_hashes: Vec<u128>,
    pub tripwire: TripwireStats,
    pub sources: Option<super::sources::SourceBuilder>,
}

/// The bound on distinct head labels kept for unknown classes.
const MAX_OTHER_HEADS: usize = 200;

/// The pass state.
pub(crate) struct TipsState<'a> {
    rules: &'a RuleSet,
    dead: &'a BTreeMap<MolKey, std::collections::BTreeSet<String>>,
    tripwire: Tripwire<'a>,
    spill: Option<BufWriter<File>>,
    group_keys: HashMap<(u16, u32), u64>,
    pub report: TipsReport,
    sources: Option<super::sources::SourceBuilder>,
}

fn lossy(key: &[u8]) -> String {
    let text = String::from_utf8_lossy(key).replace('\0', "\\0");
    text.chars().take(120).collect()
}

impl<'a> TipsState<'a> {
    pub(crate) fn new(
        rules: &'a RuleSet,
        dead: &'a BTreeMap<MolKey, std::collections::BTreeSet<String>>,
        tripwire: Tripwire<'a>,
        spill_path: Option<&Path>,
    ) -> Result<Self, ReapError> {
        let spill = match spill_path {
            Some(path) => {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                Some(BufWriter::new(File::create(path)?))
            }
            None => None,
        };
        Ok(Self {
            rules,
            dead,
            tripwire,
            spill,
            group_keys: HashMap::new(),
            report: TipsReport::default(),
            sources: None,
        })
    }

    pub(crate) fn with_sources(mut self, sources: super::sources::SourceBuilder) -> Self {
        self.sources = Some(sources);
        self
    }

    /// Fold one pair of pages (raw and decrypted) into the counts.
    pub(crate) fn absorb_page(
        &mut self,
        raw: &PhysicalScanPage,
        decoded: &PhysicalScanPage,
    ) -> Result<(), ReapError> {
        let group = raw
            .row_handle
            .as_ref()
            .map(|h| (h.shard, h.group_id.unwrap_or(u32::MAX)));
        pages_agree(raw, decoded).map_err(|message| {
            ReapError::abort("TIPS_DECODE_COUNT", format!("group {group:?}: {message}"))
        })?;
        self.report.raw_keys += raw.rows.len() as u64;
        self.report.decoded_keys += decoded.rows.len() as u64;
        if let (Some(group), false) = (group, raw.rows.is_empty()) {
            *self.group_keys.entry(group).or_default() += raw.rows.len() as u64;
        }
        for ((key, stored), (_, plain)) in raw.rows.iter().zip(&decoded.rows) {
            self.absorb_row(key, stored.len(), plain)?;
        }
        Ok(())
    }

    fn absorb_row(&mut self, key: &[u8], stored_len: usize, plain: &[u8]) -> Result<(), ReapError> {
        let id = engine_id(key);
        let text = std::str::from_utf8(&id).unwrap_or("");
        let bytes = (id.len() + stored_len) as u64;
        let classified = classify(text);
        let token_key = classified.token.as_deref().map(mol_key);
        let doomed = self.rules.matches(&id);
        let by_class = token_key.is_some_and(|k| self.dead.contains_key(&k));
        if doomed != by_class {
            return Err(ReapError::abort(
                "RULES_DISAGREE",
                format!(
                    "the rules say doomed={doomed} and the class reader says {by_class} for key {}",
                    lossy(&id)
                ),
            ));
        }
        self.tally(classified.class, text, bytes, doomed);
        if let (Some(k), Some(token)) = (token_key, classified.token.as_deref()) {
            let stat = self.report.tokens.entry(k).or_insert_with(|| TokenStat {
                keys: 0,
                bytes: 0,
                spelling: token.to_string(),
                digest_shaped: fold_db::atom::molecule_uuid::parse_molecule_uuid_bytes(token)
                    .is_some(),
            });
            stat.keys += 1;
            stat.bytes += bytes;
        }
        if doomed {
            self.report.matched_keys += 1;
            self.report.matched_bytes += bytes;
            if classified.class == Class::Mk {
                self.doomed_tip(text, plain)?;
            }
            if let Some(sources) = &mut self.sources {
                sources.observe(text, plain, classified.class, &self.tripwire)?;
            }
        } else if classified.token.is_none() {
            self.needle(classified.class, text);
        }
        Ok(())
    }

    fn tally(&mut self, class: Class, text: &str, bytes: u64, doomed: bool) {
        let stat = self.report.by_class.entry(class.name()).or_default();
        stat.keys += 1;
        stat.bytes += bytes;
        if doomed {
            stat.dead_keys += 1;
            stat.dead_bytes += bytes;
        }
        if class == Class::Other {
            if text.starts_with("protein:") || text.starts_with("molprot:") {
                self.report.protein_rows_in_tips += 1;
            }
            let head: String = text
                .split([':', '\0'])
                .next()
                .unwrap_or("")
                .chars()
                .take(24)
                .collect();
            let heads = &mut self.report.other_heads;
            if heads.len() < MAX_OTHER_HEADS || heads.contains_key(&head) {
                *heads.entry(head).or_default() += 1;
            }
        }
    }

    /// A key with no molecule token may still carry a dead token in its text.
    fn needle(&mut self, class: Class, text: &str) {
        let hit = token_shaped_segments(text).any(|seg| self.dead.contains_key(&mol_key(seg)));
        if !hit {
            return;
        }
        if class == Class::Scoped {
            self.report.scoped_dead_hits += 1;
        } else {
            self.report.unhandled_needle_hits += 1;
        }
        if self.report.needle_samples.len() < 5 {
            self.report.needle_samples.push(lossy(text.as_bytes()));
        }
    }

    fn doomed_tip(&mut self, text: &str, plain: &[u8]) -> Result<(), ReapError> {
        let tip = decode_doomed(text, plain)?;
        let molecule = mol_key(&tip.molecule_uuid);
        self.tripwire
            .check(&molecule, tip.written_at, text, &mut self.report.tripwire)?;
        self.report.doomed_mk += 1;
        self.report.tv_chain_heads += u64::from(tip.has_prev_tip);
        self.report.tombstoned_doomed += u64::from(tip.tombstoned);
        match tip.edge_key_v2 {
            Some(edge) => {
                if let Some(spill) = self.spill.as_mut() {
                    writeln!(spill, "{}", hex_lower(edge.as_bytes()))?;
                }
                self.report.edge_hashes.push(key_hash(edge.as_bytes()));
                self.report.edge_keys += 1;
            }
            None => self.report.tips_without_v2_edge += 1,
        }
        Ok(())
    }

    /// Close the spill file and return the report.
    pub(crate) fn finish(mut self) -> Result<TipsReport, ReapError> {
        if let Some(mut spill) = self.spill.take() {
            spill.flush()?;
        }
        self.report.sources = self.sources.take();
        self.report.tripwire.slack_ms = self.tripwire.slack_ms();
        self.report.groups = self.group_keys.len() as u64;
        self.report.largest_group_keys = self.group_keys.values().copied().max().unwrap_or(0);
        self.report.edge_hashes.sort_unstable();
        self.report.edge_hashes.dedup();
        Ok(self.report)
    }
}

/// Decode a doomed `mk:` row. A row that does not decode stops the plan, because
/// its tip edge key cannot be built.
fn decode_doomed(text: &str, plain: &[u8]) -> Result<TipEdgeKey, ReapError> {
    compact_tip_edge_key(text, plain).map_err(|error| {
        ReapError::abort(
            "DOOMED_TIP_UNDECODABLE",
            format!("tip {} does not decode: {error}", lossy(text.as_bytes())),
        )
    })
}

/// Walk the tips collection with both readers and absorb every page.
pub(crate) async fn run(
    raw: Arc<dyn KvStore>,
    decoded: Arc<dyn KvStore>,
    store: &Arc<dyn NamespacedStore>,
    mut state: TipsState<'_>,
) -> Result<TipsReport, ReapError> {
    let before = unsealed_discarded_count();
    walk_both(raw, decoded, "TIPS_DECODE_COUNT", |raw_page, seam_page| {
        state.absorb_page(raw_page, seam_page)
    })
    .await?;
    let mut report = state.finish()?;
    report.unsealed_discarded = unsealed_discarded_count().saturating_sub(before);
    report.keys_only_count = store
        .raw_last_store()
        .map(|last| last.collection_live_key_count("tips"))
        .transpose()
        .map_err(|error| ReapError::Failed(format!("keys-only count of tips: {error}")))?;
    check_counts(&report)?;
    Ok(report)
}

/// Gate: every reader agrees on the number of live keys.
pub(crate) fn check_counts(report: &TipsReport) -> Result<(), ReapError> {
    if report.unsealed_discarded != 0 {
        return Err(ReapError::abort(
            "UNSEALED_DISCARD",
            format!(
                "the seam discarded {} un-enveloped row(s) during the pass",
                report.unsealed_discarded
            ),
        ));
    }
    if report.decoded_keys != report.raw_keys {
        return Err(ReapError::abort(
            "TIPS_DECODE_COUNT",
            format!(
                "decoded {} key(s), raw {}",
                report.decoded_keys, report.raw_keys
            ),
        ));
    }
    if let Some(count) = report.keys_only_count {
        if count != report.decoded_keys {
            return Err(ReapError::abort(
                "TIPS_KEY_COUNT",
                format!(
                    "the keys-only count is {count}, the pass decoded {}",
                    report.decoded_keys
                ),
            ));
        }
    }
    Ok(())
}
