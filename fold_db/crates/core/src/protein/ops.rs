//! Protein operations on [`AtomStore`]: create, bind, write, fold, backfill.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::atom::AtomEntry;
use crate::db_operations::atom_store::{AtomStore, ChangedKey, MoleculeData, MoleculeRefEdge};
use crate::schema::SchemaError;
use crate::security::Ed25519KeyPair;

use super::keys::{member_backref_key, protein_record_key, PROTEIN_FOLD_JOB_PREFIX};
use super::types::{Protein, ProteinFoldJob, ProteinMember};

mod batch;
mod tip;
pub(crate) use tip::PreparedProteinTip;

struct ProteinTipScope<'a> {
    honor_tip_lww: bool,
    storage_prefix: Option<&'a str>,
}

impl AtomStore {
    /// Create a new empty protein (UUID identity). Membership is empty until
    /// [`Self::protein_add_member`].
    pub async fn protein_create(&self) -> Result<Protein, SchemaError> {
        let uuid = Uuid::new_v4().to_string();
        let protein = Protein::new(uuid);
        self.put_protein(&protein).await?;
        Ok(protein)
    }

    /// Load a protein by UUID.
    pub async fn protein_get(&self, protein_uuid: &str) -> Result<Option<Protein>, SchemaError> {
        self.raw()
            .get_item(&protein_record_key(protein_uuid))
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read protein {protein_uuid}: {e}")))
    }

    /// Protein UUID for a member molecule, if bound.
    pub async fn protein_of_molecule(
        &self,
        molecule_uuid: &str,
    ) -> Result<Option<String>, SchemaError> {
        let key = member_backref_key(molecule_uuid);
        let v: Option<String> =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read molprot {molecule_uuid}: {e}"))
            })?;
        Ok(v)
    }

    /// Bind a molecule as a protein member (bi-directional).
    ///
    /// Idempotent when the same molecule is already a member with the same
    /// key layout. Errors if the molecule is already bound to a *different*
    /// protein.
    pub async fn protein_add_member(
        &self,
        protein_uuid: &str,
        member: ProteinMember,
    ) -> Result<Protein, SchemaError> {
        self.with_protein_membership(self.protein_add_member_in_scope(protein_uuid, member))
            .await
    }

    /// The caller holds `with_protein_membership` across selection and both binds.
    pub(crate) async fn protein_add_member_in_scope(
        &self,
        protein_uuid: &str,
        member: ProteinMember,
    ) -> Result<Protein, SchemaError> {
        if member.molecule_uuid.is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member molecule_uuid must be non-empty".into(),
            ));
        }
        if member.hash_field.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member hash_field must be non-empty".into(),
            ));
        }

        if let Some(existing) = self.protein_of_molecule(&member.molecule_uuid).await? {
            if existing != protein_uuid {
                return Err(SchemaError::InvalidData(format!(
                    "molecule {} already bound to protein {existing}, not {protein_uuid}",
                    member.molecule_uuid
                )));
            }
        }

        let mut protein = self
            .protein_get(protein_uuid)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("protein {protein_uuid} not found")))?;

        // Same molecule + same key layout → no-op. Same molecule + different
        // layout → additional conformation (shared atom, multi-key tips).
        if protein
            .member_for_layout(
                &member.molecule_uuid,
                &member.hash_field,
                member.range_field.as_deref(),
            )
            .is_some()
        {
            return Ok(protein);
        }

        let molecule_ref = MoleculeRefEdge::protein_member(
            protein_uuid,
            &member.molecule_uuid,
            &member.hash_field,
            member.range_field.as_deref(),
        );
        let _target_gate = self
            .lock_liveness_molecules(std::slice::from_ref(&member.molecule_uuid))
            .await;
        // Retain first. An interrupted bind can leave an extra edge, but it
        // cannot publish a protein member that the molecule reclaimer misses.
        self.put_molecule_ref_edge(&molecule_ref, None).await?;
        protein.members.push(member.clone());
        self.put_protein(&protein).await?;
        self.raw()
            .put_item(&member_backref_key(&member.molecule_uuid), &protein_uuid)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("write molprot {}: {e}", member.molecule_uuid))
            })?;
        Ok(protein)
    }

    /// Process up to `max_jobs` pending fold jobs. Returns how many were applied.
    pub async fn protein_process_folds(&self, max_jobs: usize) -> Result<usize, SchemaError> {
        if max_jobs == 0 {
            return Ok(0);
        }
        let mut jobs: Vec<(String, ProteinFoldJob)> = self
            .raw()
            .scan_items_with_prefix(PROTEIN_FOLD_JOB_PREFIX)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan protein fold jobs: {e}")))?;
        // Stable order by job key.
        jobs.sort_by(|(a, _), (b, _)| a.cmp(b));
        let mut applied = 0usize;
        let keypair = Ed25519KeyPair::generate()
            .map_err(|e| SchemaError::InvalidData(format!("fold keypair: {e}")))?;

        for (storage_key, job) in jobs.into_iter().take(max_jobs) {
            self.protein_apply_fold_job(&job, &keypair).await?;
            let pending_source = format!("protein-fold:{}", job.job_id);
            self.raw()
                .batch_delete_keys(vec![
                    storage_key.clone(),
                    Self::pending_atom_ref_key(&job.atom_uuid, &pending_source, None),
                ])
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("delete fold job {storage_key}: {e}"))
                })?;
            self.keep_small().complete_pending_protein_fold();
            applied += 1;
        }
        if applied > 0 {
            // Debounced persist only (2026-09-21 flush storm; see protein_write).
            let _ = self.persist_keep_small().await;
        }
        Ok(applied)
    }

    /// Apply one fold job: repoint every non-entry member tip to the shared atom.
    pub async fn protein_apply_fold_job(
        &self,
        job: &ProteinFoldJob,
        keypair: &Ed25519KeyPair,
    ) -> Result<(), SchemaError> {
        let protein = self.protein_get(&job.protein_uuid).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("protein {} not found for fold", job.protein_uuid))
        })?;

        let tip = AtomEntry::thin(
            job.atom_uuid.clone(),
            job.written_at,
            if job.device_id.is_empty() {
                keypair.public_key_base64()
            } else {
                job.device_id.clone()
            },
        );

        for member in &protein.members {
            if member.molecule_uuid == job.entry_molecule_uuid {
                continue;
            }
            let Some((hash, range)) = member.tip_coords_from_fields(&job.fields) else {
                continue;
            };
            self.protein_set_member_tip(&member.molecule_uuid, &hash, &range, &tip, keypair)
                .await?;
        }
        Ok(())
    }

    /// One-time tip backfill when adding a new key as a self-maintaining member.
    ///
    /// For each tip currently present on `source_molecule` under keys that can
    /// be re-derived via `source_member` / `new_member` field maps built from
    /// `records` (each record is a full field map), set the new member's tip to
    /// the same atom as the source tip.
    pub async fn protein_backfill_member_tips(
        &self,
        source_molecule_uuid: &str,
        new_member: &ProteinMember,
        records: &[HashMap<String, String>],
        keypair: &Ed25519KeyPair,
    ) -> Result<usize, SchemaError> {
        let source_protein = self
            .protein_of_molecule(source_molecule_uuid)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "source molecule {source_molecule_uuid} is not a protein member"
                ))
            })?;
        let protein = self.protein_get(&source_protein).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("protein {source_protein} not found"))
        })?;
        let source_member = protein
            .member_for_molecule(source_molecule_uuid)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "protein missing source member {source_molecule_uuid}"
                ))
            })?;

        // Ensure new member is bound.
        if !protein.contains_molecule(&new_member.molecule_uuid) {
            self.protein_add_member(&source_protein, new_member.clone())
                .await?;
        }

        let mut filled = 0usize;
        for fields in records {
            let Some((src_hash, src_range)) = source_member.tip_coords_from_fields(fields) else {
                continue;
            };
            let Some((dst_hash, dst_range)) = new_member.tip_coords_from_fields(fields) else {
                continue;
            };
            let Some(entry) = self
                .protein_get_member_tip(source_molecule_uuid, &src_hash, &src_range)
                .await?
            else {
                continue;
            };
            self.protein_set_member_tip(
                &new_member.molecule_uuid,
                &dst_hash,
                &dst_range,
                &entry,
                keypair,
            )
            .await?;
            filled += 1;
        }
        Ok(filled)
    }

    /// Read tip atom uuid at a member's key coordinates (None if absent).
    pub async fn protein_get_member_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<AtomEntry>, SchemaError> {
        let storage_hash = self.storage_hash(molecule_uuid, hash)?;
        let storage_range = self.storage_range(molecule_uuid, range)?;
        let base = crate::atom::molecule_key_codec::hash_range_record_key(
            molecule_uuid,
            &storage_hash,
            &storage_range,
        );
        match self.get_per_key(&base, None).await? {
            Some((_, rec)) => Ok(Some(rec.entry)),
            None => Ok(None),
        }
    }

    // ── internals ──────────────────────────────────────────────────────────

    async fn put_protein(&self, protein: &Protein) -> Result<(), SchemaError> {
        self.raw()
            .put_item(&protein_record_key(&protein.uuid), protein)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("write protein {}: {e}", protein.uuid)))
    }

    /// Set (or create) a tip on a molecule at (hash, range).
    async fn protein_set_member_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tip: &AtomEntry,
        keypair: &Ed25519KeyPair,
    ) -> Result<(), SchemaError> {
        let (molecule_uuid, mol, changed, _) = self
            .protein_member_tip_update(
                molecule_uuid,
                hash,
                range,
                tip,
                keypair,
                ProteinTipScope {
                    honor_tip_lww: false,
                    storage_prefix: None,
                },
            )
            .await?;
        self.store_molecule_changed_keys(&molecule_uuid, &mol, &changed, None)
            .await?;
        Ok(())
    }
}
