//! Storage key layout for protein records, member back-refs, and fold jobs.

/// Durable protein record: `protein:{uuid}` → [`super::Protein`].
pub const PROTEIN_RECORD_PREFIX: &str = "protein:";

#[must_use]
pub fn protein_record_key(protein_uuid: &str) -> String {
    format!("{PROTEIN_RECORD_PREFIX}{protein_uuid}")
}

/// Bi-directional member back-ref: `molprot:{molecule_uuid}` → protein uuid string.
pub const MEMBER_BACKREF_PREFIX: &str = "molprot:";

#[must_use]
pub fn member_backref_key(molecule_uuid: &str) -> String {
    format!("{MEMBER_BACKREF_PREFIX}{molecule_uuid}")
}

/// Enqueued fold job: `pfq:{job_id}` → [`super::ProteinFoldJob`].
#[must_use]
pub fn protein_fold_job_key(job_id: &str) -> String {
    format!("pfq:{job_id}")
}

/// Prefix scan for pending fold jobs.
pub const PROTEIN_FOLD_JOB_PREFIX: &str = "pfq:";
