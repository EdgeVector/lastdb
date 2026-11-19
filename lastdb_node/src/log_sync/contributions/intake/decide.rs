//! Pure publish-or-discard decision for one decrypted share submission.
//!
//! The member-write map is the database setting. A schema that is not in the
//! map is owner_only. The publisher's own writes do not come through here.

use std::collections::BTreeMap;

use cloud_backup_protocol::v2::share_access::{member_write_behaviour, MemberWriteBehaviour};
use cloud_backup_protocol::Digest;
use fold_db::schema::types::{cas::CasExpectation, Mutation, MutationType};

use crate::host::share_identity::LocalIdentity;

pub(super) struct SubmissionView<'a> {
    pub(super) schema_name: &'a str,
    pub(super) mutation: &'a Mutation,
    pub(super) definition: &'a Digest,
    pub(super) base: Option<&'a str>,
    pub(super) writer: &'a str,
    pub(super) local: Option<&'a LocalIdentity>,
}

pub(super) enum IntakeDecision {
    Publish { revision: Option<String> },
    Discard(&'static str),
}

pub(super) fn decide(
    active_submitter: bool,
    member_writes: &BTreeMap<String, MemberWriteBehaviour>,
    submission: &SubmissionView<'_>,
) -> IntakeDecision {
    if !active_submitter {
        return discard("member is not an active submitter");
    }
    let mutation = submission.mutation;
    if mutation.definition_id.as_deref() != Some(submission.definition.as_str()) {
        return discard("schema definition does not match");
    }
    let behaviour = member_write_behaviour(member_writes, submission.schema_name);
    let creator = match behaviour {
        MemberWriteBehaviour::OwnerOnly => return discard("member cannot write this schema"),
        MemberWriteBehaviour::Shared => None,
        MemberWriteBehaviour::OwnRecords => match mutation.mutation_type {
            MutationType::Create => None,
            MutationType::Replace | MutationType::Delete => Some(submission.writer),
            MutationType::Purge => return discard("member cannot write this schema"),
        },
    };
    publish_at_base(submission, creator)
}

/// A present base must be the live record. `own_records` replace and delete
/// also require `created_by` at that revision to be the writer. No `created_by`
/// means the record is the publisher's alone.
fn publish_at_base(submission: &SubmissionView<'_>, creator: Option<&str>) -> IntakeDecision {
    let Some(base) = submission.base else {
        return if creator.is_some() {
            discard("record change has no base version")
        } else {
            IntakeDecision::Publish { revision: None }
        };
    };
    if !matches!(
        submission.mutation.expected,
        CasExpectation::Revision { .. }
    ) {
        return discard("conditional write has no revision");
    }
    let Some(local) = submission.local.filter(|row| row.mutation_id == base) else {
        return discard("record version is not the submitted base");
    };
    if creator.is_some_and(|creator| local.created_by.as_deref() != Some(creator)) {
        return discard("record creator does not match the writer");
    }
    IntakeDecision::Publish {
        revision: Some(local.local_revision.clone()),
    }
}

fn discard(reason: &'static str) -> IntakeDecision {
    IntakeDecision::Discard(reason)
}
