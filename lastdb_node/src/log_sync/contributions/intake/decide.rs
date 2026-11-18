//! Pure publish-or-discard decision for one decrypted share submission.

use cloud_backup_protocol::v2::share_access::ShareWriteScope;
use cloud_backup_protocol::Digest;
use fold_db::schema::types::{cas::CasExpectation, Mutation, MutationType};
use fold_db::STANDARD_COMMENT_SCHEMA;

use crate::host::share_identity::LocalIdentity;

pub(super) struct GrantView<'a> {
    pub(super) active_submitter: bool,
    pub(super) scope: Option<&'a ShareWriteScope>,
}

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

pub(super) fn decide(grant: &GrantView<'_>, submission: &SubmissionView<'_>) -> IntakeDecision {
    if !grant.active_submitter {
        return discard("member is not an active submitter");
    }
    let Some(scope) = grant.scope else {
        return discard("member has no write scope");
    };
    let mutation = submission.mutation;
    if !scope.permits(submission.schema_name, submission.definition) {
        return discard("write scope does not allow this schema");
    }
    if mutation.definition_id.as_deref() != Some(submission.definition.as_str()) {
        return discard("schema definition does not match");
    }
    let standard = submission.schema_name == STANDARD_COMMENT_SCHEMA;
    let required_creator = if standard {
        match mutation.mutation_type {
            MutationType::Create => None,
            MutationType::Replace => Some(submission.writer),
            _ => return discard("comment change is not allowed"),
        }
    } else {
        None
    };
    let Some(base) = submission.base else {
        return if required_creator.is_some() {
            discard("comment edit has no base version")
        } else {
            IntakeDecision::Publish { revision: None }
        };
    };
    if !matches!(mutation.expected, CasExpectation::Revision { .. }) {
        return discard("conditional write has no revision");
    }
    let Some(local) = submission.local.filter(|row| row.mutation_id == base) else {
        return discard("record version is not the submitted base");
    };
    if required_creator.is_some_and(|creator| local.created_by.as_deref() != Some(creator)) {
        return discard("comment creator does not match the editor");
    }
    IntakeDecision::Publish {
        revision: Some(local.local_revision.clone()),
    }
}

fn discard(reason: &'static str) -> IntakeDecision {
    IntakeDecision::Discard(reason)
}
