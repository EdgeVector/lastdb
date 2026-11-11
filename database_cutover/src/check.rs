//! Semantic checks. One refusal lists every failed check so the operator can correct the bundle.

use std::collections::BTreeSet;

use crate::error::Reason;
use crate::wire::{
    within_member_cap, within_tip_cap, Bundle, DaemonReady, DeliveryReady, LegacyPosition,
    NodeKeyGrants, SnapshotReceipt, WriterTip, DAEMON_READY_FILE, DELIVERY_READY_FILE,
    FINAL_WRITES_FILE, GRANTS_FILE, KIND_PERSONAL, KIND_SHAREABLE, LEGACY_POSITION_FILE, SCHEMA,
    SNAPSHOT_FILE,
};

const MAX_REASONS: usize = 48;
const SNAPSHOT_KIND: &str = "fresh_full_snapshot";
const LEGACY_KIND: &str = "legacy_position";
const WRITES_KIND: &str = "final_writes";
const GRANTS_KIND: &str = "node_key_grants";
const DAEMON_KIND: &str = "daemon_sync_ready";
const DELIVERY_KIND: &str = "delivery_signed_only_ready";

struct Scope<'a> {
    file: &'static str,
    kind: &'a str,
    required_kind: &'static str,
    schema: &'a str,
    environment: &'a str,
    database_id: &'a str,
    database_kind: &'a str,
}

pub(crate) fn problems(
    expected_database_id: &str,
    expected_environment: &str,
    bundle: &Bundle,
) -> Vec<Reason> {
    let mut reasons = Vec::new();
    if !label_ok(expected_database_id) {
        push(&mut reasons, Reason::InvalidDatabaseId);
    }
    let realm_named = realm_ok(expected_environment);
    if !realm_named {
        push(&mut reasons, Reason::InvalidEnvironment);
    }
    let reference_kind = bundle.snapshot.database_kind.as_str();
    check_scope(
        &scope(SNAPSHOT_FILE, &bundle.snapshot, SNAPSHOT_KIND),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    check_scope(
        &legacy_scope(&bundle.legacy),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    check_scope(
        &writes_scope(bundle),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    check_scope(
        &grants_scope(&bundle.grants),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    check_scope(
        &daemon_scope(&bundle.daemon),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    check_scope(
        &delivery_scope(&bundle.delivery),
        expected_database_id,
        expected_environment,
        realm_named,
        reference_kind,
        &mut reasons,
    );
    if !snapshot_ok(&bundle.snapshot) {
        push(&mut reasons, Reason::Snapshot);
    }
    if !position_ok(bundle) {
        push(&mut reasons, Reason::Position);
    }
    if !tips_ok(&bundle.legacy.writer_tips, &bundle.legacy.position) {
        push(&mut reasons, Reason::WriterTips);
    }
    if known_kind(&bundle.grants.database_kind) && !grants_ok(&bundle.grants) {
        push(&mut reasons, Reason::Grants);
    }
    if !daemon_ok(&bundle.daemon) {
        push(&mut reasons, Reason::Daemon);
    }
    if !delivery_ok(&bundle.delivery, bundle.grants.revision) {
        push(&mut reasons, Reason::Delivery);
    }
    reasons
}

fn scope(file: &'static str, snapshot: &SnapshotReceipt, required_kind: &'static str) -> Scope<'_> {
    Scope {
        file,
        kind: &snapshot.kind,
        required_kind,
        schema: &snapshot.schema,
        environment: &snapshot.environment,
        database_id: &snapshot.database_id,
        database_kind: &snapshot.database_kind,
    }
}

fn legacy_scope(legacy: &LegacyPosition) -> Scope<'_> {
    Scope {
        file: LEGACY_POSITION_FILE,
        kind: &legacy.kind,
        required_kind: LEGACY_KIND,
        schema: &legacy.schema,
        environment: &legacy.environment,
        database_id: &legacy.database_id,
        database_kind: &legacy.database_kind,
    }
}

fn writes_scope(bundle: &Bundle) -> Scope<'_> {
    Scope {
        file: FINAL_WRITES_FILE,
        kind: &bundle.writes.kind,
        required_kind: WRITES_KIND,
        schema: &bundle.writes.schema,
        environment: &bundle.writes.environment,
        database_id: &bundle.writes.database_id,
        database_kind: &bundle.writes.database_kind,
    }
}

fn grants_scope(grants: &NodeKeyGrants) -> Scope<'_> {
    Scope {
        file: GRANTS_FILE,
        kind: &grants.kind,
        required_kind: GRANTS_KIND,
        schema: &grants.schema,
        environment: &grants.environment,
        database_id: &grants.database_id,
        database_kind: &grants.database_kind,
    }
}

fn daemon_scope(daemon: &DaemonReady) -> Scope<'_> {
    Scope {
        file: DAEMON_READY_FILE,
        kind: &daemon.kind,
        required_kind: DAEMON_KIND,
        schema: &daemon.schema,
        environment: &daemon.environment,
        database_id: &daemon.database_id,
        database_kind: &daemon.database_kind,
    }
}

fn delivery_scope(delivery: &DeliveryReady) -> Scope<'_> {
    Scope {
        file: DELIVERY_READY_FILE,
        kind: &delivery.kind,
        required_kind: DELIVERY_KIND,
        schema: &delivery.schema,
        environment: &delivery.environment,
        database_id: &delivery.database_id,
        database_kind: &delivery.database_kind,
    }
}

fn check_scope(
    scope: &Scope<'_>,
    expected_db: &str,
    expected_environment: &str,
    realm_named: bool,
    reference_kind: &str,
    reasons: &mut Vec<Reason>,
) {
    if realm_named && scope.environment != expected_environment {
        push(reasons, Reason::Environment { file: scope.file });
    }
    if scope.schema != SCHEMA {
        push(reasons, Reason::Schema { file: scope.file });
    }
    if scope.kind != scope.required_kind {
        push(reasons, Reason::Kind { file: scope.file });
    }
    if scope.database_id != expected_db {
        push(reasons, Reason::Database { file: scope.file });
    }
    if !known_kind(scope.database_kind) || scope.database_kind != reference_kind {
        push(reasons, Reason::DatabaseKind { file: scope.file });
    }
}

fn snapshot_ok(snapshot: &SnapshotReceipt) -> bool {
    snapshot.full
        && snapshot.fresh
        && snapshot.covers_final_writes
        && snapshot_ref_ok(&snapshot.snapshot_ref)
        && byte_count_ok(snapshot.byte_count, &snapshot.through_legacy_position)
}

fn position_ok(bundle: &Bundle) -> bool {
    bundle.legacy.quiescent
        && bundle.writes.legacy_appends_after_fence == 0
        && bundle.writes.snapshot_covers_fence
        && shared_position(&[
            bundle.snapshot.through_legacy_position.as_str(),
            bundle.legacy.position.as_str(),
            bundle.writes.fence_position.as_str(),
            bundle.writes.position_after_drain.as_str(),
        ])
}

fn tips_ok(tips: &[WriterTip], position: &str) -> bool {
    if !within_tip_cap(tips.len()) {
        return false;
    }
    let Some(position) = canonical_position(position) else {
        return false;
    };
    if position == 0 {
        return tips.is_empty();
    }
    if tips.is_empty() {
        return false;
    }
    let mut seen = BTreeSet::new();
    let mut wrote = false;
    for tip in tips {
        if !label_ok(&tip.node_key) || !label_ok(&tip.device_id) {
            return false;
        }
        if !seen.insert((tip.node_key.as_str(), tip.device_id.as_str())) {
            return false;
        }
        if tip.sequence >= 1 {
            wrote = true;
        }
    }
    wrote
}

fn grants_ok(grants: &NodeKeyGrants) -> bool {
    if grants.revision == 0
        || !label_ok(&grants.authority_node_key)
        || !within_member_cap(grants.members.len())
    {
        return false;
    }
    let mut seen = BTreeSet::new();
    for member in &grants.members {
        if !label_ok(&member.node_key) || !seen.insert(member.node_key.as_str()) {
            return false;
        }
        if !matches!(member.capability.as_str(), "owner" | "receive" | "submit")
            || !matches!(member.state.as_str(), "active" | "revoked")
        {
            return false;
        }
    }
    match grants.database_kind.as_str() {
        KIND_PERSONAL => personal_grants(grants),
        KIND_SHAREABLE => shareable_grants(grants),
        _ => false,
    }
}

fn personal_grants(grants: &NodeKeyGrants) -> bool {
    let mut active = 0;
    for member in &grants.members {
        if member.capability != "owner" {
            return false;
        }
        if member.state == "active" {
            active += 1;
            if member.node_key != grants.authority_node_key {
                return false;
            }
        }
    }
    active == 1
}

fn shareable_grants(grants: &NodeKeyGrants) -> bool {
    for member in &grants.members {
        if member.node_key == grants.authority_node_key {
            return false;
        }
        if !matches!(
            (member.state.as_str(), member.capability.as_str()),
            ("active" | "revoked", "receive" | "submit")
        ) {
            return false;
        }
    }
    true
}

fn daemon_ok(daemon: &DaemonReady) -> bool {
    daemon.accepts_cutover_record
        && daemon.legacy_resume == "refused_after_signed_only"
        && daemon.final_write_fence == "honored"
        && label_ok(&daemon.adapter_revision)
        && label_ok(&daemon.migrating_device_id)
}

fn delivery_ok(delivery: &DeliveryReady, revision: u64) -> bool {
    delivery.legacy_bearer_closed
        && delivery.outstanding_urls == "drained_or_invalid"
        && delivery.grant_check == "current_node_key"
        && revision >= 1
        && delivery.grant_revision == revision
        && label_ok(&delivery.service_revision)
}

fn realm_ok(value: &str) -> bool {
    value == "dev" || value == "prod"
}

pub(crate) fn label_ok(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub(crate) fn canonical_position(value: &str) -> Option<u64> {
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

pub(crate) fn snapshot_ref_ok(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .chars()
            .all(|ch| !ch.is_control() && !ch.is_whitespace())
}

pub(crate) fn known_kind(value: &str) -> bool {
    value == KIND_PERSONAL || value == KIND_SHAREABLE
}

pub(crate) fn shared_position(values: &[&str]) -> bool {
    let Some(first) = values.first().copied() else {
        return false;
    };
    canonical_position(first).is_some() && values.iter().all(|value| *value == first)
}

fn byte_count_ok(byte_count: u64, position: &str) -> bool {
    match canonical_position(position) {
        Some(0) => byte_count == 0,
        Some(_) => byte_count > 0,
        None => false,
    }
}

fn push(reasons: &mut Vec<Reason>, reason: Reason) {
    if reasons.len() < MAX_REASONS {
        reasons.push(reason);
    }
}
