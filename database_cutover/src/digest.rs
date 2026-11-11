//! Canonical digest of a verified bundle. Timestamps are not inputs.

use sha2::{Digest, Sha256};

use crate::wire::{Bundle, SCHEMA};

pub(crate) fn digest(bundle: &Bundle) -> String {
    let mut body = String::new();
    line(&mut body, "schema", SCHEMA);
    line(&mut body, "database", &bundle.snapshot.database_id);
    line(&mut body, "kind", &bundle.snapshot.database_kind);
    line(&mut body, "snapshot", &bundle.snapshot.snapshot_ref);
    line(&mut body, "position", &bundle.legacy.position);
    let bytes = bundle.snapshot.byte_count.to_string();
    line(&mut body, "bytes", &bytes);
    line(&mut body, "authority", &bundle.grants.authority_node_key);
    let revision = bundle.grants.revision.to_string();
    line(&mut body, "revision", &revision);
    members(&mut body, bundle);
    tips(&mut body, bundle);
    line(&mut body, "daemon", &bundle.daemon.adapter_revision);
    line(&mut body, "migrating", &bundle.daemon.migrating_device_id);
    line(&mut body, "delivery", &bundle.delivery.service_revision);
    hex_encode(Sha256::digest(body.as_bytes()).as_slice())
}

fn members(body: &mut String, bundle: &Bundle) {
    let mut rows = bundle.grants.members.clone();
    rows.sort_by(|left, right| member_key(left).cmp(&member_key(right)));
    for member in &rows {
        body.push_str("member ");
        body.push_str(&member.node_key);
        body.push(' ');
        body.push_str(&member.capability);
        body.push(' ');
        body.push_str(&member.state);
        body.push('\n');
    }
}

fn member_key(member: &crate::wire::MemberGrant) -> (&str, &str, &str) {
    (&member.node_key, &member.capability, &member.state)
}

fn tips(body: &mut String, bundle: &Bundle) {
    let mut rows = bundle.legacy.writer_tips.clone();
    rows.sort_by(|left, right| tip_key(left).cmp(&tip_key(right)));
    for tip in &rows {
        body.push_str("tip ");
        body.push_str(&tip.node_key);
        body.push(' ');
        body.push_str(&tip.device_id);
        body.push(' ');
        body.push_str(&tip.sequence.to_string());
        body.push('\n');
    }
}

fn tip_key(tip: &crate::wire::WriterTip) -> (&str, &str, u64) {
    (&tip.node_key, &tip.device_id, tip.sequence)
}

fn line(body: &mut String, key: &str, value: &str) {
    body.push_str(key);
    body.push(' ');
    body.push_str(value);
    body.push('\n');
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}
