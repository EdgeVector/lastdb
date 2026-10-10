//! Historical sources and explicit contracts for non-source metadata.

use super::*;
use fold_db::atom::MoleculeRef;
use fold_db::db_operations::atom_store::reap_keys::{
    offline_delete_history_atom, offline_molecule_shape,
};
use fold_db::db_operations::AtomDeleteLedgerEntry;

pub(super) fn lineage(
    collection: &str,
    key: &[u8],
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<bool, String> {
    let sources = match collection {
        "lineage_forward" => {
            if std::str::from_utf8(key).map_err(err)?.is_empty() {
                return Err("empty lineage molecule".into());
            }
            serde_json::from_slice::<Vec<MoleculeRef>>(plain).map_err(err)?
        }
        "lineage_reverse" => {
            let derived: Vec<String> = serde_json::from_slice(plain).map_err(err)?;
            if derived.iter().any(|id| id.is_empty()) {
                return Err("empty derived lineage molecule".into());
            }
            vec![canonical_source(key)?]
        }
        _ => return Ok(false),
    };
    for source in sources {
        if source.atom_uuid.is_empty() || source.molecule_uuid.is_empty() {
            return Err("empty lineage source identity".into());
        }
        facts.hold(
            targets,
            &source.atom_uuid,
            model::hold(
                collection,
                key,
                "",
                "historical_lineage",
                Some(&source.molecule_uuid),
            ),
        );
    }
    facts.count("lineage_rows");
    Ok(true)
}

fn field(bytes: &mut &[u8]) -> Result<String, String> {
    let len = bytes.get(..4).ok_or("truncated lineage field length")?;
    let len = u32::from_be_bytes(len.try_into().map_err(err)?) as usize;
    *bytes = &bytes[4..];
    let value = bytes.get(..len).ok_or("truncated lineage field")?;
    let value = std::str::from_utf8(value).map_err(err)?.to_string();
    *bytes = &bytes[len..];
    Ok(value)
}

fn canonical_source(key: &[u8]) -> Result<MoleculeRef, String> {
    let mut bytes = key;
    let molecule_uuid = field(&mut bytes)?;
    let atom_uuid = field(&mut bytes)?;
    let key_field = field(&mut bytes)?;
    let written_at = u64::from_be_bytes(bytes.try_into().map_err(err)?);
    let source = MoleculeRef {
        molecule_uuid,
        atom_uuid,
        key: (!key_field.is_empty()).then_some(key_field),
        written_at,
    };
    if source.canonical_bytes() != key {
        return Err("noncanonical lineage source key".into());
    }
    Ok(source)
}

pub(super) fn metadata(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    if bare.starts_with("dellog:") || bare.starts_with("dellog\0") {
        return ledger(bare, plain, facts);
    }
    if bare.starts_with("rdel:") {
        if let Some(uuid) =
            offline_delete_history_atom(std::str::from_utf8(key).map_err(err)?, plain)
                .map_err(err)?
        {
            facts.hold(
                targets,
                &uuid,
                model::hold(collection, key, scope, "displaced_delete_history", None),
            );
        }
        facts.count("delete_barrier_rows");
        return Ok(());
    }
    if bare.starts_with("mh:") || bare.starts_with("mgp:v1:") {
        return offline_molecule_shape(bare, plain).map_err(err);
    }
    if collection == "metadata" {
        return node_metadata(key, bare, plain, targets, facts);
    }
    if collection == "change_feed" {
        return change_feed(bare, plain);
    }
    if collection == "idempotency" {
        let id: String = serde_json::from_slice(plain).map_err(err)?;
        return if id.is_empty() {
            Err("empty idempotency mutation identity".into())
        } else {
            Ok(())
        };
    }
    if collection == "schema_index" {
        let marker: bool = serde_json::from_slice(plain).map_err(err)?;
        return if marker {
            Ok(())
        } else {
            Err("invalid schema atom membership marker".into())
        };
    }
    if matches!(
        collection,
        "sync_capture_reexport" | "sync_capture" | "sync_outbox" | "sync_upload_quarantine"
    ) {
        return Err("durable pending capture/outbox prevents target atom proof".into());
    }
    if shape_collection(collection) {
        return Ok(());
    }
    if bare.starts_with("mord:")
        || bare.starts_with("mord\0")
        || bare.starts_with("moc:")
        || bare.starts_with("moc\0")
    {
        // Order records contain record lookup keys; every physical mk source is independent.
        return Ok(());
    }
    if bare.starts_with("mcc:") {
        let _: Vec<String> = serde_json::from_slice(plain).map_err(err)?;
        return Ok(());
    }
    if migration_or_derived(bare) {
        let _: serde_json::Value = sources::json(plain)?;
        return Ok(());
    }
    if bare.starts_with("gcatoms-probe-ref\0")
        || bare.starts_with("gcatoms-probe-tv-skip:")
        || bare.starts_with("gcatoms-meta:")
    {
        // These are probe membership/cursor metadata, never mutation replay.
        // Independent complete source/reference reads, not this cache, decide liveness.
        let _: serde_json::Value = sources::json(plain)?;
        return Ok(());
    }
    Err("unsupported durable source class prevents atom proof".into())
}

fn ledger(bare: &str, plain: &[u8], facts: &mut Facts) -> Result<(), String> {
    let tail = bare
        .strip_prefix("dellog:")
        .or_else(|| bare.strip_prefix("dellog\0"))
        .ok_or("invalid ledger prefix")?;
    let (stamp, id) = tail.split_once(':').ok_or("invalid ledger identity")?;
    if stamp.len() != 20
        || !stamp.bytes().all(|b| b.is_ascii_digit())
        || uuid::Uuid::parse_str(id).is_err()
    {
        return Err("invalid delete ledger key".into());
    }
    let entry: AtomDeleteLedgerEntry = serde_json::from_slice(plain).map_err(err)?;
    if entry.version != 1 {
        return Err("unsupported delete ledger version".into());
    }
    chrono::DateTime::parse_from_rfc3339(&entry.at).map_err(err)?;
    facts.count(if entry.committed {
        "committed_delete_ledger_rows"
    } else {
        "unconfirmed_delete_ledger_rows"
    });
    // Production intentionally records counts and key fingerprints, never atom ids or content.
    Ok(())
}

fn node_metadata(
    key: &[u8],
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    if bare == "node_id" {
        let id: String = serde_json::from_slice(plain).map_err(err)?;
        if id.is_empty() {
            return Err("empty node identity".into());
        }
    } else if let Some(writer_hash) = bare.strip_prefix("mutation_author_clock:") {
        if writer_hash.len() != 64
            || !writer_hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid mutation author clock metadata key".into());
        }
        let value = sources::json(plain)?;
        if !value.as_object().is_some_and(|fields| {
            fields
                .keys()
                .all(|field| matches!(field.as_str(), "physical_nanos" | "logical_counter"))
        }) {
            return Err("unsupported mutation author clock metadata fields".into());
        }
        let _: fold_db::schema::types::MutationAuthorClockState =
            serde_json::from_slice(plain).map_err(err)?;
    } else {
        // The generic metadata API also stores aggregate recovery/quarantine records.
        // Their replay authority is not reduced to a sample of visible UUID strings.
        let _: serde_json::Value = sources::json(plain)?;
        facts.count("unresolved_metadata_records");
        facts.hold_all(
            targets,
            model::hold("metadata", key, "", "unresolved_metadata_recovery", None),
        );
    }
    Ok(())
}

fn change_feed(bare: &str, plain: &[u8]) -> Result<(), String> {
    if bare == "tip" {
        let _: u64 = serde_json::from_slice(plain).map_err(err)?;
    } else {
        let event: fold_db::db_operations::change_feed::ChangeFeedEvent =
            serde_json::from_slice(plain).map_err(err)?;
        if bare != format!("event:{:020}", event.seq) {
            return Err("change feed key differs from its sequence".into());
        }
    }
    // Consumers fetch the current product record; this event carries no body/atom id.
    Ok(())
}

fn shape_collection(collection: &str) -> bool {
    matches!(
        collection,
        "__at_rest_strict_markers"
            | "atom_locators"
            | "blob_ref_edges"
            | "molecule_ref_edges"
            | "keep_small"
            | "schema_states"
            | "schema_superseded_by"
            | "public_keys"
            | "schemas"
            | "molecule_keys"
            | "node_id_schema_permissions"
            | "app_identity:consent_requests"
    )
}

fn migration_or_derived(bare: &str) -> bool {
    [
        "tvr-meta:",
        "amigr:atom_key_encoding_v1",
        "amigr:atom_partition_prefix_v1",
        "amigr:reseal_at_rest_v1:",
        "amigr:reseal_at_rest_v2:",
        "amigr:reap_unsealed_v1:",
        "aloc:",
        "aloc\0",
        "bref:",
        "mref:",
        "cas_blob:",
    ]
    .iter()
    .any(|prefix| bare.starts_with(prefix))
}
