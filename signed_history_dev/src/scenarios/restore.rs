//! D. Bootstrap on A, upload all, fresh history on B replays from seq 1.

use mutation_log::{
    bootstrap_from_records, AcceptOutcome, BootstrapParams, BootstrapRecord, VerifiedHistory,
};

use crate::device::{now_ms, DbBirth, Device};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner = Device::generate();
    let birth = DbBirth::generate();
    let t0 = now_ms();
    let records = [
        BootstrapRecord {
            namespace: "demo".into(),
            record_key: "r1".into(),
            value: b"one".to_vec(),
        },
        BootstrapRecord {
            namespace: "demo".into(),
            record_key: "r2".into(),
            value: b"two".to_vec(),
        },
    ];
    let boot = bootstrap_from_records(BootstrapParams {
        database_id: birth.database_id,
        signing_key: &owner.signing_key,
        owner: owner.node_key,
        device_id: owner.device_id,
        db_salt: birth.db_salt,
        birth_signature: birth.birth_signature.clone(),
        authored_at_ms: t0,
        records: &records,
        grants: &[],
    })
    .map_err(|e| e.to_string())?;

    let client = RelayClient::new(&relay.base_url, &owner);
    let db = birth.database_id.to_hex();
    client.append(&db, &boot.entries).await?;

    // Fresh device B (any reader identity for nodeSignature) restores from seq 1.
    let reader = Device::generate();
    let pull = RelayClient::new(&relay.base_url, &reader);
    let remote = pull
        .read(&db, &owner.node_key_hex(), &owner.device_id_str(), 1, 90)
        .await?;
    if remote.len() != boot.entries.len() {
        return Err(format!(
            "relay returned {} entries, expected {}",
            remote.len(),
            boot.entries.len()
        ));
    }

    let mut restored = VerifiedHistory::new(birth.database_id);
    for entry in remote {
        match restored.accept_entry(entry) {
            AcceptOutcome::Accept { .. } => {}
            AcceptOutcome::Pending { reason } => {
                return Err(format!("restore pending: {reason:?}"))
            }
            AcceptOutcome::Reject { reason } => return Err(format!("restore reject: {reason:?}")),
        }
    }

    if restored.records() != boot.history.records() {
        return Err(format!(
            "restored records {:?} != source {:?}",
            restored.records(),
            boot.history.records()
        ));
    }
    Ok(format!(
        "replayed {} entries; records equal source",
        boot.entries.len()
    ))
}
