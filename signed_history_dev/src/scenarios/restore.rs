//! D. Bootstrap on A, upload all, restore_from_relay on B, FoldDB holds winners.

use std::path::Path;

use mutation_log::{bootstrap_from_records, BootstrapParams, BootstrapRecord};

use signed_history_client::{restore_from_relay, RestoreRequest, StreamRef};
use signed_history_materialize::encode_string_field;

use crate::device::{now_ms, DbBirth, Device};
use crate::proof_store::{replay_and_check, Expected, NAMESPACE};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle, store_dir: &Path) -> Result<String, String> {
    let owner = Device::generate();
    let birth = DbBirth::generate(owner.node_key);
    let t0 = now_ms();
    let records = [
        BootstrapRecord {
            namespace: NAMESPACE.into(),
            record_key: "r1".into(),
            value: encode_string_field("one"),
        },
        BootstrapRecord {
            namespace: NAMESPACE.into(),
            record_key: "r2".into(),
            value: encode_string_field("two"),
        },
    ];
    let boot = bootstrap_from_records(&BootstrapParams {
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

    // Same restore path as CLI: page from seq 1 through AcceptEntry.
    let reader = Device::generate();
    let report = restore_from_relay(RestoreRequest {
        relay_url: relay.base_url.clone(),
        signing_key: reader.signing_key.clone(),
        node_key_hex: reader.node_key_hex(),
        device_id: reader.device_id_str(),
        database_id: db,
        streams: vec![StreamRef {
            writer_key: owner.node_key_hex(),
            device_id: owner.device_id_str(),
        }],
    })
    .await?;

    if report.history.records() != boot.history.records() {
        return Err(format!(
            "restored records {:?} != source {:?}",
            report.history.records(),
            boot.history.records()
        ));
    }
    replay_and_check(
        store_dir,
        &report.history,
        &[
            Expected {
                record_key: "r1",
                value: "one",
            },
            Expected {
                record_key: "r2",
                value: "two",
            },
        ],
    )
    .await?;
    Ok(format!(
        "restore_from_relay replayed {}; records equal source; FoldDB holds r1=one r2=two",
        report.replayed
    ))
}
