//! C. Two devices offline-author different records → same LWW via relay exchange.

use mutation_log::{AcceptOutcome, VerifiedHistory};

use crate::device::{bootstrap_owner, now_ms, owner_write, DataWrite, DbBirth, Device};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner_a = Device::generate();
    let owner_b = Device {
        signing_key: owner_a.signing_key.clone(),
        node_key: owner_a.node_key,
        device_id: mutation_log::DeviceId::from_uuid(uuid::Uuid::new_v4()),
    };
    let birth = DbBirth::generate(owner_a.node_key);
    let t0 = now_ms();
    let (mut hist_a, birth_entry) = bootstrap_owner(&owner_a, &birth, t0)?;
    let entry_a = owner_write(
        &mut hist_a,
        &owner_a,
        DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "from_a",
            value: b"A",
            authored_at_ms: t0 + 1,
        },
    )?;

    let mut hist_b = VerifiedHistory::new(birth.database_id);
    accept(&mut hist_b, birth_entry.clone())?;
    let entry_b = owner_write(
        &mut hist_b,
        &owner_b,
        DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "from_b",
            value: b"B",
            authored_at_ms: t0 + 1,
        },
    )?;

    let client = RelayClient::new(&relay.base_url, &owner_a);
    let db = birth.database_id.to_hex();
    client
        .append(&db, &[birth_entry.clone(), entry_a.clone()])
        .await?;
    let client_b = RelayClient::new(&relay.base_url, &owner_b);
    client_b.append(&db, &[entry_b.clone()]).await?;

    for e in client
        .read(
            &db,
            &owner_b.node_key_hex(),
            &owner_b.device_id_str(),
            1,
            90,
        )
        .await?
    {
        accept(&mut hist_a, e)?;
    }
    for e in client_b
        .read(
            &db,
            &owner_a.node_key_hex(),
            &owner_a.device_id_str(),
            1,
            90,
        )
        .await?
    {
        accept(&mut hist_b, e)?;
    }

    if hist_a.records() != hist_b.records() {
        return Err(format!(
            "LWW diverge: a={:?} b={:?}",
            hist_a.records(),
            hist_b.records()
        ));
    }
    let n = hist_a.records().len();
    if n < 2 {
        return Err(format!("expected ≥2 records after converge, got {n}"));
    }
    Ok(format!("both devices materialized {n} records identically"))
}

fn accept(history: &mut VerifiedHistory, entry: mutation_log::SignedEntry) -> Result<(), String> {
    match history.accept_entry(entry) {
        AcceptOutcome::Accept { .. } => Ok(()),
        AcceptOutcome::Pending { reason } => Err(format!("pending {reason:?}")),
        AcceptOutcome::Reject { reason } => Err(format!("reject {reason:?}")),
    }
}
