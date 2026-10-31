use super::*;

pub(super) async fn roster(runtime: &Arc<PackRuntime>) -> StorageResult<Roster> {
    let bytes = values(runtime, vec![roster_key()]).await?;
    // A fresh capture-disabled home has no roster. A stored roster still needs
    // its capture identity before any peer position can be trusted.
    let Some(bytes) = &bytes[0] else {
        return Ok(Roster::default());
    };
    let own_origin = enabled_origin(runtime)?;
    let roster: Roster = decode(bytes)?;
    roster.validate(&own_origin)?;
    Ok(roster)
}

pub(super) async fn peer(
    runtime: &Arc<PackRuntime>,
    roster: &Roster,
    origin: &str,
) -> StorageResult<Option<PeerPosition>> {
    verify::origin_key(origin)?;
    let bytes = values(runtime, vec![peer_key(origin)]).await?;
    let exists = roster
        .origins
        .binary_search_by(|item| item.as_str().cmp(origin))
        .is_ok();
    match (&bytes[0], exists) {
        (None, false) => Ok(None),
        (Some(bytes), true) => {
            let peer: PeerPosition = decode(bytes)?;
            peer.validate(origin)?;
            if peer.position.sequence > roster.receipt_count {
                return Err(invalid(
                    "logical peer position exceeds the global receipt count",
                ));
            }
            Ok(Some(peer))
        }
        _ => Err(invalid("logical peer roster and position disagree")),
    }
}

pub(super) async fn values(
    runtime: &Arc<PackRuntime>,
    keys: Vec<String>,
) -> StorageResult<Vec<Option<Vec<u8>>>> {
    let context = read::ReadContext::current(runtime).await?;
    let pending = transaction::PendingBatch::current(runtime);
    let runtime = Arc::clone(runtime);
    run_blocking(move || {
        let coordinates = keys
            .into_iter()
            .map(|key| (COLLECTION.into(), key))
            .collect::<Vec<_>>();
        runtime.raw_many(&context, &coordinates, pending.as_ref())
    })
    .await
}
