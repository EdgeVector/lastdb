use super::*;

impl PackRuntime {
    /// Only an owner action may pin the initial anchor supplied by another device.
    pub async fn logical_capture_enroll_peer(
        self: &Arc<Self>,
        anchor: Position,
    ) -> StorageResult<Position> {
        let own_origin = enabled_origin(self)?;
        verify::origin_key(&anchor.origin)?;
        unhex::<32>(&anchor.sha256)?;
        if anchor.sequence != 0 || anchor.origin == own_origin {
            return Err(invalid(
                "logical peer enrollment needs another device's initial anchor",
            ));
        }
        let runtime = Arc::clone(self);
        transaction::request_scope(Some(Arc::clone(self)), async move {
            let mut roster = state::roster(&runtime).await?;
            if let Some(peer) = state::peer(&runtime, &roster, &anchor.origin).await? {
                if peer.anchor != anchor {
                    return Err(invalid("logical peer enrollment anchor differs"));
                }
                return Ok(peer.position);
            }
            if roster.origins.len() >= MAX_PEERS {
                return Err(StorageError::WriteResourceLimit {
                    detail: "logical peer enrollment reached its 32 origin cap".into(),
                });
            }
            roster.origins.push(anchor.origin.clone());
            roster.origins.sort();
            let peer = PeerPosition {
                anchor: anchor.clone(),
                position: anchor.clone(),
            };
            let pending = transaction::PendingBatch::current(&runtime)
                .ok_or_else(|| invalid("logical peer enrollment needs a native transaction"))?;
            pending.stage(vec![
                operation(roster_key(), encode(&roster)?),
                operation(peer_key(&anchor.origin), encode(&peer)?),
            ])?;
            Ok(anchor)
        })
        .await
    }

    pub async fn logical_capture_peer_position(
        self: &Arc<Self>,
        origin: &str,
    ) -> StorageResult<Option<Position>> {
        let scope = read::PackReadScope::for_runtime(self).await?;
        scope
            .run_result(async {
                let roster = state::roster(self).await?;
                Ok(state::peer(self, &roster, origin)
                    .await?
                    .map(|peer| peer.position))
            })
            .await
    }

    /// Read only the bounded roster's exact keys; no event payload enters status.
    pub async fn logical_capture_peer_positions(
        self: &Arc<Self>,
    ) -> StorageResult<Vec<PeerPosition>> {
        let scope = read::PackReadScope::for_runtime(self).await?;
        scope.run_result(self.read_peer_positions()).await
    }

    async fn read_peer_positions(self: &Arc<Self>) -> StorageResult<Vec<PeerPosition>> {
        let roster = state::roster(self).await?;
        let values = state::values(
            self,
            roster
                .origins
                .iter()
                .map(|origin| peer_key(origin))
                .collect(),
        )
        .await?;
        let mut peers = Vec::with_capacity(values.len());
        let mut receipts = 0u64;
        for (origin, bytes) in roster.origins.iter().zip(values) {
            let peer: PeerPosition =
                decode(&bytes.ok_or_else(|| invalid("logical peer position is absent"))?)?;
            peer.validate(origin)?;
            receipts = receipts
                .checked_add(peer.position.sequence)
                .ok_or_else(|| invalid("logical peer receipt count overflow"))?;
            peers.push(peer);
        }
        if receipts != roster.receipt_count {
            return Err(invalid("logical peer positions and receipt count disagree"));
        }
        Ok(peers)
    }
}
