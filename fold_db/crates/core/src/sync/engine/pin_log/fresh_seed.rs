//! Install only the incorporated frontier of a verified fresh replacement.

use super::*;

impl PinLog {
    pub(crate) async fn install_fresh_seed_frontiers(
        &self,
        frontiers: &BTreeMap<String, BTreeMap<String, u64>>,
        inherited_floor: u64,
    ) -> Result<u64, String> {
        let _append = self.append_lock.lock().await;
        let _published = self.published_f_lock.lock().await;
        let floor = frontiers
            .values()
            .flat_map(|frontier| frontier.values().copied())
            .max()
            .unwrap_or(0)
            .max(inherited_floor);
        if floor == u64::MAX {
            return Err("fresh replacement exhausts the writer allocation floor".into());
        }
        let mut rows = frontiers
            .iter()
            .map(|(prefix, frontier)| {
                Ok((
                    pin_log_published_f_key(&target_id_for_prefix(prefix)),
                    serde_json::to_vec(frontier).map_err(|error| error.to_string())?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        rows.push((
            PIN_LOG_APPENDED_F_KEY.to_vec(),
            floor.to_be_bytes().to_vec(),
        ));
        let store = self.pin_log_store().await?;
        store
            .batch_put(rows)
            .await
            .map_err(|error| error.to_string())?;
        store.flush().await.map_err(|error| error.to_string())?;
        self.state.lock().await.clear();
        self.known_pending_prefixes.lock().await.clear();
        Ok(floor)
    }
}
