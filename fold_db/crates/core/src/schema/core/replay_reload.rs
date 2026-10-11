//! Replace all schema authority only after a required replay read succeeds.

use super::locks::{lock_map, lock_set, write_map};
use super::SchemaCore;
use crate::schema::SchemaError;

impl SchemaCore {
    /// Refresh schema bodies, states, supersession and retired name claims.
    /// Returns the number of durable schema bodies loaded. Unlike boot and the
    /// normal merge reload, unreadable authority refuses dependent replay.
    pub(crate) async fn reload_for_replay(&self) -> Result<usize, SchemaError> {
        let catalog = self.db_ops.schemas().load_replay_catalog().await?;
        let count = catalog.schemas.len();
        // Acquire every lock before replacing a map, so a poisoned lock cannot
        // leave a partially refreshed authority set. No await under these locks.
        let mut schemas = write_map(&self.schemas, "schemas")?;
        let mut states = lock_map(&self.schema_states, "schema_states")?;
        let mut superseded_by = lock_map(&self.superseded_by, "superseded_by")?;
        let mut retired = lock_set(&self.retired_name_claims, "retired_name_claims")?;
        *schemas = catalog.schemas;
        *states = catalog.states;
        *superseded_by = catalog.superseded_by;
        *retired = catalog.retired_name_claims;
        self.mark_catalog_ready();
        Ok(count)
    }
}
