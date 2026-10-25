//! One-time read-only CI observation of the supported v2 merge state.

use super::*;

impl PackRuntime {
    pub fn ci_run_status(&self) -> serde_json::Value {
        match &self.physical {
            physical::Physical::V1(_) => serde_json::json!({"format": 1}),
            physical::Physical::V2(v2) => {
                let status = v2.home.status();
                serde_json::json!({"format": 2, "generation": status.generation,
                    "frontier": status.frontier, "levels": status.levels,
                    "merges": status.generation.saturating_sub(status.frontier)})
            }
        }
    }
}
