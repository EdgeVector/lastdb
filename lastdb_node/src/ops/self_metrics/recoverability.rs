use super::*;

/// The manual V2 backup line of the status report.
pub(super) fn durability_status_line(durability: &DurabilityHealth) -> String {
    durability.status_line()
}
