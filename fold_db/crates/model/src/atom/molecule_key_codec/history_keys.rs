/// `history\0{molecule}:` — prefix covering one molecule's history rows.
#[must_use]
pub fn history_molecule_prefix(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored("history", &format!("{molecule_uuid}:"))
}
