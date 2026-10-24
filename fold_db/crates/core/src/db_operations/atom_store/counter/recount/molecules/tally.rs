//! Validate and count every source family before molecule repair writes.

use super::{invalid, Count, Counts, Pass};
use crate::db_operations::atom_store::counter::scan::{
    family_range, record_holders, record_molecules, tip_holders, tip_molecules, Family,
};
use crate::db_operations::atom_store::molecule_ref_edges::{molecule_target, MoleculeRefEdge};
use crate::db_operations::atom_store::types::MoleculeHeader;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;

const MAX_MOLECULES: usize = 100_000;
const MAX_HEADER_BYTES: usize = 64 * 1024 * 1024;

fn selected<'a>(
    counts: &'a mut Counts,
    token: &str,
    pass: &Pass<'_>,
) -> Result<Option<&'a mut Count>, SchemaError> {
    if !pass.owns(token) {
        return Ok(None);
    }
    if !counts.contains_key(token) && counts.len() >= MAX_MOLECULES {
        return Err(invalid("too many molecules in one pass; increase --shards"));
    }
    Ok(Some(counts.entry(token.to_string()).or_default()))
}

fn add(
    counts: &mut Counts,
    molecule: &str,
    n: u32,
    tips: bool,
    pass: &Pass<'_>,
) -> Result<(), SchemaError> {
    if let Some(count) = selected(counts, &molecule_target(molecule), pass)? {
        count.molecule = Some(molecule.to_string());
        let total = if tips {
            &mut count.slots
        } else {
            &mut count.records
        };
        *total = total
            .checked_add(u64::from(n))
            .ok_or_else(|| invalid("count overflow"))?;
    }
    Ok(())
}

impl AtomStore {
    pub(super) async fn tally_molecule_recount(
        &self,
        pass: &Pass<'_>,
    ) -> Result<Counts, SchemaError> {
        let mut counts = Counts::new();
        let mut header_bytes = 0usize;
        for family in [
            Family::Tips,
            Family::Records,
            Family::Headers,
            Family::BindingRecords,
            Family::BindingCounters,
        ] {
            let (start, end) = family_range(family, pass.scope);
            let mut walk = RangeWalk::new(&start, &end, 512);
            while !walk.done() {
                let page = self.walk_step(&mut walk).await?;
                if page.unsealed != 0 {
                    return Err(invalid("a source read discarded undecodable storage; no molecule changes were written"));
                }
                match family {
                    Family::Tips | Family::Records => {
                        tally_references(&mut counts, &page.rows, family, pass)?
                    }
                    Family::Headers => {
                        for (key, value) in page.rows {
                            serde_json::from_slice::<MoleculeHeader>(&value).map_err(invalid)?;
                            let key = std::str::from_utf8(&key).map_err(invalid)?;
                            let molecule =
                                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(
                                    key,
                                )
                                .ok_or_else(|| invalid("invalid molecule header key"))?;
                            if let Some(count) =
                                selected(&mut counts, &molecule_target(molecule), pass)?
                            {
                                header_bytes = header_bytes.saturating_add(value.len());
                                if header_bytes > MAX_HEADER_BYTES {
                                    return Err(invalid(
                                        "header budget exceeded; increase --shards",
                                    ));
                                }
                                count.molecule = Some(molecule.to_string());
                                count.header =
                                    Some(serde_json::from_slice(&value).map_err(invalid)?);
                            }
                        }
                    }
                    Family::BindingRecords => tally_bindings(&mut counts, page.rows, pass)?,
                    Family::BindingCounters => {
                        for (key, value) in page.rows {
                            let token = key
                                .strip_prefix(start.as_slice())
                                .ok_or_else(|| invalid("binding counter scope differs"))?;
                            let token = std::str::from_utf8(token).map_err(invalid)?;
                            if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
                                return Err(invalid("invalid binding counter token"));
                            }
                            if let Some(count) = selected(&mut counts, token, pass)? {
                                count.binding_counter = Some(value);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        if counts
            .values()
            .any(|count| (count.slots > 0 || count.records > 0) && count.header.is_none())
        {
            return Err(invalid(
                "a live molecule has no header; restore its metadata before recount",
            ));
        }
        Ok(counts)
    }
}

fn tally_references(
    counts: &mut Counts,
    rows: &[(Vec<u8>, Vec<u8>)],
    family: Family,
    pass: &Pass<'_>,
) -> Result<(), SchemaError> {
    let tips = matches!(family, Family::Tips);
    let bad = if tips {
        tip_holders(rows).1
    } else {
        record_holders(rows).1
    };
    if bad.total != 0 {
        return Err(invalid("a tip or counted conflict record does not decode"));
    }
    let found = if tips {
        tip_molecules(rows)
    } else {
        record_molecules(rows)
    };
    if tips && found.values().map(|n| *n as usize).sum::<usize>() != rows.len() {
        return Err(invalid("a tip has no molecule identity"));
    }
    for (molecule, n) in found {
        add(counts, &molecule, n, tips, pass)?;
    }
    Ok(())
}

fn tally_bindings(
    counts: &mut Counts,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    pass: &Pass<'_>,
) -> Result<(), SchemaError> {
    for (key, value) in rows {
        let edge: MoleculeRefEdge = serde_json::from_slice(&value).map_err(invalid)?;
        if edge.storage_key(pass.scope).as_bytes() != key.as_slice()
            || edge.molecule_uuid.is_empty()
        {
            return Err(invalid("binding record identity differs from its key"));
        }
        if let Some(count) = selected(counts, &molecule_target(&edge.molecule_uuid), pass)? {
            count.molecule = Some(edge.molecule_uuid);
            count.bindings = count
                .bindings
                .checked_add(1)
                .ok_or_else(|| invalid("bindings overflow"))?;
        }
    }
    Ok(())
}
