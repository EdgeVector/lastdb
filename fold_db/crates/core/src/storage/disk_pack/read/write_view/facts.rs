//! Counters and exact confirmed absences are typed core facts, never bytes.

use super::*;
use crate::db_operations::atom_store::{counter::AtomCounter, PerKeyRecord};
use crate::resident::{CoreMemorySource, CoreMemoryValue, CoreWarmSet};

static FACTS: u8 = 0;

pub(super) enum WriteFact {
    Absent,
    Counter(AtomCounter),
}

pub(super) fn get(
    core: &mut CoreWarmSet,
    coordinate: &Coordinate,
) -> Option<Arc<CoreMemoryValue<WriteFact>>> {
    core.memory_get(owner(), &name(coordinate))
}

pub(super) fn retain(core: &mut CoreWarmSet, coordinate: &Coordinate, body: Option<&[u8]>) {
    if coordinate.0 == super::super::super::physical::CAS_COLLECTION {
        return;
    }
    let fact = match body {
        None => WriteFact::Absent,
        Some(body)
            if coordinate.0 == "atoms"
                && body.len() <= 16 * 1024
                && crate::atom::atom_key_codec::is_counter_key(&coordinate.1) =>
        {
            let Ok(counter) = serde_json::from_slice(body) else {
                return;
            };
            WriteFact::Counter(counter)
        }
        Some(body) => {
            if coordinate.0 == "tips"
                && crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(&coordinate.1)
                    .is_some()
            {
                if let Ok(tip) = serde_json::from_slice::<PerKeyRecord>(body) {
                    core.authorize_tip_source(
                        &coordinate.0,
                        &coordinate.1,
                        &tip.entry,
                        tip.meta.as_ref(),
                    );
                }
            }
            return;
        }
    };
    if get(core, coordinate).is_some() {
        return;
    }
    let bytes = match &fact {
        WriteFact::Absent => std::mem::size_of::<WriteFact>(),
        WriteFact::Counter(counter) => counter.blobs.iter().fold(
            std::mem::size_of::<WriteFact>()
                + counter.blobs.capacity() * std::mem::size_of::<String>()
                + counter.source_schema.as_ref().map_or(0, String::capacity),
            |bytes, blob| bytes.saturating_add(blob.capacity()),
        ),
    };
    core.memory_insert(
        owner(),
        name(coordinate),
        fact,
        vec![CoreMemorySource {
            collection: coordinate.0.clone(),
            key: Some(coordinate.1.clone()),
        }],
        bytes,
    );
}

fn owner() -> usize {
    std::ptr::from_ref(&FACTS) as usize
}

fn name(coordinate: &Coordinate) -> String {
    serde_json::to_string(coordinate).expect("exact typed fact coordinate")
}
