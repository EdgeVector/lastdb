//! Restore only this schema's installed molecule map before publication.

use crate::schema::types::Schema;

pub(super) fn restore_owned_molecules(schema: &mut Schema, stored: Option<&Schema>) {
    let Some(molecules) = stored.and_then(|stored| stored.field_molecule_uuids.as_ref()) else {
        return;
    };
    for (name, field) in &mut schema.runtime_fields {
        if let Some(uuid) = molecules.get(name) {
            field.common_mut().set_molecule_uuid(uuid.clone());
        }
    }
}
