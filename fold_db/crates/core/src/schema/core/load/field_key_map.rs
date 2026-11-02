//! Restore only this schema's installed field key map before publication.

use crate::schema::types::Schema;

pub(super) fn restore_owned_field_keys(schema: &mut Schema, stored: Option<&Schema>) {
    let Some(field_keys) = stored.and_then(|stored| stored.field_key_ids.as_ref()) else {
        return;
    };
    for (name, field) in &mut schema.runtime_fields {
        if let Some(uuid) = field_keys.get(name) {
            field.common_mut().set_field_key_id(uuid.clone());
        }
    }
}
