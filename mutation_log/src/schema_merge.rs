//! Order-independent additive field merge for concurrent `schema` declarations.

use std::collections::{BTreeMap, BTreeSet};

use crate::json_val::{encode_json, parse_json, JsonVal};

const PER_FIELD_MAPS: [&str; 7] = [
    "field_types",
    "field_classifications",
    "field_descriptions",
    "field_interest_categories",
    "field_hashes",
    "field_versions",
    "field_declarations",
];

/// Union fields added concurrently in `other` into `winner` when both schemas
/// share identical `name`, `schema_type`, `record_keys`, `file_required`, and
/// overlapping field types. Returns `None` when incompatible or when `winner`
/// already contains every field of `other` (leaving `winner` bytes verbatim).
pub(crate) fn merge_schema_definitions(winner: &[u8], other: &[u8]) -> Option<Vec<u8>> {
    let JsonVal::Object(mut win_obj) = parse_json(winner)? else {
        return None;
    };
    let JsonVal::Object(other_obj) = parse_json(other)? else {
        return None;
    };
    if !layout_compatible(&win_obj, &other_obj) || !field_types_compatible(&win_obj, &other_obj) {
        return None;
    }
    let win_fields = declared_fields(&win_obj)?;
    let other_fields = declared_fields(&other_obj)?;
    let added: BTreeSet<String> = other_fields.difference(&win_fields).cloned().collect();
    if added.is_empty() {
        return None;
    }
    let all_fields: Vec<JsonVal> = win_fields
        .union(&other_fields)
        .cloned()
        .map(JsonVal::Str)
        .collect();
    win_obj.insert("fields".to_owned(), JsonVal::Array(all_fields));
    copy_added_field_metadata(&mut win_obj, &other_obj, &added);
    win_obj.remove("identity_hash");
    win_obj.remove("identity_hash_algo_version");
    Some(encode_json(&JsonVal::Object(win_obj)))
}

fn layout_compatible(win: &BTreeMap<String, JsonVal>, other: &BTreeMap<String, JsonVal>) -> bool {
    win.get("name") == other.get("name")
        && win.get("schema_type") == other.get("schema_type")
        && win.get("record_keys") == other.get("record_keys")
        && bool_or_false(win.get("file_required")) == bool_or_false(other.get("file_required"))
}

fn bool_or_false(val: Option<&JsonVal>) -> Option<bool> {
    match val {
        None | Some(JsonVal::Null) => Some(false),
        Some(JsonVal::Bool(b)) => Some(*b),
        Some(_) => None,
    }
}

fn field_types_compatible(
    win: &BTreeMap<String, JsonVal>,
    other: &BTreeMap<String, JsonVal>,
) -> bool {
    let win_ft = match win.get("field_types") {
        None | Some(JsonVal::Null) => None,
        Some(JsonVal::Object(map)) => Some(map),
        Some(_) => return false,
    };
    let other_ft = match other.get("field_types") {
        None | Some(JsonVal::Null) => None,
        Some(JsonVal::Object(map)) => Some(map),
        Some(_) => return false,
    };
    let (Some(win_map), Some(other_map)) = (win_ft, other_ft) else {
        return true;
    };
    other_map
        .iter()
        .all(|(field, ty)| win_map.get(field).is_none_or(|win_ty| win_ty == ty))
}

fn declared_fields(obj: &BTreeMap<String, JsonVal>) -> Option<BTreeSet<String>> {
    let mut set = BTreeSet::new();
    match obj.get("fields") {
        None | Some(JsonVal::Null) => {}
        Some(JsonVal::Array(arr)) => {
            for item in arr {
                let JsonVal::Str(name) = item else {
                    return None;
                };
                set.insert(name.clone());
            }
        }
        Some(JsonVal::Object(map)) => {
            set.extend(map.keys().cloned());
        }
        Some(_) => return None,
    }
    if let Some(JsonVal::Object(ft)) = obj.get("field_types") {
        set.extend(ft.keys().cloned());
    }
    Some(set)
}

fn copy_added_field_metadata(
    win: &mut BTreeMap<String, JsonVal>,
    other: &BTreeMap<String, JsonVal>,
    added: &BTreeSet<String>,
) {
    for map_name in PER_FIELD_MAPS {
        let Some(JsonVal::Object(other_map)) = other.get(map_name) else {
            continue;
        };
        let mut to_insert = Vec::new();
        for field in added {
            if let Some(val) = other_map.get(field) {
                to_insert.push((field.clone(), val.clone()));
            }
        }
        if to_insert.is_empty() {
            continue;
        }
        let entry = win
            .entry(map_name.to_owned())
            .or_insert_with(|| JsonVal::Object(BTreeMap::new()));
        if let JsonVal::Object(win_map) = entry {
            for (field, val) in to_insert {
                win_map.entry(field).or_insert(val);
            }
        }
    }
}
