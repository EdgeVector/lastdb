//! Record batch and alternate-key pointer encoding.

use super::{
    applied_deletes, inline_files, inv, AlternateAddress, AlternateEntry, RecordChange,
    MAX_ALTERNATE_ENTRIES,
};
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_pack::record_warm::WrittenRecord;
use crate::storage::format::metadata::{record_value_error, FileChange};
use crate::storage::KvMutation;
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_value::InlineWrite;
use std::collections::HashSet;
use std::sync::Arc;

type EncodedBatch = (Vec<KvMutation>, Vec<FileChange>, Vec<WrittenRecord>);

fn encode_alternates(
    schema: &Schema,
    changes: &[RecordChange],
    node_key: &[u8; 32],
) -> Result<Vec<KvMutation>, SchemaError> {
    let defs = schema.require_record_keys()?;
    if defs.alternates.is_empty() {
        return Ok(Vec::new());
    }
    let mut pointers = Vec::new();
    for change in changes {
        let before = change.before.as_ref().filter(|record| record.is_live());
        let after = change.after.is_live().then_some(&change.after);
        for (name, def) in &defs.alternates {
            let old = before.and_then(|record| record.alternates.get(name)?.as_ref());
            let new = after.and_then(|record| record.alternates.get(name)?.as_ref());
            if old == new {
                continue;
            }
            let field_key = alternate_field_key_id(schema, name);
            let codec = FieldKeyCodec::from_node_key(
                node_key,
                &field_key,
                def.range_profile().map_err(inv)?,
            );
            let address = |value| {
                AlternateAddress::new(&codec, &field_key, value, &change.key).map(|a| a.encoded)
            };
            if let Some(old) = old {
                pointers.push(KvMutation::delete(address(old)?));
            }
            if let Some(new) = new {
                let entry = AlternateEntry {
                    primary: change.key.key.clone(),
                    matched: new.clone(),
                };
                let bytes =
                    serde_json::to_vec(&entry).map_err(|_| inv("encode alternate entry"))?;
                pointers.push(KvMutation::put(address(new)?, bytes));
            }
            if pointers.len() > MAX_ALTERNATE_ENTRIES {
                return Err(inv("alternate change batch exceeds 4096 entries"));
            }
        }
    }
    Ok(pointers)
}

pub(super) fn encode_batch(
    schema: &Schema,
    changes: Vec<RecordChange>,
    node_key: &[u8; 32],
) -> Result<EncodedBatch, SchemaError> {
    let alternates = encode_alternates(schema, &changes, node_key)?;
    let count = changes.len();
    let mut seen = HashSet::with_capacity(count);
    if !changes
        .iter()
        .all(|change| seen.insert(change.key.as_str()))
    {
        return Err(inv("a record batch repeats a record address"));
    }
    let mut main = Vec::with_capacity(count + alternates.len());
    let (mut files, mut written) = (Vec::with_capacity(count), Vec::with_capacity(count));
    for change in changes {
        if change.after.is_live() && change.after.key != change.key.key {
            return Err(inv("record payload differs from its bound primary address"));
        }
        let before = change.before.filter(|record| record.is_live());
        let after = Arc::new(change.after);
        files.push(FileChange {
            address: change.key.clone(),
            before,
            after: Arc::clone(&after),
            write: change.file_write,
        });
        let encoded_key = change.key.encoded;
        if after.is_live() {
            match &change.inline_write {
                Some(InlineWrite::Put(file))
                    if after.inline_file.as_ref() == Some(&file.marker) =>
                {
                    main.push(inline_files::put(&encoded_key, file)?);
                }
                Some(InlineWrite::Clear) if after.inline_file.is_none() => {
                    main.push(inline_files::delete(&encoded_key));
                }
                None => {}
                Some(_) => return Err(inv("the inline file write differs from the record marker")),
            }
            let value = after
                .encode_uncapped()
                .map_err(|e| record_value_error(&e))?;
            written.push(WrittenRecord::new(&encoded_key, after, &value));
            main.push(KvMutation::put(encoded_key, value));
        } else {
            let delete_marker = applied_deletes::encode(&encoded_key, &after.stamp)?;
            main.extend([
                inline_files::delete(&encoded_key),
                delete_marker,
                KvMutation::delete(encoded_key),
            ]);
        }
    }
    main.extend(alternates);
    Ok((main, files, written))
}
