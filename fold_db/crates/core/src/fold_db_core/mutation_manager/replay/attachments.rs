//! Prepare complete attachment slots without exposing a deferred record.

use super::{
    candidate::same_content,
    records::as_mutation,
    state::{file_path, invalid},
    MutationManager,
};
use crate::schema::types::{FileWrite, Mutation, SchemaError};
use crate::storage::format::{
    metadata::ReplayCapture,
    replay_pin::{self, AdmittedBody, PinState},
};
use lastdb_model::record_value::{FileMarker, InlineFile, InlineWrite, StoredRecord};

pub(super) struct AttachmentContext<'a> {
    pub address: &'a str,
    pub before: Option<&'a StoredRecord>,
    pub live: Option<&'a StoredRecord>,
    pub capture: &'a ReplayCapture,
}

pub(super) struct Pins {
    pub inline: Option<Vec<u8>>,
    pub file_ref: Option<Vec<u8>>,
}

pub(super) fn inline_row(bytes: &[u8], marker: &FileMarker) -> Result<InlineFile, SchemaError> {
    let file = InlineFile::decode(bytes).map_err(invalid)?;
    if file.marker != *marker {
        return Err(invalid("inline pin differs from its marker"));
    }
    file.bytes().map_err(invalid)?;
    Ok(file)
}

impl MutationManager {
    pub(super) async fn candidate_attachments(
        &self,
        context: &AttachmentContext<'_>,
        mutation: &Mutation,
        next: &mut StoredRecord,
        save_file: bool,
    ) -> Result<Pins, SchemaError> {
        let inline = match (&next.inline_file, &mutation.inline_write) {
            (Some(marker), Some(InlineWrite::Put(file))) if marker == &file.marker => {
                file.bytes().map_err(invalid)?;
                Some(file.encode().map_err(invalid)?)
            }
            (Some(marker), None) => Some(
                self.saved_inline(context, marker)
                    .await?
                    .encode()
                    .map_err(invalid)?,
            ),
            (None, None | Some(InlineWrite::Clear)) => None,
            _ => return Err(invalid("inline candidate differs from its write")),
        };
        let file_ref = self
            .candidate_file(context, mutation, next, save_file)
            .await?;
        Ok(Pins { inline, file_ref })
    }

    async fn saved_inline(
        &self,
        context: &AttachmentContext<'_>,
        marker: &FileMarker,
    ) -> Result<InlineFile, SchemaError> {
        if let Some(row) = &context.capture.inline_pin {
            return inline_row(row, marker);
        }
        if context.live.and_then(|r| r.inline_file.as_ref()) == Some(marker) {
            let row = replay_pin::read_inline_rows(
                &self.db_ops.records().format,
                &[context.address.to_owned()],
            )
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| invalid("winning inline bytes are missing"))?;
            return inline_row(&row, marker);
        }
        Err(invalid("winning inline bytes are missing"))
    }

    async fn candidate_file(
        &self,
        context: &AttachmentContext<'_>,
        mutation: &Mutation,
        next: &mut StoredRecord,
        save_file: bool,
    ) -> Result<Option<Vec<u8>>, SchemaError> {
        let format = &self.db_ops.records().format;
        let Some(marker) = next.file.clone() else {
            return Ok(None);
        };
        if let Some(write) = &mutation.file_write {
            match replay_pin::admit_write(format, write)? {
                AdmittedBody::Row {
                    marker: admitted,
                    row,
                } if marker == admitted => {
                    next.file_version = context
                        .live
                        .filter(|r| r.file.as_ref() == Some(&marker))
                        .and_then(|r| r.file_version.clone())
                        .or_else(|| Some(next.stamp.position.clone()));
                    let path = file_path(context.address, Some(next))?
                        .ok_or_else(|| invalid("file version missing"))?;
                    if save_file {
                        replay_pin::put_cas_row(format, &path, row).await?;
                    }
                    return Ok(None);
                }
                AdmittedBody::Local {
                    marker: admitted,
                    path,
                } if marker == admitted => {
                    let path = path
                        .to_str()
                        .ok_or_else(|| invalid("non-UTF-8 file reference"))?;
                    replay_pin::verify_external(path, &marker).await?;
                    next.file_version = Some(next.stamp.position.clone());
                    return Ok(Some(path.as_bytes().to_vec()));
                }
                _ => return Err(invalid("candidate file differs from its write")),
            }
        }
        if context.before.and_then(|r| r.file.as_ref()) != Some(&marker) {
            return Err(invalid("file Keep has no confirmed preimage"));
        }
        let path = file_path(context.address, Some(next))?
            .ok_or_else(|| invalid("file version missing"))?;
        if matches!(
            replay_pin::cas_state(format, &path, &marker).await?,
            PinState::Ready
        ) {
            return Ok(None);
        }
        if let Some(reference) = context.capture.file_ref.as_deref() {
            replay_pin::verify_external(std::str::from_utf8(reference).map_err(invalid)?, &marker)
                .await?;
            return Ok(Some(reference.to_vec()));
        }
        if matches!(
            replay_pin::catalog_state(&format.runtime(), &path, &marker)?,
            PinState::Ready
        ) {
            self.winning_file(context, next).await?;
            return Ok(None);
        }
        if let Some(live) = context.live.filter(|r| r.file.as_ref() == Some(&marker)) {
            self.read_live_file(context.address, live).await?;
            next.file_version.clone_from(&live.file_version);
            return Ok(None);
        }
        Err(invalid("winning file bytes are missing"))
    }

    async fn winning_file(
        &self,
        context: &AttachmentContext<'_>,
        saved: &StoredRecord,
    ) -> Result<Vec<u8>, SchemaError> {
        let marker = saved
            .file
            .as_ref()
            .ok_or_else(|| invalid("file marker missing"))?;
        let version = saved
            .file_version
            .as_ref()
            .ok_or_else(|| invalid("file version missing"))?;
        let reference = context
            .capture
            .file_ref
            .as_deref()
            .map(std::str::from_utf8)
            .transpose()
            .map_err(invalid)?;
        let saved_bytes = replay_pin::read_winning_bytes(
            &self.db_ops.records().format,
            context.address,
            version,
            marker,
            reference,
        )
        .await;
        if saved_bytes.is_err() {
            if let Some(live) = context.live.filter(|r| r.file.as_ref() == Some(marker)) {
                return self.read_live_file(context.address, live).await;
            }
        }
        saved_bytes
    }

    async fn read_live_file(
        &self,
        address: &str,
        live: &StoredRecord,
    ) -> Result<Vec<u8>, SchemaError> {
        replay_pin::read_winning_bytes(
            &self.db_ops.records().format,
            address,
            live.file_version
                .as_ref()
                .ok_or_else(|| invalid("live file version missing"))?,
            live.file
                .as_ref()
                .ok_or_else(|| invalid("live file marker missing"))?,
            None,
        )
        .await
    }

    pub(super) async fn materialize_attachments(
        &self,
        context: &AttachmentContext<'_>,
        saved: &StoredRecord,
        mutation: &mut Mutation,
        unchanged: bool,
    ) -> Result<(), SchemaError> {
        if let Some(marker) = &saved.file {
            let bytes = self.winning_file(context, saved).await?;
            if !unchanged {
                mutation.file = Some(marker.clone());
                mutation.file_write = Some(
                    self.db_ops
                        .records()
                        .format
                        .seal_record_file(&bytes, marker.name.clone(), marker.media_type.clone())
                        .map_err(invalid)?,
                );
            }
        } else {
            mutation.file_write = Some(FileWrite::Clear);
        }
        mutation.inline_write = Some(match &saved.inline_file {
            Some(marker) => InlineWrite::Put(self.saved_inline(context, marker).await?),
            None => InlineWrite::Clear,
        });
        Ok(())
    }

    /// An immediately settled entry already owns its admitted file payload. Pass it
    /// straight to the record writer instead of staging and then copying a large row.
    pub(super) async fn settle_candidate(
        &self,
        context: &AttachmentContext<'_>,
        original: &Mutation,
        record: Option<&StoredRecord>,
        pins: &Pins,
    ) -> Result<(), SchemaError> {
        let unchanged = same_content(context.live, record);
        let mut mutation = as_mutation(
            &original.schema_name,
            original.key_value.clone(),
            record.cloned(),
        );
        if let Some(record) = record {
            if let Some(write) = &original.file_write {
                mutation.file.clone_from(&record.file);
                mutation.file_write = Some(write.clone());
            } else if let Some(marker) = &record.file {
                let bytes = self.winning_file(context, record).await?;
                if !unchanged {
                    mutation.file = Some(marker.clone());
                    mutation.file_write = Some(
                        self.db_ops
                            .records()
                            .format
                            .seal_record_file(
                                &bytes,
                                marker.name.clone(),
                                marker.media_type.clone(),
                            )
                            .map_err(invalid)?,
                    );
                }
            } else {
                mutation.file_write = Some(FileWrite::Clear);
            }
            mutation.inline_write = Some(match &record.inline_file {
                Some(marker) => InlineWrite::Put(inline_row(
                    pins.inline
                        .as_deref()
                        .ok_or_else(|| invalid("inline candidate missing"))?,
                    marker,
                )?),
                None => InlineWrite::Clear,
            });
        }
        if !unchanged {
            let writer = original
                .writer
                .as_deref()
                .ok_or_else(|| invalid("replay writer is missing"))?;
            self.apply_replay_record(mutation, writer).await?;
        }
        Ok(())
    }
}
