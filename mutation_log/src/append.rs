use crate::{
    AppendBatch, BoundPolicy, DatabaseKind, Entry, LogError, LogState, StoredEntry, Writer,
    APPEND_BATCH_LIMIT,
};

/// Result of an append that did not fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The batch stored nothing.
    ///
    /// `next_seq` is the next writer sequence this writer will accept: one past
    /// the stored tip, not one past a sequence this call did not store.
    Duplicate {
        /// Next acceptable writer sequence for this writer.
        next_seq: u64,
    },
    /// The new suffix was stored as one transaction.
    Accepted {
        /// Delivery sequences assigned to the new entries, in batch order.
        delivery_seqs: Vec<u64>,
    },
}

impl LogState {
    fn authorize_append(&self, writer: &Writer) -> Result<(), LogError> {
        let allowed = match &self.kind {
            DatabaseKind::Personal => crate::read::holds_owner(self, &writer.key),
            DatabaseKind::Shareable { publisher } => {
                // `read: anyone` does not append. `write: direct` uses this
                // same check: an active Submit holder appends to this log.
                &writer.key == publisher || crate::read::holds_submit(self, &writer.key)
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(LogError::Unauthorized)
        }
    }

    /// Check a previously admitted entry against a loaded writer history.
    /// The caller authorizes the writer before reading that history. The
    /// separate inbox log uses this to recognize an exact append retry.
    pub fn is_append_retry(&self, entry: &Entry) -> Result<bool, LogError> {
        if entry.database_id != self.database_id {
            return Err(LogError::WrongDatabase);
        }
        let last = self.tips.get(&entry.writer).copied().unwrap_or(0);
        if entry.writer_seq > last {
            return Ok(false);
        }
        confirm_repeat(self, entry)?;
        Ok(true)
    }

    /// Apply one batch. A gap or an app-private namespace stores nothing.
    ///
    /// Dedupe is per writer (node key, device). A sequence at or below that
    /// writer's tip is a duplicate when the content matches, and a conflict
    /// when the stored entry differs. A trimmed sequence at or below the tip
    /// is a duplicate. The next value is accepted. A higher value is a gap.
    /// An exact re-append is a duplicate once in total. A different payload
    /// on a stored submission is [`LogError::Conflict`].
    pub fn append(
        &self,
        batch: &AppendBatch,
        policy: &BoundPolicy,
    ) -> Result<(Self, AppendOutcome), LogError> {
        check_batch(self, batch, policy)?;
        let last = self.tips.get(&batch.writer).copied().unwrap_or(0);
        confirm_repeats(self, batch, last)?;
        let seqs: Vec<u64> = batch.entries.iter().map(|entry| entry.writer_seq).collect();
        let start = classify_seqs(last, &seqs)?;
        if start == seqs.len() {
            return Ok((self.clone(), duplicate_outcome(last)?));
        }
        require_fresh(self, batch, start)?;
        if fresh_is_repeat(self, batch, start)? {
            return Ok((self.clone(), duplicate_outcome(last)?));
        }
        ensure_delivery_room(self, seqs.len() - start)?;
        Ok(commit(self, batch, start))
    }
}

fn check_batch(
    state: &LogState,
    batch: &AppendBatch,
    policy: &BoundPolicy,
) -> Result<(), LogError> {
    if batch.entries.is_empty() {
        return Err(LogError::EmptyBatch);
    }
    if batch.entries.len() > APPEND_BATCH_LIMIT {
        return Err(LogError::BatchTooLarge);
    }
    state.authorize_append(&batch.writer)?;
    for entry in &batch.entries {
        check_entry(state, batch, entry, policy)?;
    }
    Ok(())
}

fn check_entry(
    state: &LogState,
    batch: &AppendBatch,
    entry: &Entry,
    policy: &BoundPolicy,
) -> Result<(), LogError> {
    if entry.database_id != state.database_id {
        return Err(LogError::WrongDatabase);
    }
    if entry.writer != batch.writer {
        return Err(LogError::WriterMismatch);
    }
    if matches!(state.kind, DatabaseKind::Shareable { .. })
        && !policy.allows_in_share(&entry.namespace)
    {
        return Err(LogError::AppPrivateNamespace);
    }
    Ok(())
}

fn ensure_delivery_room(state: &LogState, added: usize) -> Result<(), LogError> {
    let delivery = state.head.as_ref().map_or(0, |head| head.delivery_seq);
    if delivery.checked_add(added as u64).is_none() {
        return Err(LogError::InvalidSequence);
    }
    Ok(())
}

/// Index of the first sequence that is not a duplicate.
fn classify_seqs(last: u64, seqs: &[u64]) -> Result<usize, LogError> {
    if seqs.windows(2).any(|pair| pair[1] <= pair[0]) || seqs.contains(&0) {
        return Err(LogError::InvalidSequence);
    }
    let mut index = 0;
    while index < seqs.len() && seqs[index] <= last {
        index += 1;
    }
    if index == seqs.len() {
        return Ok(index);
    }
    let mut expected = last.checked_add(1).ok_or(LogError::SequenceExhausted)?;
    let fresh = &seqs[index..];
    for (offset, seq) in fresh.iter().enumerate() {
        if *seq == u64::MAX {
            return Err(LogError::SequenceExhausted);
        }
        if *seq != expected {
            return Err(LogError::Gap {
                expected,
                got: *seq,
            });
        }
        if offset + 1 == fresh.len() {
            break;
        }
        expected = seq.checked_add(1).ok_or(LogError::SequenceExhausted)?;
    }
    Ok(index)
}

fn duplicate_outcome(last: u64) -> Result<AppendOutcome, LogError> {
    Ok(AppendOutcome::Duplicate {
        next_seq: last.checked_add(1).ok_or(LogError::SequenceExhausted)?,
    })
}

fn confirm_repeats(state: &LogState, batch: &AppendBatch, last: u64) -> Result<(), LogError> {
    for entry in &batch.entries {
        if entry.writer_seq <= last {
            confirm_repeat(state, entry)?;
        }
    }
    Ok(())
}

fn confirm_repeat(state: &LogState, entry: &Entry) -> Result<(), LogError> {
    match stored_entry(state, &entry.writer, entry.writer_seq) {
        Some(stored) if same_content(&stored.entry, entry) => Ok(()),
        None if entry.writer_seq <= trimmed_tip(state, &entry.writer) => Ok(()),
        Some(_) | None => Err(LogError::Conflict),
    }
}

fn stored_entry<'a>(state: &'a LogState, writer: &Writer, seq: u64) -> Option<&'a StoredEntry> {
    state
        .entries
        .iter()
        .find(|stored| stored.entry.writer == *writer && stored.entry.writer_seq == seq)
}

fn trimmed_tip(state: &LogState, writer: &Writer) -> u64 {
    state.trimmed_tips.get(writer).copied().unwrap_or(0)
}

fn same_content(stored: &Entry, entry: &Entry) -> bool {
    stored.payload == entry.payload
        && stored.namespace == entry.namespace
        && stored.incorporated_from == entry.incorporated_from
}

fn fresh_is_repeat(state: &LogState, batch: &AppendBatch, start: usize) -> Result<bool, LogError> {
    let mut copied = false;
    let mut unseen = false;
    for entry in &batch.entries[start..] {
        if already_copied(state, entry) {
            copied = true;
        } else {
            unseen = true;
        }
    }
    if copied && unseen {
        return Err(LogError::MixedIncorporation);
    }
    Ok(copied)
}

fn already_copied(state: &LogState, entry: &Entry) -> bool {
    entry.incorporated_from.as_ref().is_some_and(|from| {
        state
            .incorporated
            .contains(&(from.writer.clone(), from.writer_seq))
    })
}

fn require_fresh(state: &LogState, batch: &AppendBatch, start: usize) -> Result<(), LogError> {
    for entry in batch.entries.iter().skip(start) {
        require_submission(state, entry)?;
    }
    Ok(())
}

fn require_submission(state: &LogState, entry: &Entry) -> Result<(), LogError> {
    let Some(from) = &entry.incorporated_from else {
        return Ok(());
    };
    let DatabaseKind::Shareable { publisher } = &state.kind else {
        return Err(LogError::InvalidEntry);
    };
    if &entry.writer.key != publisher {
        return Err(LogError::InvalidEntry);
    }
    let Some(stored) = stored_entry(state, &from.writer, from.writer_seq) else {
        return Err(LogError::UnknownSubmission);
    };
    if stored.entry.incorporated_from.is_none()
        && stored.entry.namespace == entry.namespace
        && stored.entry.payload == entry.payload
    {
        Ok(())
    } else if stored.entry.incorporated_from.is_none() {
        Err(LogError::Conflict)
    } else {
        Err(LogError::UnknownSubmission)
    }
}

fn commit(state: &LogState, batch: &AppendBatch, start: usize) -> (LogState, AppendOutcome) {
    let mut next = state.clone();
    let mut delivery = next.head.as_ref().map_or(0, |head| head.delivery_seq);
    let mut delivery_seqs = Vec::new();
    for entry in batch.entries.iter().skip(start) {
        delivery = delivery
            .checked_add(1)
            .expect("delivery sequence fits the batch");
        delivery_seqs.push(delivery);
        let stored = StoredEntry {
            delivery_seq: delivery,
            accepted_at_ms: batch.accepted_at_ms,
            entry: entry.clone(),
        };
        next.head = Some(crate::Head {
            delivery_seq: delivery,
            stamp: stored.merge_stamp(),
        });
        next.entries.push(stored);
        if let Some(from) = &entry.incorporated_from {
            next.incorporated
                .insert((from.writer.clone(), from.writer_seq));
        }
    }
    if let Some(entry) = batch.entries.last() {
        next.tips.insert(batch.writer.clone(), entry.writer_seq);
    }
    (next, AppendOutcome::Accepted { delivery_seqs })
}
