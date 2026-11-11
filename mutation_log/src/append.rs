use crate::{
    AppendBatch, DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, StoredEntry,
    APPEND_BATCH_LIMIT,
};

/// Result of an append that did not fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The batch stored nothing.
    ///
    /// `next_seq` is the next writer sequence this stream will accept: one past
    /// the stored tip, not one past a sequence this call did not store.
    Duplicate {
        /// Next acceptable writer sequence on the batch's stream.
        next_seq: u64,
    },
    /// The new suffix was stored as one transaction.
    Accepted {
        /// Delivery sequences assigned to the new entries, in batch order.
        delivery_seqs: Vec<u64>,
    },
}

impl LogState {
    /// Apply one batch. A gap or an app-private namespace stores nothing.
    ///
    /// Dedupe uses the writer sequence. A sequence at or below the stream's
    /// last accepted value is a duplicate. The next value is accepted. A
    /// higher value is a gap. An incorporation of a submission that was already
    /// copied is also a duplicate, and it does not advance the stream.
    pub fn append(
        &self,
        batch: &AppendBatch,
        policy: &impl NamespacePolicy,
    ) -> Result<(Self, AppendOutcome), LogError> {
        validate_batch(self, batch)?;
        self.authorize_append(&batch.author, &batch.stream)?;
        reject_app_private(self, batch, policy)?;
        if !self.inflight.contains_key(&batch.stream) {
            return Err(LogError::AppendNotOpen);
        }
        let last = self
            .streams
            .get(&batch.stream)
            .map(|progress| progress.last_writer_seq)
            .ok_or(LogError::Unauthorized)?;
        let seqs: Vec<u64> = batch.entries.iter().map(|entry| entry.writer_seq).collect();
        let start = classify_seqs(last, &seqs)?;
        if start == seqs.len() || repeats_incorporation(self, batch, start) {
            return Ok((self.clone(), duplicate_outcome(last)?));
        }
        let added = (seqs.len() - start) as u64;
        let delivery = self.head.as_ref().map_or(0, |head| head.delivery_seq);
        if delivery.checked_add(added).is_none() {
            return Err(LogError::InvalidSequence);
        }
        Ok(commit(self, batch, start))
    }
}

fn validate_batch(state: &LogState, batch: &AppendBatch) -> Result<(), LogError> {
    if batch.entries.is_empty() {
        return Err(LogError::EmptyBatch);
    }
    if batch.entries.len() > APPEND_BATCH_LIMIT {
        return Err(LogError::BatchTooLarge);
    }
    for entry in &batch.entries {
        if entry.database_id != state.database_id {
            return Err(LogError::WrongDatabase);
        }
        if entry.stream != batch.stream || entry.author != batch.author {
            return Err(LogError::StreamMismatch);
        }
        require_submission(state, entry)?;
    }
    Ok(())
}

fn on_publisher_stream(state: &LogState, stream: &crate::StreamId) -> bool {
    matches!(&state.kind, DatabaseKind::Shareable { publisher_stream, .. } if publisher_stream == stream)
}

fn reject_app_private(
    state: &LogState,
    batch: &AppendBatch,
    policy: &impl NamespacePolicy,
) -> Result<(), LogError> {
    if !matches!(state.kind, DatabaseKind::Shareable { .. }) {
        return Ok(());
    }
    if batch
        .entries
        .iter()
        .any(|entry| policy.classify(&entry.namespace) == NamespaceKind::AppPrivate)
    {
        return Err(LogError::AppPrivateNamespace);
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

fn repeats_incorporation(state: &LogState, batch: &AppendBatch, start: usize) -> bool {
    let mut fresh = batch.entries.iter().skip(start).peekable();
    if fresh.peek().is_none() {
        return false;
    }
    fresh.all(|entry| match &entry.incorporated_from {
        Some(from) => state
            .incorporated
            .contains(&(from.submission_stream.clone(), from.writer_seq)),
        None => false,
    })
}

fn require_submission(state: &LogState, entry: &crate::Entry) -> Result<(), LogError> {
    let Some(from) = &entry.incorporated_from else {
        return Ok(());
    };
    if !on_publisher_stream(state, &entry.stream) {
        return Err(LogError::InvalidEntry);
    }
    if state
        .incorporated
        .contains(&(from.submission_stream.clone(), from.writer_seq))
    {
        return Ok(());
    }
    let matched = state.entries.iter().any(|stored| {
        stored.entry.incorporated_from.is_none()
            && stored.entry.stream == from.submission_stream
            && stored.entry.writer_seq == from.writer_seq
            && stored.entry.author == from.contributor
            && stored.entry.namespace == entry.namespace
            && stored.entry.payload == entry.payload
    });
    if matched {
        Ok(())
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
        next.head = Some(crate::Head {
            delivery_seq: delivery,
            stamp: entry.stamp(),
        });
        next.entries.push(StoredEntry {
            delivery_seq: delivery,
            accepted_at_ms: batch.accepted_at_ms,
            entry: entry.clone(),
        });
        if let Some(from) = &entry.incorporated_from {
            next.incorporated
                .insert((from.submission_stream.clone(), from.writer_seq));
        }
    }
    if let Some(progress) = next.streams.get_mut(&batch.stream) {
        if let Some(entry) = batch.entries.last() {
            progress.last_writer_seq = entry.writer_seq;
        }
    }
    (next, AppendOutcome::Accepted { delivery_seqs })
}
