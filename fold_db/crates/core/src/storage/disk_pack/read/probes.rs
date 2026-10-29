//! Pair counter reads with body absence probes under the same held root.

use super::*;
use laststore::disk_run::home::{ProbedValue, ReadProbe};

impl PackRuntime {
    /// Private stages retain the existing raw read path. Disk-only pairs can
    /// share each block decode. None leaves that slot to the original reader.
    pub(crate) async fn counter_body_probes(
        self: &Arc<Self>,
        keys: Vec<(String, String)>,
    ) -> StorageResult<Vec<Option<ProbedValue>>> {
        let Some(pending) = transaction::PendingBatch::current(self) else {
            return Ok(keys.iter().map(|_| None).collect());
        };
        let context = ReadContext::current(self).await?;
        for (counter, body) in &keys {
            crate::storage::current_keys::validate("main", counter.as_bytes())?;
            crate::storage::current_keys::validate("main", body.as_bytes())?;
        }
        let runtime = Arc::clone(self);
        super::super::run_blocking(move || {
            let result = runtime.disk_counter_body_probes(&context, &pending, &keys);
            context.remember(&result);
            result
        })
        .await
    }

    fn disk_counter_body_probes(
        &self,
        context: &ReadContext,
        pending: &Arc<transaction::PendingBatch>,
        keys: &[(String, String)],
    ) -> StorageResult<Vec<Option<ProbedValue>>> {
        context.validate_owner(Some(pending))?;
        let primaries: Vec<_> = keys
            .iter()
            .map(|(counter, _)| super::super::kv::coordinate("main", counter.as_bytes()))
            .collect();
        let known = self.known_write_counters(context, pending, &primaries)?;
        let mut selected = Vec::new();
        let mut pairs = Vec::new();
        for (position, (_, body)) in keys.iter().enumerate() {
            let primary = primaries[position].clone();
            let fallback = super::super::kv::coordinate("main", body.as_bytes());
            if primary.0 == super::super::physical::CAS_COLLECTION
                || fallback.0 == super::super::physical::CAS_COLLECTION
                || known[position]
                || context
                    .overlay_record(Some(pending), &fallback.0, &fallback.1)
                    .is_some()
            {
                continue;
            }
            selected.push(position);
            pairs.push(ReadProbe { primary, fallback });
        }
        drop(known);
        let mut output: Vec<_> = keys.iter().map(|_| None).collect();
        if pairs.is_empty() {
            return Ok(output);
        }
        let mut call = crate::read_receipts::lock(&context.call);
        context.require_room(&call, 0, 0)?;
        let costs = call.costs();
        let limits = call.budget();
        let bytes = pairs.iter().fold(0u64, |sum, pair| {
            sum.saturating_add(
                (pair.primary.0.len()
                    + pair.primary.1.len()
                    + pair.fallback.0.len()
                    + pair.fallback.1.len()) as u64,
            )
        });
        if costs.examined_keys.saturating_add(pairs.len() as u64 * 2) > limits.keys
            || costs.budget_bytes.saturating_add(bytes) > limits.bytes
        {
            return Ok(output);
        }
        let Some(values) = call
            .main
            .get_many_or_probe(&pairs)
            .map_err(|error| call_error(error, &call))?
        else {
            return Ok(output);
        };
        context.require_room(&call, 0, 0)?;
        // Match the original logical key count: one counter, plus one body
        // only when its counter is absent. Speculative selectors retain no body.
        laststore::record_requested_keys(
            values
                .iter()
                .map(|value| 1 + u64::from(value.value.is_none()))
                .sum(),
        );
        let mut observations = Vec::with_capacity(pairs.len() * 2);
        for (pair, value) in pairs.iter().zip(&values) {
            observations.push((&pair.primary, value.value.as_deref()));
            if value.value.is_none() && !value.fallback_present {
                observations.push((&pair.fallback, None));
            }
        }
        self.remember_write_view(context, &observations)?;
        for (position, value) in selected.into_iter().zip(values) {
            output[position] = Some(value);
        }
        Ok(output)
    }
}
