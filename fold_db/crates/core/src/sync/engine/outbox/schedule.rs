//! Scheduling of outbox entries into the upload queue.

use super::*;

impl SyncEngine {
    // lint:fn-size-ok verbatim move from outbox.rs; splitting this function is separate work
    pub(crate) async fn schedule_outbox_entries(&self) -> Result<(), String> {
        // In-memory upload queue depth. Prefer the tighter of max_pending and
        // max_upload_entries_per_cycle so a multi-thousand durable outbox cannot
        // deserialize thousands of full LogEntry payloads into RAM before the
        // per-cycle seal/upload caps run (re-enable RSS 4→12 GiB with only
        // 321 KiB selected for upload — the rest sat queued in `pending`).
        //
        // Caps come from the adaptive upload policy (refreshed at do_sync
        // start); static SyncConfig is only a fixed-mode / floor hint.
        let caps = self.active_upload_caps().await;
        let queue_cap = upload_queue_cap(caps.max_pending, caps.max_upload_entries);

        // Seed the outbox metadata (which also advances `self.seq` past any
        // persisted entry) without holding any other lock.
        drop(self.outbox_meta().await?);

        // If the adaptive policy *shrank* since last cycle, drop in-memory
        // tail entries so we don't hold more than this cycle will upload.
        // Durable outbox keeps them for a later cycle.
        if queue_cap > 0 {
            let mut pending = self.pending.lock().await;
            if pending.len() > queue_cap {
                let released = pending.len() - queue_cap;
                // Keep the *front* (oldest / next to upload).
                pending.truncate(queue_cap);
                tracing::info!(
                    target: "fold_db::sync::memory",
                    kept = queue_cap,
                    released,
                    "trimmed in-memory upload queue to adaptive cap"
                );
            }
        }

        // Adaptive cycle budget — selection/queue only. Absolute config/env max
        // is the only threshold that may permanently forget a durable row.
        let select_max_bytes = caps.max_upload_bytes;
        let forget_max_bytes = self.absolute_outbox_max_bytes();
        let mut skipped_oversize = 0usize;
        let mut deferred_select = 0usize;

        // Recompute the adaptive defer barrier from scratch every cycle: this
        // cycle's budget may be larger than the one that set it.
        self.clear_adaptive_deferred_head();

        // Evict / forget rows already sitting in the in-memory queue.
        // - size > absolute max → durable forget (true poison; can never upload)
        // - absolute >= size > adaptive select → memory-only defer (keep durable)
        //   *and* nothing at or above that seq may stay queued, or a later small
        //   row would upload past a still-durable older row.
        {
            let mut oversize_seqs: Vec<(u64, usize)> = {
                let pending = self.pending.lock().await;
                pending
                    .iter()
                    .map(|entry| (entry.seq, entry.serialized_len()))
                    .collect()
            };
            oversize_seqs.sort_unstable_by_key(|(seq, _)| *seq);
            let mut defer_head: Option<u64> = None;
            for (seq, size) in oversize_seqs {
                if forget_max_bytes > 0 && size > forget_max_bytes {
                    skipped_oversize += 1;
                    self.drop_oversize_outbox_entry(
                        seq,
                        size,
                        forget_max_bytes,
                        "upload_queue",
                        "dropping oversize in-memory upload-queue entry so catch-up can progress",
                    )
                    .await?;
                } else if select_max_bytes > 0 && size > select_max_bytes {
                    deferred_select += 1;
                    tracing::info!(
                        target: "fold_db::sync::memory",
                        seq,
                        size,
                        select_max_bytes,
                        forget_max_bytes,
                        "deferring in-memory upload-queue entry over adaptive budget; durable outbox retained"
                    );
                    defer_head = Some(defer_head.map_or(seq, |head: u64| head.min(seq)));
                }
            }
            if let Some(head) = defer_head {
                self.defer_from_seq_for_cycle(head).await;
            }
        }

        // Only the oldest `queue_cap` entries can ever occupy the bounded
        // upload queue — we always drain the queue from the front, so the next
        // entries to schedule are always the lowest unscheduled seqs. Read just
        // that front window instead of deserializing the whole outbox. When
        // `queue_cap == 0` the upload queue is unbounded, so fall back to the
        // full set.
        // Fill the upload queue one durable row at a time (keyset walk) so a
        // multi-MB poison BatchPut cannot force us to decrypt N huge values just
        // to learn they are oversize (re-enable thrash: window_len=16 → 15 GiB).
        // Absolute-oversize rows are forgotten using **raw** length before JSON
        // deserialize when possible. Adaptive-oversize rows are deferred (left
        // durable) and block further FIFO fill this cycle so history order holds.
        let mut window_steps = 0usize;
        let mut pending_seqs: HashSet<u64> = {
            let pending = self.pending.lock().await;
            pending.iter().map(|entry| entry.seq).collect()
        };
        // Always walk from the oldest durable head — never from max(pending),
        // which is often a *new* live write and would skip the entire backlog
        // (window_steps=0 / pending_len=1 on re-enable).
        let mut after_seq: Option<u64> = None;

        let target_len = if queue_cap == 0 {
            usize::MAX
        } else {
            queue_cap
        };
        let max_steps = target_len.saturating_mul(32).max(32);
        while window_steps < max_steps {
            {
                let pending = self.pending.lock().await;
                if pending.len() >= target_len {
                    break;
                }
            }
            // Pass absolute forget max into the row reader so we still deserialize
            // rows that fit absolute but exceed adaptive select budget.
            let Some((seq, raw_len, entry_opt)) =
                self.outbox_row_after(after_seq, forget_max_bytes).await?
            else {
                break;
            };
            window_steps += 1;
            after_seq = Some(seq);
            if pending_seqs.contains(&seq) {
                continue;
            }
            let Some(entry) = entry_opt else {
                // Raw row exceeded absolute forget_max — poison; forget without deserialize.
                skipped_oversize += 1;
                self.drop_oversize_outbox_entry(
                    seq,
                    raw_len,
                    forget_max_bytes,
                    "durable_outbox_raw",
                    "dropping oversize durable outbox entry (raw size; no deserialize) so catch-up can progress",
                )
                .await?;
                continue;
            };
            // Defense in depth: serialized form can exceed raw after encode quirks.
            let size = entry.serialized_len().max(raw_len);
            if forget_max_bytes > 0 && size > forget_max_bytes {
                skipped_oversize += 1;
                drop(entry);
                self.drop_oversize_outbox_entry(
                    seq,
                    size,
                    forget_max_bytes,
                    "durable_outbox_decoded",
                    "dropping oversize durable outbox entry so catch-up can progress",
                )
                .await?;
                continue;
            }
            if select_max_bytes > 0 && size > select_max_bytes {
                // Fits absolute max (admitted legitimately) but not this cycle's
                // adaptive budget. Keep durable; stop FIFO fill so we do not
                // reorder cloud history past the deferred head. `record_op` may
                // already have admitted a later small seq before this row was
                // deferred, so evict that tail too and hold the barrier until
                // this row uploads or is absolutely forgotten.
                deferred_select += 1;
                tracing::info!(
                    target: "fold_db::sync::memory",
                    seq,
                    size,
                    select_max_bytes,
                    forget_max_bytes,
                    "deferring durable outbox entry over adaptive budget; leaving for a larger-budget cycle"
                );
                self.defer_from_seq_for_cycle(seq).await;
                pending_seqs.retain(|queued| *queued < seq);
                break;
            }
            let mut pending = self.pending.lock().await;
            if pending_seqs.insert(entry.seq) {
                pending.push(entry);
            }
        }
        let pending_len = self.pending.lock().await.len();
        tracing::info!(
            target: "fold_db::sync::memory",
            queue_cap,
            window_steps,
            skipped_oversize,
            deferred_select,
            select_max_bytes,
            forget_max_bytes,
            pending_len,
            "schedule_outbox_entries refilled upload queue"
        );
        Ok(())
    }
}
