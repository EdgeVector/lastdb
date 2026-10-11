//! Logical-commit completion: stage the capture job and build the cloud receipt.

use super::*;

type CommitResult<T, E> = Result<(T, Option<CloudMutationReceipt>), E>;
type Completed = tokio::sync::oneshot::Receiver<LogicalCaptureCompletion>;

fn receipt(
    mutation_uuid: String,
    capture_state: CloudCaptureState,
    publication_state: CloudPublicationState,
    targets: Vec<CloudPublicationTarget>,
    error: Option<String>,
) -> CloudMutationReceipt {
    CloudMutationReceipt {
        mutation_uuid,
        capture_state,
        publication_state,
        targets,
        error,
    }
}

fn failed_receipt(mutation_uuid: String, error: String) -> CloudMutationReceipt {
    receipt(
        mutation_uuid,
        CloudCaptureState::Failed,
        CloudPublicationState::Failed,
        Vec::new(),
        Some(error),
    )
}

/// Receipt for a marker that is durable but whose exact append failed.
fn append_failed_receipt(mutation_uuid: String, error: String) -> CloudMutationReceipt {
    receipt(
        mutation_uuid,
        CloudCaptureState::Durable,
        CloudPublicationState::Failed,
        Vec::new(),
        Some(error),
    )
}

/// Submit a logical-intent job that reports back through a completion channel.
fn submit_with_completion(
    reservation: CaptureReservation,
    envelopes: Vec<MutationEnvelope>,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
    policy: CloudCapturePolicy,
) -> Completed {
    let (completion, completed) = tokio::sync::oneshot::channel();
    let record_now = matches!(policy, CloudCapturePolicy::WaitForPublication { .. });
    reservation.submit(CaptureJobKind::LogicalIntent {
        envelopes,
        author_clock_barrier,
        record_now,
        completion: Some(completion),
    });
    completed
}

/// Run the commit future inside its scope, then stage capture and build the receipt.
pub(super) async fn finish_logical_commit<F, T, E>(
    future: F,
    reservation: Option<CaptureReservation>,
    policy: CloudCapturePolicy,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
) -> CommitResult<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let result = future.await;
    let scope = LOGICAL_COMMIT_SCOPE.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
    if !scope.record_intent {
        return result.map(|value| (value, None));
    }
    let value = match result {
        Ok(value) => value,
        Err(error) if !scope.published => return Err(error),
        Err(error) => {
            // Tips are already visible. Stage capture, then
            // return the persist/flush/erase Err to the caller.
            if let Some(reservation) = reservation {
                stage_after_error(reservation, scope.envelopes, policy, author_clock_barrier).await;
            }
            return Err(error);
        }
    };
    let mutation_uuid = scope
        .envelopes
        .iter()
        .find_map(|envelope| {
            (!envelope.mutation_uuid.is_empty()).then(|| envelope.mutation_uuid.clone())
        })
        .unwrap_or_default();
    let Some(reservation) = reservation else {
        let cloud = (!matches!(policy, CloudCapturePolicy::Async)).then(|| {
            failed_receipt(
                mutation_uuid,
                "cloud sync capture is unavailable".to_string(),
            )
        });
        return Ok((value, cloud));
    };
    if matches!(policy, CloudCapturePolicy::Async) {
        reservation.submit(CaptureJobKind::LogicalIntent {
            envelopes: scope.envelopes,
            author_clock_barrier,
            record_now: false,
            completion: None,
        });
        return Ok((value, None));
    }

    let engine = Arc::clone(&reservation.engine);
    let wait_started = std::time::Instant::now();
    let completed =
        submit_with_completion(reservation, scope.envelopes, author_clock_barrier, policy);

    if matches!(policy, CloudCapturePolicy::Durable) {
        let cloud = durable_receipt(mutation_uuid, completed.await);
        return Ok((value, Some(cloud)));
    }

    let CloudCapturePolicy::WaitForPublication { timeout } = policy else {
        unreachable!("async and durable policies returned above")
    };
    let cloud = publication_receipt(&engine, mutation_uuid, completed, timeout, wait_started).await;
    Ok((value, Some(cloud)))
}

async fn stage_after_error(
    reservation: CaptureReservation,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
) {
    if matches!(policy, CloudCapturePolicy::Async) {
        reservation.submit(CaptureJobKind::LogicalIntent {
            envelopes,
            author_clock_barrier,
            record_now: false,
            completion: None,
        });
    } else {
        let completed =
            submit_with_completion(reservation, envelopes, author_clock_barrier, policy);
        let _ = completed.await;
    }
}

fn durable_receipt(
    mutation_uuid: String,
    completed: Result<LogicalCaptureCompletion, tokio::sync::oneshot::error::RecvError>,
) -> CloudMutationReceipt {
    match completed {
        Ok(LogicalCaptureCompletion::MarkerDurable) => receipt(
            mutation_uuid,
            CloudCaptureState::Durable,
            CloudPublicationState::NotRequested,
            Vec::new(),
            None,
        ),
        Ok(LogicalCaptureCompletion::MarkerFailed(error)) => failed_receipt(mutation_uuid, error),
        Ok(LogicalCaptureCompletion::AppendFailed(error)) => {
            append_failed_receipt(mutation_uuid, error)
        }
        Ok(LogicalCaptureCompletion::Appended(_)) => {
            unreachable!("durable marker-only capture cannot return an append receipt")
        }
        Err(_) => failed_receipt(
            mutation_uuid,
            "cloud capture worker stopped before durable marker receipt".to_string(),
        ),
    }
}

async fn publication_receipt(
    engine: &Arc<SyncEngine>,
    mutation_uuid: String,
    completed: Completed,
    timeout: std::time::Duration,
    wait_started: std::time::Instant,
) -> CloudMutationReceipt {
    let appended = tokio::time::timeout(timeout, completed).await;
    let append = match appended {
        Ok(Ok(LogicalCaptureCompletion::Appended(append))) => append,
        Ok(Ok(LogicalCaptureCompletion::MarkerFailed(error))) => {
            return failed_receipt(mutation_uuid, error);
        }
        Ok(Ok(LogicalCaptureCompletion::AppendFailed(error))) => {
            return append_failed_receipt(mutation_uuid, error);
        }
        Ok(Ok(LogicalCaptureCompletion::MarkerDurable)) => {
            unreachable!("exact capture must append after its durable marker")
        }
        Ok(Err(_)) => {
            return failed_receipt(
                mutation_uuid,
                "cloud capture worker stopped before receipt".to_string(),
            );
        }
        Err(_) => {
            return failed_receipt(
                mutation_uuid,
                "timed out before the durable exact mutation-log target receipt; capture worker continues"
                    .to_string(),
            );
        }
    };
    let targets = append
        .targets
        .iter()
        .map(|target| CloudPublicationTarget {
            target_id: target.target_id.clone(),
            target_label: target.target_label.clone(),
            writer_id: target.writer_id.clone(),
            frontier: target.frontier,
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return receipt(
            mutation_uuid,
            CloudCaptureState::Durable,
            CloudPublicationState::Failed,
            targets,
            Some("cloud capture produced no required mutation-log target".to_string()),
        );
    }
    let remaining = timeout.saturating_sub(wait_started.elapsed());
    let publication = engine
        .wait_for_mutation_publication(&append.targets, remaining)
        .await;
    let publication_state = match publication {
        crate::sync::engine::MutationPublicationWait::Published => CloudPublicationState::Published,
        crate::sync::engine::MutationPublicationWait::Pending => CloudPublicationState::Pending,
    };
    receipt(
        mutation_uuid,
        CloudCaptureState::Durable,
        publication_state,
        targets,
        None,
    )
}
