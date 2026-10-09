//! Logical-commit scopes: wrap one local mutation and stage its capture.

use super::*;

/// Run `future` without recording the mutation log. Used by replay so applying
/// a captured envelope cannot append a second copy of the same write.
pub(crate) async fn with_capture_suppressed<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    LOGICAL_COMMIT_SCOPE
        .scope(
            RefCell::new(LogicalCommitScope {
                suppress_kv: true,
                record_intent: false,
                published: false,
                envelopes: Vec::new(),
            }),
            future,
        )
        .await
}

/// Capture one logical commit with an optional crash-safe or off-box receipt.
///
/// Only the durable-delete owner route selects a non-async policy. The normal
/// path remains the bounded queue handoff above.
pub(crate) async fn capture_logical_commit_with_policy<F, T, E>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    future: F,
) -> Result<(T, Option<CloudMutationReceipt>), E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    capture_logical_commit_with_policy_and_author_clock(router, envelopes, policy, None, future)
        .await
}

/// Capture one logical commit after its author clock reaches durable metadata.
pub(crate) async fn capture_logical_commit_with_policy_and_author_clock<F, T, E>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
    future: F,
) -> Result<(T, Option<CloudMutationReceipt>), E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    // lint:fn-size-ok verbatim move from write_path.rs; splitting this function is separate work
    // Router-wide `kv_suppress_depth` exists only so leftover KvStore puts on
    // a different task (persist/replay run their body on `spawn_blocking`,
    // which does not inherit `LOGICAL_COMMIT_SCOPE`) still see suppress — see
    // `MutationLogCaptureRouter::kv_suppress_depth`. A product write reaches
    // this function on its own request task, so a store-wide guard held for
    // the whole persist/replay/compact/reseal body must not skip it: only the
    // current task's own scope (nested replay on the same task) may.
    if task_local_suppressed() {
        return future.await.map(|value| (value, None));
    }
    // A live sync activation must wait for local-only commits which began
    // before the engine was attached. Later commits wait behind activation
    // and reserve capture against the new engine before resident apply.
    let _activation_guard = match router.as_ref() {
        Some(router) => Some(router.enter_mutation().await),
        None => None,
    };
    let reservation = match router.as_ref() {
        Some(router) if !envelopes.is_empty() => router.reserve_capture().await.map_err(E::from)?,
        _ => None,
    };
    LOGICAL_COMMIT_SCOPE
        .scope(
            RefCell::new(LogicalCommitScope {
                suppress_kv: true,
                record_intent: true,
                published: false,
                envelopes,
            }),
            async move {
                let result = future.await;
                let scope =
                    LOGICAL_COMMIT_SCOPE.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
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
                            if matches!(policy, CloudCapturePolicy::Async) {
                                reservation.submit(CaptureJobKind::LogicalIntent {
                                    envelopes: scope.envelopes,
                                    author_clock_barrier,
                                    record_now: false,
                                    completion: None,
                                });
                            } else {
                                let (completion, completed) = tokio::sync::oneshot::channel();
                                let record_now =
                                    matches!(policy, CloudCapturePolicy::WaitForPublication { .. });
                                reservation.submit(CaptureJobKind::LogicalIntent {
                                    envelopes: scope.envelopes,
                                    author_clock_barrier,
                                    record_now,
                                    completion: Some(completion),
                                });
                                let _ = completed.await;
                            }
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
                        CloudMutationReceipt {
                            mutation_uuid,
                            capture_state: CloudCaptureState::Failed,
                            publication_state: CloudPublicationState::Failed,
                            targets: Vec::new(),
                            error: Some("cloud sync capture is unavailable".to_string()),
                        }
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
                let (completion, completed) = tokio::sync::oneshot::channel();
                let record_now = matches!(policy, CloudCapturePolicy::WaitForPublication { .. });
                reservation.submit(CaptureJobKind::LogicalIntent {
                    envelopes: scope.envelopes,
                    author_clock_barrier,
                    record_now,
                    completion: Some(completion),
                });

                if matches!(policy, CloudCapturePolicy::Durable) {
                    return match completed.await {
                        Ok(LogicalCaptureCompletion::MarkerDurable) => Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Durable,
                                publication_state: CloudPublicationState::NotRequested,
                                targets: Vec::new(),
                                error: None,
                            }),
                        )),
                        Ok(LogicalCaptureCompletion::MarkerFailed(error)) => Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Failed,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(error),
                            }),
                        )),
                        Ok(LogicalCaptureCompletion::AppendFailed(error)) => Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Durable,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(error),
                            }),
                        )),
                        Ok(LogicalCaptureCompletion::Appended(_)) => unreachable!(
                            "durable marker-only capture cannot return an append receipt"
                        ),
                        Err(_) => Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Failed,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(
                                    "cloud capture worker stopped before durable marker receipt"
                                        .to_string(),
                                ),
                            }),
                        )),
                    };
                }

                let CloudCapturePolicy::WaitForPublication { timeout } = policy else {
                    unreachable!("async and durable policies returned above")
                };
                let appended = tokio::time::timeout(timeout, completed).await;
                let append = match appended {
                    Ok(Ok(LogicalCaptureCompletion::Appended(append))) => append,
                    Ok(Ok(LogicalCaptureCompletion::MarkerFailed(error))) => {
                        return Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Failed,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(error),
                            }),
                        ));
                    }
                    Ok(Ok(LogicalCaptureCompletion::AppendFailed(error))) => {
                        return Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Durable,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(error),
                            }),
                        ));
                    }
                    Ok(Ok(LogicalCaptureCompletion::MarkerDurable)) => unreachable!(
                        "exact capture must append after its durable marker"
                    ),
                    Ok(Err(_)) => {
                        return Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Failed,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(
                                    "cloud capture worker stopped before receipt".to_string(),
                                ),
                            }),
                        ));
                    }
                    Err(_) => {
                        return Ok((
                            value,
                            Some(CloudMutationReceipt {
                                mutation_uuid,
                                capture_state: CloudCaptureState::Failed,
                                publication_state: CloudPublicationState::Failed,
                                targets: Vec::new(),
                                error: Some(
                                    "timed out before the durable exact mutation-log target receipt; capture worker continues"
                                        .to_string(),
                                ),
                            }),
                        ));
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
                    return Ok((
                        value,
                        Some(CloudMutationReceipt {
                            mutation_uuid,
                            capture_state: CloudCaptureState::Durable,
                            publication_state: CloudPublicationState::Failed,
                            targets,
                            error: Some(
                                "cloud capture produced no required mutation-log target"
                                    .to_string(),
                            ),
                        }),
                    ));
                }
                let remaining = timeout.saturating_sub(wait_started.elapsed());
                let publication = engine
                    .wait_for_mutation_publication(&append.targets, remaining)
                    .await;
                let (publication_state, error) = match publication {
                    crate::sync::engine::MutationPublicationWait::Published => {
                        (CloudPublicationState::Published, None)
                    }
                    crate::sync::engine::MutationPublicationWait::Pending => {
                        (CloudPublicationState::Pending, None)
                    }
                };
                Ok((
                    value,
                    Some(CloudMutationReceipt {
                        mutation_uuid,
                        capture_state: CloudCaptureState::Durable,
                        publication_state,
                        targets,
                        error,
                    }),
                ))
            },
        )
        .await
}
