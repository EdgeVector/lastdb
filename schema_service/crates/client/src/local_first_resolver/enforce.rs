//! Enforce-mode resolve and the live-call helper for the facade.

use super::*;

impl<S, E, L> LocalFirstSchemaResolver<S, E, L>
where
    S: ResolverPackObjectStore + Send + Sync + 'static,
    E: Embedder + Send + Sync + 'static,
    L: LiveSchemaGateway,
{
    // lint:fn-size-ok moved verbatim from its original module
    pub(super) async fn resolve_enforce(
        &self,
        proposals: Vec<FacadeProposal>,
        active: Option<&NativeResolverState>,
    ) -> FoldDbResult<Vec<FacadeResolveItem>> {
        let pack_versions = active.map(|s| {
            (
                s.pack.manifest.format_version,
                s.pack.resolver_config.format_version,
            )
        });

        // Preserve input order: slot is either a finished local item or a
        // pending live fallback (index into `fallback`).
        enum Slot {
            Local {
                proposal_id: String,
                result: Box<SchemaResolveResult>,
                decision: ResolverDecision,
            },
            PendingLive {
                fallback_idx: usize,
            },
        }

        let mut slots: Vec<Slot> = Vec::with_capacity(proposals.len());
        let mut fallback: Vec<FacadeProposal> = Vec::new();
        let mut fallback_reasons: HashMap<String, &'static str> = HashMap::new();

        for p in proposals {
            let Some(state) = active else {
                fallback_reasons.insert(p.proposal_id.clone(), "no_active_pack");
                let idx = fallback.len();
                fallback.push(p);
                slots.push(Slot::PendingLive { fallback_idx: idx });
                continue;
            };
            if let Ok(outcome) = evaluate_local_proposal(
                &state.pack,
                self.embedder.as_ref(),
                &p.proposal_id,
                &p.proposal,
            ) {
                if let Some(result) = outcome.resolve_result {
                    slots.push(Slot::Local {
                        proposal_id: p.proposal_id,
                        result: Box::new(result),
                        decision: outcome.output.decision,
                    });
                } else {
                    fallback_reasons.insert(
                        p.proposal_id.clone(),
                        outcome.fallback_reason.unwrap_or("local_fallback"),
                    );
                    let idx = fallback.len();
                    fallback.push(p);
                    slots.push(Slot::PendingLive { fallback_idx: idx });
                }
            } else {
                fallback_reasons.insert(p.proposal_id.clone(), "local_eval_error");
                let idx = fallback.len();
                fallback.push(p);
                slots.push(Slot::PendingLive { fallback_idx: idx });
            }
        }

        let live_map = if fallback.is_empty() {
            None
        } else {
            Some(self.call_live(&fallback).await?)
        };

        let mut claimed = HashSet::new();
        let mut out = Vec::with_capacity(slots.len());
        for slot in slots {
            match slot {
                Slot::Local {
                    proposal_id,
                    result,
                    decision,
                } => {
                    out.push(FacadeResolveItem {
                        proposal_id,
                        result: *result,
                        path: FacadePath::Local,
                        local_decision: Some(decision),
                        fallback_reason: None,
                        pack_format_version: pack_versions.map(|v| v.0),
                        config_format_version: pack_versions.map(|v| v.1),
                    });
                }
                Slot::PendingLive { fallback_idx } => {
                    let p = &fallback[fallback_idx];
                    let live = live_map
                        .as_ref()
                        .and_then(|m| {
                            match_live_result(
                                m,
                                &p.proposal_id,
                                &p.proposal.descriptive_name,
                                &mut claimed,
                            )
                        })
                        .cloned()
                        .unwrap_or_else(novel_result);
                    out.push(FacadeResolveItem {
                        proposal_id: p.proposal_id.clone(),
                        result: live,
                        path: FacadePath::Live,
                        local_decision: None,
                        fallback_reason: fallback_reasons.get(&p.proposal_id).copied(),
                        pack_format_version: pack_versions.map(|v| v.0),
                        config_format_version: pack_versions.map(|v| v.1),
                    });
                }
            }
        }
        Ok(out)
    }

    pub(super) async fn call_live(
        &self,
        proposals: &[FacadeProposal],
    ) -> FoldDbResult<SchemaResolveResponse> {
        let wire: Vec<SchemaResolveProposal> =
            proposals.iter().map(|p| p.proposal.clone()).collect();
        self.live_resolve_batches.fetch_add(1, Ordering::SeqCst);
        self.live_resolve_proposals
            .fetch_add(wire.len() as u64, Ordering::SeqCst);
        self.live.resolve_schemas(None, wire).await
    }
}
