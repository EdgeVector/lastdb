//! Enforce-mode resolve and the live-call helper for the facade.

use super::*;

impl<S, E, L> LocalFirstSchemaResolver<S, E, L>
where
    S: ResolverPackObjectStore + Send + Sync + 'static,
    E: Embedder + Send + Sync + 'static,
    L: LiveSchemaGateway,
{
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

        let plan = self.plan_enforce(proposals, active);
        let live_map = if plan.fallback.is_empty() {
            None
        } else {
            Some(self.call_live(&plan.fallback).await?)
        };
        Ok(assemble_enforce_items(
            plan,
            live_map.as_ref(),
            pack_versions,
        ))
    }

    /// Evaluate each proposal locally; queue the ones that need a live call.
    fn plan_enforce(
        &self,
        proposals: Vec<FacadeProposal>,
        active: Option<&NativeResolverState>,
    ) -> EnforcePlan {
        let mut plan = EnforcePlan {
            slots: Vec::with_capacity(proposals.len()),
            fallback: Vec::new(),
            fallback_reasons: HashMap::new(),
        };
        for p in proposals {
            let Some(state) = active else {
                plan.push_fallback(p, "no_active_pack");
                continue;
            };
            let Ok(outcome) = evaluate_local_proposal(
                &state.pack,
                self.embedder.as_ref(),
                &p.proposal_id,
                &p.proposal,
            ) else {
                plan.push_fallback(p, "local_eval_error");
                continue;
            };
            match outcome.resolve_result {
                Some(result) => plan.slots.push(EnforceSlot::Local {
                    proposal_id: p.proposal_id,
                    result: Box::new(result),
                    decision: outcome.output.decision,
                }),
                None => plan.push_fallback(p, outcome.fallback_reason.unwrap_or("local_fallback")),
            }
        }
        plan
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

/// Preserve input order: a slot is either a finished local item or a
/// pending live fallback (index into `EnforcePlan::fallback`).
enum EnforceSlot {
    Local {
        proposal_id: String,
        result: Box<SchemaResolveResult>,
        decision: ResolverDecision,
    },
    PendingLive {
        fallback_idx: usize,
    },
}

struct EnforcePlan {
    slots: Vec<EnforceSlot>,
    fallback: Vec<FacadeProposal>,
    fallback_reasons: HashMap<String, &'static str>,
}

impl EnforcePlan {
    fn push_fallback(&mut self, p: FacadeProposal, reason: &'static str) {
        self.fallback_reasons.insert(p.proposal_id.clone(), reason);
        let idx = self.fallback.len();
        self.fallback.push(p);
        self.slots
            .push(EnforceSlot::PendingLive { fallback_idx: idx });
    }
}

fn assemble_enforce_items(
    plan: EnforcePlan,
    live_map: Option<&SchemaResolveResponse>,
    pack_versions: Option<(u32, u32)>,
) -> Vec<FacadeResolveItem> {
    let EnforcePlan {
        slots,
        fallback,
        fallback_reasons,
    } = plan;
    let mut claimed = HashSet::new();
    let mut out = Vec::with_capacity(slots.len());
    for slot in slots {
        match slot {
            EnforceSlot::Local {
                proposal_id,
                result,
                decision,
            } => out.push(FacadeResolveItem {
                proposal_id,
                result: *result,
                path: FacadePath::Local,
                local_decision: Some(decision),
                fallback_reason: None,
                pack_format_version: pack_versions.map(|v| v.0),
                config_format_version: pack_versions.map(|v| v.1),
            }),
            EnforceSlot::PendingLive { fallback_idx } => {
                let p = &fallback[fallback_idx];
                let live = live_map
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
    out
}
