//! Field-match probe (eval/debug).

use serde_json::Value;

use super::*;

/// Eval/debug: score left field texts against a right catalog of field texts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeItem {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeRequest {
    pub left: Vec<FieldMatchProbeItem>,
    pub right: Vec<FieldMatchProbeItem>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize {
    24
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeHit {
    pub id: String,
    pub score: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeResponse {
    pub matches: HashMap<String, Vec<FieldMatchProbeHit>>,
}

/// The field-match probe result both HTTP routers answer.
#[derive(Debug)]
pub enum FieldMatchProbeReply {
    /// The probe scored the texts. HTTP 200.
    Ready(FieldMatchProbeResponse),
    /// The probe refused the request. HTTP 400.
    ///
    /// `body` is `{ "error": "Field match probe failed: {detail}" }`.
    Refused { body: Value },
}

impl SchemaServiceState {
    /// Cosine similarity of embedded texts — used by component_cover.mjs embedding-beam.
    pub fn field_match_probe(
        &self,
        request: &FieldMatchProbeRequest,
    ) -> Result<FieldMatchProbeResponse, String> {
        let top_k = request.top_k.clamp(1, 128);
        let mut right_emb = Vec::with_capacity(request.right.len());
        for item in &request.right {
            let v = self
                .embedder
                .embed_text(&item.text)
                .map_err(|e| format!("embed right {}: {e}", item.id))?;
            right_emb.push((item.id.clone(), v));
        }

        let mut matches = HashMap::new();
        for left in &request.left {
            let lv = self
                .embedder
                .embed_text(&left.text)
                .map_err(|e| format!("embed left {}: {e}", left.id))?;
            let mut hits: Vec<FieldMatchProbeHit> = right_emb
                .iter()
                .map(|(id, rv)| FieldMatchProbeHit {
                    id: id.clone(),
                    score: crate::embedder::cosine_similarity(&lv, rv),
                })
                .collect();
            hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            hits.truncate(top_k);
            matches.insert(left.id.clone(), hits);
        }
        Ok(FieldMatchProbeResponse { matches })
    }

    /// The same call as [`Self::field_match_probe`], classified for HTTP.
    ///
    /// A refusal stays HTTP 400. The body carries the prefix. Each router
    /// still writes its response. [`Self::field_match_probe`] stays public.
    #[must_use]
    pub fn field_match_probe_reply(
        &self,
        request: &FieldMatchProbeRequest,
    ) -> FieldMatchProbeReply {
        match self.field_match_probe(request) {
            Ok(response) => FieldMatchProbeReply::Ready(response),
            Err(detail) => FieldMatchProbeReply::Refused {
                body: crate::error_only_body(&format!("Field match probe failed: {detail}")),
            },
        }
    }
}
