use super::*;

impl SchemaServiceState {
    /// Whether every field the proposal shares BY NAME with `candidate` is
    /// described the same way: identical text (case-insensitive), or an
    /// embedding cosine at [`PURPOSE_SIMILARITY_THRESHOLD`]. A description on
    /// one side only, or an embedder failure, is a disagreement. Renamed
    /// correspondences are not re-checked: `directional_field_coverage`
    /// already matched them on their descriptions.
    pub(super) fn shared_field_descriptions_agree(
        &self,
        incoming: &Schema,
        candidate: &BestReuseCandidate,
    ) -> bool {
        let Some(fields) = incoming.fields.as_ref() else {
            return true;
        };
        fields
            .iter()
            .filter(|field| candidate.fields.contains(field))
            .all(|field| {
                let ours = incoming.field_descriptions.get(field).map(|d| d.trim());
                let theirs = candidate.field_descriptions.get(field).map(|d| d.trim());
                match (ours, theirs) {
                    (None, None) => true,
                    (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => true,
                    (Some(a), Some(b)) => {
                        let (Ok(va), Ok(vb)) =
                            (self.embedder.embed_text(a), self.embedder.embed_text(b))
                        else {
                            return false;
                        };
                        cosine_similarity(&va, &vb) >= PURPOSE_SIMILARITY_THRESHOLD
                    }
                    _ => false,
                }
            })
    }

    /// Field-fidelity measure the reuse-before-NEW path floors on (see
    /// [`REUSE_FIELD_FIDELITY_FLOOR`]): the **stronger of the two directional
    /// coverages** between the proposal and the reuse `candidate`, where a
    /// directional coverage is the fraction of one side's fields that genuinely
    /// correspond to a field on the other side (literal match OR a confident,
    /// mutual semantic rename via [`Self::semantic_field_rename_map`]).
    ///
    /// **Why `max` of both directions, not just proposal→candidate.** The guard
    /// exists to reject *over-generalization* — a purpose-coincidental pair whose
    /// field shapes genuinely diverge. But a one-directional (proposal→candidate)
    /// measure also rejects a legitimate case: the same concept caught at two
    /// different *richnesses*, where the proposal is a strict superset of a
    /// **minimal** earlier canonical. Concretely (the `contact` dup-explosion this
    /// fixes): a minimal "Contacts (Hash)" canonical of `{name, email}` registered
    /// first, then a richer "Contact Records" proposal of
    /// `{full_name, email_address, phone_number, company}` arrives. Only
    /// `full_name→name` (and sometimes `email_address→email`) map, so the
    /// proposal→candidate coverage is 0.25–0.5 — order-sensitively flipping below
    /// the floor and dup-exploding the concept, *even though every `{name, email}`
    /// of the candidate is fully present in the proposal*. The reverse direction
    /// (candidate→proposal) is 1.0 there, which is the honest signal: the proposal
    /// genuinely *is* an instance of the candidate concept (it carries all of it)
    /// and merely evolves it with extra fields — exactly what the expand path is
    /// for. Taking the max admits this superset/refinement case while a true
    /// over-generalization (fields divergent in BOTH directions, e.g. candidate
    /// `{amount}` vs proposal `{camera_model, dimensions}`) still scores 0 both
    /// ways and is correctly rejected.
    ///
    /// Returns `1.0` when the proposal has no fields (nothing to be unfaithful
    /// about — let the purpose/name gates decide). `semantic_field_rename_map` is
    /// reused verbatim so the correspondence notion is identical to what the
    /// expansion path will apply, and its bidirectional check keeps a single
    /// field from absorbing several unrelated ones.
    pub(super) fn reuse_field_coverage(
        &self,
        incoming: &Schema,
        candidate: &BestReuseCandidate,
    ) -> f32 {
        let incoming_fields = match incoming.fields.as_ref() {
            Some(f) if !f.is_empty() => f,
            _ => return 1.0,
        };
        // A candidate with no fields can't cover or be covered — fall back to the
        // proposal→candidate direction alone (which will be 0, rejecting reuse).
        if candidate.fields.is_empty() {
            return 0.0;
        }

        // Forward: fraction of the PROPOSAL's fields that land in the candidate
        // (the original measure — catches the subset/surface-name-variant case).
        let forward = self.directional_field_coverage(
            incoming_fields,
            &incoming.field_descriptions,
            &candidate.fields,
            &candidate.field_descriptions,
            &candidate.desc,
        );
        // Reverse: fraction of the CANDIDATE's fields that land in the proposal
        // (catches the superset/richer-evolution case — see the doc comment).
        let reverse = self.directional_field_coverage(
            &candidate.fields,
            &candidate.field_descriptions,
            incoming_fields,
            &incoming.field_descriptions,
            &candidate.desc,
        );
        forward.max(reverse)
    }

    /// Fraction of `from_fields` that correspond to a field in `to_fields` — by
    /// literal (exact) name match OR a confident, mutual semantic rename. Both
    /// embeddings are computed under `context_name` so the field-correspondence
    /// notion is identical to the expansion path's. Returns `0.0` for an empty
    /// `from_fields` (no caller passes one, but keep it total).
    pub(super) fn directional_field_coverage(
        &self,
        from_fields: &[String],
        from_descriptions: &HashMap<String, String>,
        to_fields: &[String],
        to_descriptions: &HashMap<String, String>,
        context_name: &str,
    ) -> f32 {
        if from_fields.is_empty() {
            return 0.0;
        }
        let to_set: HashSet<&String> = to_fields.iter().collect();
        // Literal matches first (cheap, no embedding).
        let literal = from_fields.iter().filter(|f| to_set.contains(*f)).count();
        // Semantic renames for the rest (from_field → to_field).
        let rename_map = self.semantic_field_rename_map(
            from_fields,
            to_fields,
            context_name,
            from_descriptions,
            to_descriptions,
        );
        let mut mapped = literal + rename_map.len();

        let literal_from: HashSet<&String> =
            from_fields.iter().filter(|f| to_set.contains(*f)).collect();
        let mut claimed_to: HashSet<String> = rename_map.values().cloned().collect();
        for f in from_fields {
            if literal_from.contains(f) || rename_map.contains_key(f) {
                continue;
            }
            if let Some(to) = to_fields.iter().find(|to| {
                !claimed_to.contains(*to) && lexical_field_corresponds(f.as_str(), to.as_str())
            }) {
                claimed_to.insert(to.clone());
                mapped += 1;
            }
        }
        mapped as f32 / from_fields.len() as f32
    }

    /// Get or compute the embedding for a field, combining name-in-context with description.
    ///
    /// Embeds "the {field_name} of the {descriptive_name}: {description}" when a description
    /// is available. The name-in-context prefix provides structural signal (fields with the
    /// same role in the same domain cluster together), while the description adds semantic
    /// specificity that prevents false positives (e.g. "subject" won't match "calendar"
    /// because their descriptions are unrelated).
    pub(crate) fn get_field_embedding(
        &self,
        field_name: &str,
        descriptive_name: &str,
        field_description: Option<&str>,
    ) -> Option<Vec<f32>> {
        let context_text = match field_description {
            Some(desc) => format!("the {field_name} of the {descriptive_name}: {desc}"),
            None => format!("the {field_name} of the {descriptive_name}"),
        };
        // Cache key includes a hash of the description to correctly distinguish
        // entries with different descriptions for the same field name.
        let desc_hash = match field_description {
            Some(desc) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                desc.hash(&mut hasher);
                hasher.finish()
            }
            None => 0,
        };
        let cache_key = format!("{descriptive_name}:{field_name}:{desc_hash}");

        // Check cache first
        if let Ok(cache) = self.field_embeddings.read() {
            if let Some(vec) = cache.get(&cache_key) {
                return Some(vec.clone());
            }
        }

        match self.embedder.embed_text(&context_text) {
            Ok(vec) => {
                if let Ok(mut cache) = self.field_embeddings.write() {
                    cache.insert(cache_key, vec.clone());
                }
                Some(vec)
            }
            Err(e) => {
                tracing::warn!(
                target: "schema_service::schema",
                        "Failed to embed field '{}' with context '{}': {}",
                        field_name,
                        descriptive_name,
                        e
                    );
                None
            }
        }
    }
}
