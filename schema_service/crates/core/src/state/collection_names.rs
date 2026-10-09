use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

/// Lowercased name prefixes that are dead giveaways of AI-generated captions.
/// Used by `is_valid_collection_name`, `heuristic_collection_name_check`, and
/// `is_caption_name` so all three agree on the complete set.
const CAPTION_PREFIXES: &[&str] = &[
    "this is ",
    "the image ",
    "this image ",
    "- **",
    "- this",
    "a close-up ",
    "a photo ",
    "an image ",
];

/// Reference collection names used as anchor points in embedding space.
/// Real collection names cluster near these; AI captions are far from all of them.
const COLLECTION_NAME_ANCHORS: &[&str] = &[
    "Photo Collection",
    "Recipe Collection",
    "Medical Records",
    "Journal Entries",
    "Financial Transactions",
    "Product Catalog",
    "User Profiles",
    "Event Schedule",
    "Document Collection",
    "Contact Directory",
    "Task List",
    "Insurance Records",
    "Tax Documents",
    "Course Materials",
    "Meeting Notes",
    "Travel Itinerary",
    "Order History",
    "Sales Records",
    "Music Library",
    "Email Archive",
    "Inventory List",
    "Workout Log",
    "Customer Database",
    "Blog Posts",
];

/// Minimum cosine similarity to any anchor for a descriptive_name to be accepted.
const COLLECTION_NAME_SIMILARITY_THRESHOLD: f32 = 0.3;

impl SchemaServiceState {
    /// Compute embeddings for the reference collection name anchors.
    /// Returns an empty vec if the embedding model is unavailable.
    pub(super) fn compute_anchor_embeddings(embedder: &dyn Embedder) -> Vec<Vec<f32>> {
        let mut anchors = Vec::with_capacity(COLLECTION_NAME_ANCHORS.len());
        for name in COLLECTION_NAME_ANCHORS {
            match embedder.embed_text(name) {
                Ok(vec) => anchors.push(vec),
                Err(e) => {
                    tracing::warn!(
                    target: "schema_service::schema",
                                "Failed to embed anchor '{}': {} — falling back to heuristic validation",
                                name,
                                e
                            );
                    return Vec::new();
                }
            }
        }
        anchors
    }

    /// Check whether a descriptive_name looks like a proper collection name
    /// rather than an AI-generated caption or description.
    ///
    /// Applies fast heuristic pre-filters (word count, sentence patterns) first,
    /// then uses embedding similarity against reference collection names when the
    /// embedding model is available.
    pub fn is_valid_collection_name(&self, name: &str) -> bool {
        // Fast pre-filter: names with more than 8 words are almost certainly captions
        let word_count = name.split_whitespace().count();
        if word_count > 8 {
            return false;
        }

        // Fast pre-filter: reject names that start with common sentence/caption patterns.
        // These are dead giveaways of AI-generated descriptions regardless of embedding similarity.
        let dn_lower = name.to_lowercase();
        if CAPTION_PREFIXES.iter().any(|p| dn_lower.starts_with(p)) {
            return false;
        }

        // If anchors are empty (embedding model unavailable), use heuristic fallback
        if self.collection_name_anchors.is_empty() {
            return Self::heuristic_collection_name_check(name);
        }

        // Compute embedding for the candidate name
        let Ok(candidate_embedding) = self.embedder.embed_text(name) else {
            return Self::heuristic_collection_name_check(name);
        };

        // Find max cosine similarity to any anchor
        let max_sim = self
            .collection_name_anchors
            .iter()
            .map(|anchor| cosine_similarity(&candidate_embedding, anchor))
            .fold(f32::NEG_INFINITY, f32::max);

        max_sim >= COLLECTION_NAME_SIMILARITY_THRESHOLD
    }

    /// Heuristic fallback when embedding model is unavailable.
    /// Rejects names that match any caption prefix or exceed 80 characters.
    pub(super) fn heuristic_collection_name_check(name: &str) -> bool {
        let dn_lower = name.to_lowercase();
        let is_caption =
            CAPTION_PREFIXES.iter().any(|p| dn_lower.starts_with(p)) || name.len() > 80;
        !is_caption
    }

    /// Detect AI-generated captions/descriptions masquerading as names.
    ///
    /// Returns `true` for sentence-like names (> 8 words, or starting with
    /// common caption patterns like "This is", "A photo of", etc.).
    pub(super) fn is_caption_name(name: &str) -> bool {
        let word_count = name.split_whitespace().count();
        if word_count > 8 {
            return true;
        }
        let lower = name.to_lowercase();
        CAPTION_PREFIXES.iter().any(|p| lower.starts_with(p))
    }

    /// Compute an improved descriptive name for a user-source schema whose
    /// current `descriptive_name` is a caption or generic structural name.
    ///
    /// Returns `Some(new_name)` only if the correction is strictly different
    /// from `current`. Returns `None` when the fallback chain (title-cased
    /// schema name → field inference → schema name as last resort) produces
    /// the same string we started with — e.g. `descriptive_name = "Text
    /// Documents"` with `schema.name = "text_documents"` and no field pattern
    /// match. Callers must reject user schemas in that case rather than
    /// persisting a generic name.
    ///
    /// Also returns `None` when a candidate correction would land on a
    /// `descriptive_name` that is already bound to an active schema of a
    /// **different** `schema_type`. Without this guard the smart-folder
    /// ingest pipeline silently drops the file: the auto-correction
    /// manufactures a name collision the schema service then refuses with
    /// `DescriptiveNameConflict` (cross-schema_type expansion would corrupt
    /// molecule reads — see `state_expansion::is_cross_schema_type_expansion`).
    /// Returning `None` lets the caller fail fast so ingestion can re-prompt
    /// instead of manufacturing a cross-type collision or persisting the
    /// generic original.
    pub(super) fn improve_descriptive_name(
        &self,
        schema: &Schema,
        current: &str,
    ) -> Option<String> {
        let title_cased = Self::snake_to_title_case(&schema.name);
        let title_cased_is_bad = crate::name_validator::is_generic_name(&title_cased)
            || crate::name_validator::is_over_specific_name(&title_cased);
        let corrected = if title_cased_is_bad {
            // schema.name is also generic / over-specific (e.g. "content_articles" or
            // "roasted_tomato_soup_recipe") — infer from fields.
            let inferred = self.generate_collection_name(schema);
            if crate::name_validator::is_generic_name(&inferred)
                || crate::name_validator::is_over_specific_name(&inferred)
            {
                // Last resort: use title-cased schema.name as-is (better than a
                // circular generic; the WARN log records the mismatch so
                // operators can see ingest-time noise).
                title_cased
            } else {
                inferred
            }
        } else {
            title_cased
        };

        if self.descriptive_name_conflicts_cross_type(&corrected, schema) {
            tracing::warn!(
                target: "schema_service::schema",
                rejected_candidate = %corrected,
                current = %current,
                incoming_schema_type = ?schema.schema_type,
                "Auto-correction candidate collides with existing descriptive_name of different schema_type — rejecting fallback",
            );
            return None;
        }

        if corrected == current {
            None
        } else {
            Some(corrected)
        }
    }

    /// Best-effort check: does `candidate` already resolve (via the
    /// descriptive_name index) to an active schema whose `schema_type`
    /// differs from `incoming.schema_type`?
    ///
    /// Used by [`Self::improve_descriptive_name`] to refuse auto-corrections
    /// that would manufacture a cross-schema_type collision. Returns `false`
    /// on any lookup failure (poisoned lock, missing entry, superseded
    /// schema) — the caller treats `false` as "no collision detected", so we
    /// fall back to the original behaviour rather than blocking the
    /// correction on a transient read failure.
    pub(super) fn descriptive_name_conflicts_cross_type(
        &self,
        candidate: &str,
        incoming: &Schema,
    ) -> bool {
        // Namespace-aware lookup. The doc-comment promises this scopes to
        // the incoming schema's `owner_app_id`, but `descriptive_name_key`
        // is not injective on `(Option<&str>, &str)` — both
        // `(None, "kanban/Tasks")` and `(Some("kanban"), "Tasks")` encode
        // to the index key `"kanban/Tasks"` (see PR #466). A raw
        // `index.get` therefore could surface a legacy un-owned schema
        // whose `descriptive_name` aliases the incoming namespace's key,
        // manufacture a spurious cross-`schema_type` "collision", and
        // refuse a safe auto-correction. Routing through
        // `lookup_descriptive_name_in_namespace` re-resolves the resolved
        // schema's `owner_app_id` and discards mismatched-owner hits,
        // restoring the namespace invariant the dedup index is meant to
        // enforce.
        let Ok(Some(existing_hash)) =
            self.lookup_descriptive_name_in_namespace(incoming.owner_app_id.as_deref(), candidate)
        else {
            return false;
        };

        let Ok(schemas) = read_lock(&self.schemas, "schemas") else {
            return false;
        };
        let Some(existing) = schemas
            .get(&existing_hash)
            .filter(|s| s.superseded_by.is_none())
        else {
            return false;
        };
        existing.schema_type != incoming.schema_type
    }

    /// If a User proposal's `descriptive_name` exactly collides — in the
    /// incoming schema's `owner_app_id` namespace — with an active **starter
    /// seed** (`SchemaSource::StarterSeed`/`SystemSeed`) of an *incompatible*
    /// `schema_type`, derive a unique, non-colliding `descriptive_name` so the
    /// proposal can register as its own canonical instead of hard-failing.
    ///
    /// Why this is scoped to seeds (not User-vs-User): a starter seed is a
    /// suggestion the node may fork/override (`SchemaSource` doc), and the
    /// ingestion LLM routinely proposes a `Hash`/`Single` shape for a name a
    /// persona seed pre-claimed as `Range` (e.g. "Contacts"). Cross-`schema_type`
    /// expansion would corrupt molecule reads (see
    /// [`crate::state_expansion::is_cross_schema_type_expansion`]), so the
    /// proposal genuinely needs a *separate* canonical — but blocking it with a
    /// 409 silently drops the user's document. De-colliding the name lets the
    /// document ingest cleanly while leaving the seed anchor untouched.
    ///
    /// A User-vs-User collision is deliberately left to the existing 409 path:
    /// silently re-naming a real user schema would regrow the duplicate
    /// `descriptive_name` pile the cross-type guard exists to prevent (see
    /// `cross_schema_type_does_not_propose_expansion`).
    ///
    /// Returns `Some(new_name)` only when a rename is required; `None` when
    /// there is no seed collision (the proposal keeps its name and follows the
    /// normal dedup/expansion/409 path). Best-effort: any lookup failure
    /// returns `None`.
    pub(super) fn decollision_candidates<'a>(
        desc_name: &'a str,
        schema_type: &'a schema_types::DeclarativeSchemaType,
    ) -> impl Iterator<Item = String> + 'a {
        let type_tag = format!("{schema_type:?}");
        std::iter::once(format!("{desc_name} ({type_tag})"))
            .chain((2..).map(move |n| format!("{desc_name} ({type_tag} {n})")))
    }

    pub(super) fn find_decollided_idempotent_repost(
        &self,
        incoming: &Schema,
        mutation_mappers: &HashMap<String, String>,
    ) -> Option<Schema> {
        let desc_name = incoming.descriptive_name.as_deref()?;
        let owner = incoming.owner_app_id.as_deref();

        for candidate in Self::decollision_candidates(desc_name, &incoming.schema_type) {
            match self.lookup_descriptive_name_in_namespace(owner, &candidate) {
                Ok(Some(_)) => {
                    if let Some(existing) = self.classify_idempotent_repost_with_descriptive_name(
                        incoming,
                        mutation_mappers,
                        Some(&candidate),
                    ) {
                        tracing::info!(
                            target: "schema_service::schema",
                            original = %desc_name,
                            de_collided = %candidate,
                            existing_canonical = %existing.name,
                            "Idempotent re-POST matched an existing de-collided canonical",
                        );
                        return Some(existing);
                    }
                }
                Ok(None) | Err(_) => return None,
            }
        }
        None
    }

    pub(super) fn decollide_seed_descriptive_name(&self, incoming: &Schema) -> Option<String> {
        use schema_types::SchemaSource;

        // Only User proposals get de-collided; seeds register exactly.
        if incoming.source != SchemaSource::User {
            return None;
        }
        let desc_name = incoming.descriptive_name.as_deref()?;
        let owner = incoming.owner_app_id.as_deref();

        let existing_hash = self
            .lookup_descriptive_name_in_namespace(owner, desc_name)
            .ok()??;
        {
            let schemas = read_lock(&self.schemas, "schemas").ok()?;
            let existing = schemas
                .get(&existing_hash)
                .filter(|s| s.superseded_by.is_none())?;
            // Only de-collide against a SEED of an incompatible type. A
            // same-type seed collision is a legitimate reuse/expansion (handled
            // downstream), and a User-vs-User collision keeps the 409 contract.
            if existing.source == SchemaSource::User
                || !crate::state_expansion::is_cross_schema_type_expansion(incoming, existing)
            {
                return None;
            }
        }

        // Find the first free variant: "<name> (<Type>)", then numeric suffixes
        // if even that is taken (another incompatible proposal landed first).
        for candidate in Self::decollision_candidates(desc_name, &incoming.schema_type) {
            match self.lookup_descriptive_name_in_namespace(owner, &candidate) {
                Ok(Some(_)) => continue, // taken — try the next variant
                Ok(None) => {
                    tracing::warn!(
                        target: "schema_service::schema",
                        original = %desc_name,
                        de_collided = %candidate,
                        existing_canonical = %existing_hash,
                        incoming_schema_type = ?incoming.schema_type,
                        "Descriptive_name collides with an incompatible-schema_type starter seed — \
                         de-colliding so the document ingests as its own canonical instead of a 409",
                    );
                    return Some(candidate);
                }
                Err(_) => return None, // transient read failure — leave as-is
            }
        }
        None
    }

    /// Find a free de-collided `descriptive_name` variant for a User proposal
    /// whose name collides with an existing **active User canonical of the same
    /// concept-shape** that the dual-signal purpose gate VETOED.
    ///
    /// This is the User-vs-User analogue of [`decollide_seed_descriptive_name`],
    /// used only at the final duplicate guard as the last resort BEFORE a 409.
    /// By the time we reach it the proposal has already been offered same-name
    /// reuse (the looser purpose τ 0.72 + field-fidelity gate via
    /// [`crate::state_matching::SchemaServiceState::find_purpose_reuse_target_allow_same_name`])
    /// and that gate declined — i.e. the two same-name schemas are genuinely
    /// DISTINCT concepts (Meeting-Notes discuss-vs-schedule, Hiking distinct).
    /// A hard 409 there is a dead-end that rejects a well-formed ingestion; this
    /// instead disambiguates the name so the proposal registers as its OWN
    /// canonical — the design-blessed non-merge outcome (the
    /// `distinct_purpose_blocks_merge_when_flag_on` test explicitly accepts a
    /// fresh `Added` canonical as an alternative to the 409). Card
    /// `schema-canon-exact-name-veto-409`.
    ///
    /// Uses the SAME `"<name> (<Type>)"` suffix grammar as the seed de-collide
    /// so [`crate::state_matching::strip_decollision_suffix`] recognises and
    /// strips it for semantic comparison — the suffix is a storage artifact and
    /// must not poison later reuse matching.
    ///
    /// Returns `Some(new_name)` only when a rename is needed; `None` on any
    /// lookup failure (caller keeps the 409 contract).
    pub(super) fn decollide_user_descriptive_name(&self, incoming: &Schema) -> Option<String> {
        let desc_name = incoming.descriptive_name.as_deref()?;
        let owner = incoming.owner_app_id.as_deref();

        // Find the first free variant: "<name> (<Type>)", then numeric suffixes.
        // Mirrors `decollide_seed_descriptive_name`'s grammar so the suffix is a
        // recognised de-collision artifact (see `strip_decollision_suffix`).
        for candidate in Self::decollision_candidates(desc_name, &incoming.schema_type) {
            match self.lookup_descriptive_name_in_namespace(owner, &candidate) {
                Ok(Some(_)) => continue, // taken — try the next variant
                Ok(None) => {
                    tracing::warn!(
                        target: "schema_service::schema",
                        original = %desc_name,
                        de_collided = %candidate,
                        incoming_schema_type = ?incoming.schema_type,
                        "Exact descriptive_name collides with a purpose-distinct User canonical — \
                         de-colliding so the document ingests as its own canonical instead of a 409 \
                         (card schema-canon-exact-name-veto-409)",
                    );
                    return Some(candidate);
                }
                Err(_) => return None, // transient read failure — keep the 409
            }
        }
        None
    }

    /// Convert a snake_case name to Title Case (e.g. "technical_notes" → "Technical Notes").
    pub(super) fn snake_to_title_case(name: &str) -> String {
        name.replace('_', " ")
            .split_whitespace()
            .map(|w| {
                let mut chars = w.chars();
                match chars.next() {
                    None => String::new(),
                    Some(f) => f.to_uppercase().to_string() + chars.as_str(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Generate a proper collection name from a schema's fields and field_descriptions.
    ///
    /// 1. Concatenates field names and descriptions into a single text
    /// 2. Embeds the text and compares against anchor collection names
    /// 3. If max similarity > 0.25, uses the best-matching anchor name
    /// 4. Otherwise, infers a name from field name patterns or falls back to the schema name
    pub fn generate_collection_name(&self, schema: &Schema) -> String {
        // Build text from fields + descriptions
        let field_text = Self::build_field_text(schema);

        // Try embedding-based matching against anchors
        if !self.collection_name_anchors.is_empty() && !field_text.is_empty() {
            if let Ok(embedding) = self.embedder.embed_text(&field_text) {
                let mut best_sim = f32::NEG_INFINITY;
                let mut best_idx = 0;
                for (i, anchor) in self.collection_name_anchors.iter().enumerate() {
                    let sim = cosine_similarity(&embedding, anchor);
                    if sim > best_sim {
                        best_sim = sim;
                        best_idx = i;
                    }
                }
                if best_sim > 0.25 {
                    return COLLECTION_NAME_ANCHORS[best_idx].to_string();
                }
            }
        }

        // Fallback: infer from field name patterns
        Self::infer_name_from_fields(schema)
    }

    /// Build a text string from schema fields and their descriptions for embedding.
    pub(super) fn build_field_text(schema: &Schema) -> String {
        let fields = match schema.fields.as_ref() {
            Some(f) if !f.is_empty() => f,
            _ => return String::new(),
        };

        fields
            .iter()
            .map(|f| {
                if let Some(desc) = schema.field_descriptions.get(f) {
                    format!("{f}: {desc}")
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Infer a collection name from field name patterns when embedding matching fails.
    pub(super) fn infer_name_from_fields(schema: &Schema) -> String {
        if let Some(ref fields) = schema.fields {
            let all_lower: Vec<String> = fields.iter().map(|f| f.to_lowercase()).collect();
            let joined = all_lower.join(" ");

            if joined.contains("gps") || joined.contains("camera") || joined.contains("focal") {
                return "Photography".to_string();
            }
            if (joined.contains("photo") || joined.contains("image"))
                && (joined.contains("url")
                    || joined.contains("taken")
                    || joined.contains("captured")
                    || joined.contains("camera"))
            {
                return "Photography".to_string();
            }
            if joined.contains("amount")
                || joined.contains("balance")
                || joined.contains("transaction")
            {
                return "Financial Records".to_string();
            }
            if joined.contains("title") && joined.contains("content") && joined.contains("author") {
                return "Written Works".to_string();
            }
        }

        // Use schema name if it looks like a word (not a hash)
        let name = &schema.name;
        if !name.is_empty()
            && name.len() < 40
            && name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == ' ' || c == '-')
            && name.chars().any(char::is_alphabetic)
        {
            // Don't use it if it looks like a hex hash
            if !(name.len() > 16 && name.chars().all(|c| c.is_ascii_hexdigit() || c == '_')) {
                return name.clone();
            }
        }

        "Data Records".to_string()
    }
}
