use super::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KeyUseSnapshot {
    pub window_minutes: u64,
    pub distinct_tip_keys: u64,
    pub overflow_tip_keys: u64,
    pub series: Vec<KeyUseRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyUseRow {
    pub client: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub distinct_tip_keys: u64,
}

#[derive(Debug, Default)]
pub(super) struct KeyUseBucket {
    pub(super) global: TipKeySketch,
    pub(super) overflow: TipKeySketch,
    pub(super) series: HashMap<(String, Option<String>), TipKeySketch>,
}

#[derive(Debug, Default)]
pub(super) struct KeyUseWindow {
    pub(super) buckets: HashMap<u64, KeyUseBucket>,
}

impl KeyUseWindow {
    pub(super) fn record(
        &mut self,
        ts_ms: u64,
        client: &str,
        schema: Option<&str>,
        keys: &TipKeySketch,
    ) {
        if keys.is_empty() {
            return;
        }
        let epoch = ts_ms / KEY_USE_BUCKET_MS;
        self.buckets
            .retain(|old, _| *old <= epoch && epoch - *old < KEY_USE_BUCKETS);
        let bucket = self.buckets.entry(epoch).or_default();
        bucket.global.merge(keys);
        let label = (client.to_string(), schema.map(str::to_string));
        if let Some(series) = bucket.series.get_mut(&label) {
            series.merge(keys);
        } else if bucket.series.len() < MAX_KEY_USE_SERIES {
            bucket.series.insert(label, keys.clone());
        } else {
            bucket.overflow.merge(keys);
        }
    }

    pub(super) fn snapshot(&self, ts_ms: u64) -> Option<KeyUseSnapshot> {
        let epoch = ts_ms / KEY_USE_BUCKET_MS;
        let mut global = TipKeySketch::default();
        let mut overflow = TipKeySketch::default();
        let mut series: HashMap<(String, Option<String>), TipKeySketch> = HashMap::new();
        for (bucket_epoch, bucket) in &self.buckets {
            if *bucket_epoch > epoch || epoch - *bucket_epoch >= KEY_USE_BUCKETS {
                continue;
            }
            global.merge(&bucket.global);
            overflow.merge(&bucket.overflow);
            for (label, keys) in &bucket.series {
                series.entry(label.clone()).or_default().merge(keys);
            }
        }
        if global.is_empty() {
            return None;
        }
        let mut rows: Vec<KeyUseRow> = series
            .into_iter()
            .map(|((client, schema), keys)| KeyUseRow {
                client,
                schema,
                distinct_tip_keys: keys.estimate(),
            })
            .collect();
        rows.sort_by(|a, b| {
            b.distinct_tip_keys
                .cmp(&a.distinct_tip_keys)
                .then_with(|| a.client.cmp(&b.client))
                .then_with(|| a.schema.cmp(&b.schema))
        });
        Some(KeyUseSnapshot {
            window_minutes: 60,
            distinct_tip_keys: global.estimate(),
            overflow_tip_keys: overflow.estimate(),
            series: rows,
        })
    }
}
