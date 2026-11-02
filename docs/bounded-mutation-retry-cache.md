# Bounded mutation retry cache

The retry cache keeps at most 8,192 mutation receipts in 256 stored records.
Each stored record keeps at most 32 receipts and 8 KiB of serialized data.
A bucket key is `retry-cache:{first fingerprint byte in hex}`.

The cache uses the full scoped mutation-content key to calculate a SHA-256
fingerprint. The first byte selects a bucket. The complete fingerprint must
match before the cache returns a mutation ID. Different databases use different
fingerprints, even when their mutation content is the same.

A bucket keeps its latest writes. A new receipt removes the oldest receipt
when the bucket reaches its cap. A cache hit does not rewrite the bucket.
A read fetches all selected buckets in one batch.

A cached receipt is an optimization. The mutation manager still compares the
current values before it skips a repeated mutation. A changed or deleted record
does not become a duplicate because an old receipt remains in the cache.
Preconditions, the delete replay cutoff, and the normal write rules remain in
force after a miss or an eviction. An evicted retry can execute as a new write
and return a new mutation ID. The cache does not promise an unlimited retry
window or a stable receipt after eviction.

The native request stores cache changes in the same private snapshot as its
record changes. A failed request publishes neither change. Other storage
implementations can lose a cache entry to a concurrent bucket replacement;
this produces a miss and does not permit a false duplicate.

## Operational status

The owner-only `GET /api/db/native-status` response includes `retry_cache`.
It reports the caps. It returns no receipt, key, or value.
