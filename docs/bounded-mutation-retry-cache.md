# Bounded mutation retry cache

The retry cache keeps at most 8,192 mutation receipts in 256 stored records.
Each stored record keeps at most 32 receipts and 8 KiB of serialized data.
The namespace also keeps one completion marker and a temporary continuation
key for the old cache cleanup.

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

## Old homes

The new reader does not use the old one-record-per-content-hash cache.
Each nonempty cache write requests one yielding range page of at most 512
metadata records until it removes the old `idem:` records. A saved continuation
starts the next call after the last examined key, including keys that no longer
have a value. Unknown families also advance the continuation and remain in
place. A short page does not mark completion when the backend supplies a
continuation. Scoped old records use the same check. Cleanup does not read or
change product records.

The continuation, completion marker, and erasures belong to the same native
snapshot. A failed write does not advance the saved position. The temporary
continuation key leaves when the range ends. The new cache never adds old
`idem:` keys behind that position; use one LastDB writer version at a time.
The native format can retain deletion markers until a later format transfer.
Compaction can remove the obsolete payload files, but this change does not
claim that old deletion markers disappear from the current format.

No primary upgrade or live cleanup accompanies this source change. Use the
normal reviewed upgrade path when an operator selects the new program.
