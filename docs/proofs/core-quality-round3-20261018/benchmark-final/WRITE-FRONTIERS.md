# Actual write ACK observations

Final Round 3 conceptual cleanup and peer-key completeness fix: PR217 reference 8c0531e6ca032bbc1e5b11379792c3a120f170a6 (reused accepted release 1b79500275f732f15b6405bd0d46d63fe853362790dcd76934cead37651b1c6c; no new reference build) versus final candidate 8c2b0eb89bc140cd453fb80ee5637b378c0513cb (release 264ed137a697526f8f779bca3e70a98ed7b75c614a6a71785486efec256aa003). Both use the current native packed disk engine and logical warm set. Fresh synthetic homes, unchanged V6 requests, two reversed-order rounds, preload on/off. Earlier894c459d measurements remain separately source-labelled.

This report derives from the unchanged ordinary API receipts. It adds no product calls.

| Program and preload | Key form | Caller condition | Samples | Pending after ACK | Caught up after ACK | Dirty after ACK | Model refusals |
|---|---|---|---:|---:|---:|---:|---:|
| before-on | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| before-on | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| before-on | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| before-on | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| before-on | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| before-on | hashrange | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| before-off | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| before-off | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| before-off | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| before-off | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| before-off | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| before-off | hashrange | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-on | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-on | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-on | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-on | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-on | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-on | hashrange | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-off | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-off | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-off | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-off | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-off | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-off | hashrange | update-read-primed | 10 | 10 | 0 | 10 | 0 |

All reply modes in this run are queued. Status follows the reply and can observe a subsequent flush.
Caught-up status alone does not prove a disk-first reply. A queued reply does not prove memory-only preparation.
The JSON retains every request ID, response phase, allowance counter, and before/after status.
