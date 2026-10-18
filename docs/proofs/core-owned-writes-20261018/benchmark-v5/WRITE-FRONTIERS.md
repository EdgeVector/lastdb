# Actual write ACK observations

This report derives from the unchanged ordinary API receipts. It adds no product calls.

| Program and preload | Key form | Caller condition | Samples | Pending after ACK | Caught up after ACK | Dirty after ACK | Model refusals |
|---|---|---|---:|---:|---:|---:|---:|
| before-on | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | unavailable |
| before-on | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | unavailable |
| before-on | hash | update-read-primed | 10 | 10 | 0 | 10 | unavailable |
| before-on | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | unavailable |
| before-on | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | unavailable |
| before-on | hashrange | update-read-primed | 10 | 10 | 0 | 10 | unavailable |
| before-off | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | unavailable |
| before-off | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | unavailable |
| before-off | hash | update-read-primed | 10 | 10 | 0 | 10 | unavailable |
| before-off | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | unavailable |
| before-off | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | unavailable |
| before-off | hashrange | update-read-primed | 10 | 10 | 0 | 10 | unavailable |
| current-on | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-on | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-on | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-on | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-on | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-on | hashrange | update-read-primed | 10 | 2 | 8 | 2 | 8 |
| current-off | hash | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-off | hash | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-off | hash | update-read-primed | 10 | 10 | 0 | 10 | 0 |
| current-off | hashrange | new-key-after-reopen | 2 | 2 | 0 | 2 | 0 |
| current-off | hashrange | update-no-prior-value-read | 2 | 2 | 0 | 2 | 0 |
| current-off | hashrange | update-read-primed | 10 | 2 | 8 | 2 | 8 |

All reply modes in this run are queued. Status follows the reply and can observe a subsequent flush.
Caught-up status alone does not prove a disk-first reply. A queued reply does not prove memory-only preparation.
The JSON retains every request ID, response phase, allowance counter, and before/after status.
