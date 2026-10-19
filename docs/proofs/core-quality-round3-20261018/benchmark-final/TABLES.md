# Matched measurements

Final Round 3 conceptual cleanup and peer-key completeness fix: PR217 reference 8c0531e6ca032bbc1e5b11379792c3a120f170a6 (reused accepted release 1b79500275f732f15b6405bd0d46d63fe853362790dcd76934cead37651b1c6c; no new reference build) versus final candidate 8c2b0eb89bc140cd453fb80ee5637b378c0513cb (release 264ed137a697526f8f779bca3e70a98ed7b75c614a6a71785486efec256aa003). Both use the current native packed disk engine and logical warm set. Fresh synthetic homes, unchanged V6 requests, two reversed-order rounds, preload on/off. Earlier894c459d measurements remain separately source-labelled.

Each latency is in milliseconds. A positive change means a longer latency.

## Writes

| Preload | Key form | Caller condition | Ack before | Ack after | Ack change | Through flush before | Through flush after | Flush change | Reply modes |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| off | hash | new-key-after-reopen | 172.166 | 168.010 | -2.4% | 433.220 | 419.174 | -3.2% | queued → queued |
| off | hash | update-no-prior-value-read | 188.103 | 183.603 | -2.4% | 507.639 | 463.348 | -8.7% | queued → queued |
| off | hash | update-read-primed | 82.826 | 87.801 | +6.0% | 383.885 | 466.224 | +21.4% | queued → queued |
| off | hashrange | new-key-after-reopen | 239.469 | 220.184 | -8.1% | 594.307 | 470.635 | -20.8% | queued → queued |
| off | hashrange | update-no-prior-value-read | 271.376 | 251.707 | -7.2% | 707.884 | 611.067 | -13.7% | queued → queued |
| off | hashrange | update-read-primed | 229.114 | 187.635 | -18.1% | 544.388 | 494.186 | -9.2% | queued → queued |
| on | hash | new-key-after-reopen | 167.578 | 169.340 | +1.1% | 415.026 | 407.054 | -1.9% | queued → queued |
| on | hash | update-no-prior-value-read | 188.735 | 179.127 | -5.1% | 506.202 | 450.078 | -11.1% | queued → queued |
| on | hash | update-read-primed | 88.504 | 91.086 | +2.9% | 418.162 | 427.185 | +2.2% | queued → queued |
| on | hashrange | new-key-after-reopen | 226.906 | 221.409 | -2.4% | 469.344 | 466.605 | -0.6% | queued → queued |
| on | hashrange | update-no-prior-value-read | 262.151 | 237.297 | -9.5% | 606.004 | 556.674 | -8.1% | queued → queued |
| on | hashrange | update-read-primed | 225.446 | 175.999 | -21.9% | 543.039 | 498.690 | -8.2% | queued → queued |

New-key and no-prior-value-read groups have two samples. Read-primed updates have ten samples.
A queued reply does not establish memory-only preparation or absence of a disk wait.

## Reads

| Preload | Key form | Shape | Width | Caller condition | Before | After | Change | Samples |
|---|---|---|---:|---|---:|---:|---:|---:|
| off | hash | exact | 1 | first-after-reopen | 3.585 | 3.339 | -6.8% | 2 |
| off | hash | exact | 1 | repeated-same-keys | 0.336 | 0.328 | -2.3% | 10 |
| off | hash | exact | 16 | first-after-reopen | 14.277 | 13.674 | -4.2% | 2 |
| off | hash | exact | 16 | repeated-same-keys | 1.677 | 1.517 | -9.5% | 10 |
| off | hash | exact | 64 | first-after-reopen | 40.838 | 38.239 | -6.4% | 2 |
| off | hash | exact | 64 | repeated-same-keys | 4.900 | 4.929 | +0.6% | 10 |
| off | hashrange | exact | 1 | first-after-reopen | 3.638 | 4.398 | +20.9% | 2 |
| off | hashrange | exact | 1 | repeated-same-keys | 0.332 | 0.371 | +11.7% | 10 |
| off | hashrange | exact | 1 | other-keys-same-process | 2.182 | 2.518 | +15.4% | 2 |
| off | hashrange | exact | 16 | first-after-reopen | 14.803 | 15.427 | +4.2% | 2 |
| off | hashrange | exact | 16 | repeated-same-keys | 0.925 | 0.965 | +4.3% | 10 |
| off | hashrange | exact | 16 | other-keys-same-process | 11.536 | 11.715 | +1.6% | 2 |
| off | hashrange | exact | 64 | first-after-reopen | 43.185 | 40.790 | -5.5% | 2 |
| off | hashrange | exact | 64 | repeated-same-keys | 2.377 | 2.303 | -3.1% | 10 |
| off | hashrange | exact | 64 | other-keys-same-process | 33.669 | 33.067 | -1.8% | 2 |
| off | hashrange | bounded | 16 | first-after-reopen | 11.164 | 13.390 | +19.9% | 2 |
| off | hashrange | bounded | 16 | repeated-same-keys | 1.846 | 2.094 | +13.4% | 10 |
| off | hashrange | bounded | 16 | other-keys-same-process | 9.059 | 9.618 | +6.2% | 2 |
| off | hashrange | bounded | 64 | first-after-reopen | 30.900 | 33.505 | +8.4% | 2 |
| off | hashrange | bounded | 64 | repeated-same-keys | 4.484 | 4.573 | +2.0% | 10 |
| off | hashrange | bounded | 64 | other-keys-same-process | 26.905 | 28.393 | +5.5% | 2 |
| on | hash | exact | 1 | first-after-reopen | 3.021 | 3.011 | -0.3% | 2 |
| on | hash | exact | 1 | repeated-same-keys | 0.316 | 0.274 | -13.4% | 10 |
| on | hash | exact | 16 | first-after-reopen | 13.610 | 14.718 | +8.1% | 2 |
| on | hash | exact | 16 | repeated-same-keys | 1.552 | 1.576 | +1.6% | 10 |
| on | hash | exact | 64 | first-after-reopen | 37.327 | 38.476 | +3.1% | 2 |
| on | hash | exact | 64 | repeated-same-keys | 4.974 | 5.191 | +4.4% | 10 |
| on | hashrange | exact | 1 | first-after-reopen | 92.866 | 93.589 | +0.8% | 2 |
| on | hashrange | exact | 1 | repeated-same-keys | 0.363 | 0.365 | +0.7% | 10 |
| on | hashrange | exact | 1 | other-keys-same-process | 1.938 | 1.918 | -1.0% | 2 |
| on | hashrange | exact | 16 | first-after-reopen | 99.329 | 100.112 | +0.8% | 2 |
| on | hashrange | exact | 16 | repeated-same-keys | 0.819 | 0.837 | +2.3% | 10 |
| on | hashrange | exact | 16 | other-keys-same-process | 10.076 | 9.806 | -2.7% | 2 |
| on | hashrange | exact | 64 | first-after-reopen | 124.376 | 127.964 | +2.9% | 2 |
| on | hashrange | exact | 64 | repeated-same-keys | 2.403 | 2.472 | +2.9% | 10 |
| on | hashrange | exact | 64 | other-keys-same-process | 34.454 | 35.065 | +1.8% | 2 |
| on | hashrange | bounded | 16 | first-after-reopen | 101.727 | 100.121 | -1.6% | 2 |
| on | hashrange | bounded | 16 | repeated-same-keys | 1.687 | 1.616 | -4.2% | 10 |
| on | hashrange | bounded | 16 | other-keys-same-process | 10.796 | 10.558 | -2.2% | 2 |
| on | hashrange | bounded | 64 | first-after-reopen | 130.061 | 126.201 | -3.0% | 2 |
| on | hashrange | bounded | 64 | repeated-same-keys | 5.281 | 5.258 | -0.4% | 10 |
| on | hashrange | bounded | 64 | other-keys-same-process | 37.687 | 37.293 | -1.0% | 2 |

The paired JSON retains every sample, minimum, maximum, reported file cost, and write status observation.
Process reopen does not clear the OS file cache. This is not a maximum throughput measurement.
