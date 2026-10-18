# Matched measurements

Before is accepted completion a0ad6190 from merged main011b29f3. After is the independently frozen core-owned-writes candidate. This does not repeat the original engine, e10-to-v4, or v4-to-completion comparisons.

Each latency is in milliseconds. A positive change means a longer latency.

## Writes

| Preload | Key form | Caller condition | Ack before | Ack after | Ack change | Through flush before | Through flush after | Flush change | Reply modes |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| off | hash | new-key-after-reopen | 175.678 | 167.810 | -4.5% | 434.492 | 419.007 | -3.6% | queued → queued |
| off | hash | update-no-prior-value-read | 160.394 | 161.700 | +0.8% | 451.005 | 446.635 | -1.0% | queued → queued |
| off | hash | update-read-primed | 86.066 | 97.796 | +13.6% | 417.461 | 422.538 | +1.2% | queued → queued |
| off | hashrange | new-key-after-reopen | 240.276 | 222.391 | -7.4% | 483.660 | 472.061 | -2.4% | queued → queued |
| off | hashrange | update-no-prior-value-read | 251.354 | 251.933 | +0.2% | 573.188 | 581.657 | +1.5% | queued → queued |
| off | hashrange | update-read-primed | 231.938 | 243.302 | +4.9% | 533.426 | 546.056 | +2.4% | queued → queued |
| on | hash | new-key-after-reopen | 166.934 | 182.421 | +9.3% | 433.403 | 417.551 | -3.7% | queued → queued |
| on | hash | update-no-prior-value-read | 163.505 | 175.733 | +7.5% | 464.719 | 454.242 | -2.3% | queued → queued |
| on | hash | update-read-primed | 86.700 | 130.812 | +50.9% | 432.489 | 429.716 | -0.6% | queued → queued |
| on | hashrange | new-key-after-reopen | 214.873 | 216.983 | +1.0% | 477.864 | 464.680 | -2.8% | queued → queued |
| on | hashrange | update-no-prior-value-read | 247.708 | 241.423 | -2.5% | 601.481 | 566.076 | -5.9% | queued → queued |
| on | hashrange | update-read-primed | 221.672 | 238.207 | +7.5% | 558.614 | 552.564 | -1.1% | queued → queued |

New-key and no-prior-value-read groups have two samples. Read-primed updates have ten samples.
A queued reply does not establish memory-only preparation or absence of a disk wait.

## Reads

| Preload | Key form | Shape | Width | Caller condition | Before | After | Change | Samples |
|---|---|---|---:|---|---:|---:|---:|---:|
| off | hash | exact | 1 | first-after-reopen | 4.662 | 3.021 | -35.2% | 2 |
| off | hash | exact | 1 | repeated-same-keys | 0.278 | 0.278 | -0.0% | 10 |
| off | hash | exact | 16 | first-after-reopen | 14.878 | 12.591 | -15.4% | 2 |
| off | hash | exact | 16 | repeated-same-keys | 1.502 | 1.444 | -3.9% | 10 |
| off | hash | exact | 64 | first-after-reopen | 37.663 | 37.146 | -1.4% | 2 |
| off | hash | exact | 64 | repeated-same-keys | 4.892 | 4.727 | -3.4% | 10 |
| off | hashrange | exact | 1 | first-after-reopen | 5.394 | 3.301 | -38.8% | 2 |
| off | hashrange | exact | 1 | repeated-same-keys | 0.301 | 0.277 | -7.8% | 10 |
| off | hashrange | exact | 1 | other-keys-same-process | 2.305 | 1.959 | -15.0% | 2 |
| off | hashrange | exact | 16 | first-after-reopen | 14.852 | 12.228 | -17.7% | 2 |
| off | hashrange | exact | 16 | repeated-same-keys | 0.747 | 0.835 | +11.9% | 10 |
| off | hashrange | exact | 16 | other-keys-same-process | 11.626 | 10.222 | -12.1% | 2 |
| off | hashrange | exact | 64 | first-after-reopen | 47.271 | 35.586 | -24.7% | 2 |
| off | hashrange | exact | 64 | repeated-same-keys | 2.103 | 2.327 | +10.7% | 10 |
| off | hashrange | exact | 64 | other-keys-same-process | 41.186 | 34.038 | -17.4% | 2 |
| off | hashrange | bounded | 16 | first-after-reopen | 14.284 | 9.968 | -30.2% | 2 |
| off | hashrange | bounded | 16 | repeated-same-keys | 1.710 | 1.785 | +4.3% | 10 |
| off | hashrange | bounded | 16 | other-keys-same-process | 10.716 | 8.542 | -20.3% | 2 |
| off | hashrange | bounded | 64 | first-after-reopen | 39.483 | 32.324 | -18.1% | 2 |
| off | hashrange | bounded | 64 | repeated-same-keys | 4.150 | 4.535 | +9.3% | 10 |
| off | hashrange | bounded | 64 | other-keys-same-process | 35.382 | 27.811 | -21.4% | 2 |
| on | hash | exact | 1 | first-after-reopen | 4.587 | 3.036 | -33.8% | 2 |
| on | hash | exact | 1 | repeated-same-keys | 0.288 | 0.279 | -3.1% | 10 |
| on | hash | exact | 16 | first-after-reopen | 13.396 | 13.967 | +4.3% | 2 |
| on | hash | exact | 16 | repeated-same-keys | 1.407 | 1.550 | +10.2% | 10 |
| on | hash | exact | 64 | first-after-reopen | 36.785 | 34.804 | -5.4% | 2 |
| on | hash | exact | 64 | repeated-same-keys | 4.864 | 4.779 | -1.8% | 10 |
| on | hashrange | exact | 1 | first-after-reopen | 90.774 | 84.072 | -7.4% | 2 |
| on | hashrange | exact | 1 | repeated-same-keys | 0.379 | 0.360 | -5.0% | 10 |
| on | hashrange | exact | 1 | other-keys-same-process | 2.073 | 1.853 | -10.6% | 2 |
| on | hashrange | exact | 16 | first-after-reopen | 99.933 | 95.752 | -4.2% | 2 |
| on | hashrange | exact | 16 | repeated-same-keys | 0.859 | 0.801 | -6.8% | 10 |
| on | hashrange | exact | 16 | other-keys-same-process | 13.497 | 10.314 | -23.6% | 2 |
| on | hashrange | exact | 64 | first-after-reopen | 131.664 | 119.270 | -9.4% | 2 |
| on | hashrange | exact | 64 | repeated-same-keys | 2.356 | 2.344 | -0.5% | 10 |
| on | hashrange | exact | 64 | other-keys-same-process | 43.497 | 34.360 | -21.0% | 2 |
| on | hashrange | bounded | 16 | first-after-reopen | 98.688 | 91.513 | -7.3% | 2 |
| on | hashrange | bounded | 16 | repeated-same-keys | 1.647 | 1.660 | +0.8% | 10 |
| on | hashrange | bounded | 16 | other-keys-same-process | 12.751 | 10.116 | -20.7% | 2 |
| on | hashrange | bounded | 64 | first-after-reopen | 132.103 | 116.810 | -11.6% | 2 |
| on | hashrange | bounded | 64 | repeated-same-keys | 4.966 | 4.946 | -0.4% | 10 |
| on | hashrange | bounded | 64 | other-keys-same-process | 45.398 | 35.615 | -21.5% | 2 |

The paired JSON retains every sample, minimum, maximum, reported file cost, and write status observation.
Process reopen does not clear the OS file cache. This is not a maximum throughput measurement.
