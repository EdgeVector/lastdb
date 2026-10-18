# Matched measurements

Before is accepted completion a0ad6190 from merged main011b29f3. After is the independently frozen core-owned-writes candidate. This does not repeat the original engine, e10-to-v4, or v4-to-completion comparisons.

Each latency is in milliseconds. A positive change means a longer latency.

## Writes

| Preload | Key form | Caller condition | Ack before | Ack after | Ack change | Through flush before | Through flush after | Flush change | Reply modes |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| off | hash | new-key-after-reopen | 166.129 | 222.628 | +34.0% | 410.170 | 566.735 | +38.2% | queued → queued |
| off | hash | update-no-prior-value-read | 158.886 | 171.437 | +7.9% | 440.446 | 474.284 | +7.7% | queued → queued |
| off | hash | update-read-primed | 88.366 | 92.309 | +4.5% | 439.213 | 408.292 | -7.0% | queued → queued |
| off | hashrange | new-key-after-reopen | 300.064 | 234.999 | -21.7% | 540.129 | 481.566 | -10.8% | queued → queued |
| off | hashrange | update-no-prior-value-read | 252.273 | 280.250 | +11.1% | 583.365 | 597.129 | +2.4% | queued → queued |
| off | hashrange | update-read-primed | 190.380 | 500.717 | +163.0% | 490.905 | 573.493 | +16.8% | queued → queued |
| on | hash | new-key-after-reopen | 168.434 | 180.778 | +7.3% | 399.468 | 425.863 | +6.6% | queued → queued |
| on | hash | update-no-prior-value-read | 169.923 | 185.052 | +8.9% | 432.913 | 464.596 | +7.3% | queued → queued |
| on | hash | update-read-primed | 85.545 | 89.631 | +4.8% | 393.046 | 421.671 | +7.3% | queued → queued |
| on | hashrange | new-key-after-reopen | 210.898 | 221.310 | +4.9% | 450.553 | 459.943 | +2.1% | queued → queued |
| on | hashrange | update-no-prior-value-read | 231.057 | 241.147 | +4.4% | 546.136 | 563.753 | +3.2% | queued → queued |
| on | hashrange | update-read-primed | 152.509 | 443.213 | +190.6% | 443.882 | 560.193 | +26.2% | queued → queued |

New-key and no-prior-value-read groups have two samples. Read-primed updates have ten samples.
A queued reply does not establish memory-only preparation or absence of a disk wait.

## Reads

| Preload | Key form | Shape | Width | Caller condition | Before | After | Change | Samples |
|---|---|---|---:|---|---:|---:|---:|---:|
| off | hash | exact | 1 | first-after-reopen | 5.048 | 3.066 | -39.3% | 2 |
| off | hash | exact | 1 | repeated-same-keys | 0.309 | 0.278 | -10.2% | 10 |
| off | hash | exact | 16 | first-after-reopen | 15.302 | 12.579 | -17.8% | 2 |
| off | hash | exact | 16 | repeated-same-keys | 1.510 | 1.525 | +1.0% | 10 |
| off | hash | exact | 64 | first-after-reopen | 35.855 | 35.563 | -0.8% | 2 |
| off | hash | exact | 64 | repeated-same-keys | 4.523 | 5.103 | +12.8% | 10 |
| off | hashrange | exact | 1 | first-after-reopen | 4.815 | 3.204 | -33.5% | 2 |
| off | hashrange | exact | 1 | repeated-same-keys | 0.325 | 0.294 | -9.5% | 10 |
| off | hashrange | exact | 1 | other-keys-same-process | 2.260 | 1.965 | -13.1% | 2 |
| off | hashrange | exact | 16 | first-after-reopen | 15.238 | 11.684 | -23.3% | 2 |
| off | hashrange | exact | 16 | repeated-same-keys | 0.836 | 0.803 | -4.0% | 10 |
| off | hashrange | exact | 16 | other-keys-same-process | 12.507 | 10.307 | -17.6% | 2 |
| off | hashrange | exact | 64 | first-after-reopen | 46.032 | 36.909 | -19.8% | 2 |
| off | hashrange | exact | 64 | repeated-same-keys | 2.272 | 2.434 | +7.1% | 10 |
| off | hashrange | exact | 64 | other-keys-same-process | 43.091 | 35.151 | -18.4% | 2 |
| off | hashrange | bounded | 16 | first-after-reopen | 14.020 | 9.911 | -29.3% | 2 |
| off | hashrange | bounded | 16 | repeated-same-keys | 1.709 | 1.715 | +0.3% | 10 |
| off | hashrange | bounded | 16 | other-keys-same-process | 10.601 | 8.545 | -19.4% | 2 |
| off | hashrange | bounded | 64 | first-after-reopen | 40.791 | 32.349 | -20.7% | 2 |
| off | hashrange | bounded | 64 | repeated-same-keys | 4.529 | 4.676 | +3.2% | 10 |
| off | hashrange | bounded | 64 | other-keys-same-process | 36.843 | 28.721 | -22.0% | 2 |
| on | hash | exact | 1 | first-after-reopen | 4.593 | 3.018 | -34.3% | 2 |
| on | hash | exact | 1 | repeated-same-keys | 0.277 | 0.292 | +5.4% | 10 |
| on | hash | exact | 16 | first-after-reopen | 14.099 | 12.900 | -8.5% | 2 |
| on | hash | exact | 16 | repeated-same-keys | 1.395 | 1.575 | +12.9% | 10 |
| on | hash | exact | 64 | first-after-reopen | 36.090 | 37.370 | +3.5% | 2 |
| on | hash | exact | 64 | repeated-same-keys | 4.885 | 4.747 | -2.8% | 10 |
| on | hashrange | exact | 1 | first-after-reopen | 84.855 | 82.930 | -2.3% | 2 |
| on | hashrange | exact | 1 | repeated-same-keys | 0.339 | 0.361 | +6.7% | 10 |
| on | hashrange | exact | 1 | other-keys-same-process | 1.860 | 1.757 | -5.5% | 2 |
| on | hashrange | exact | 16 | first-after-reopen | 94.568 | 96.840 | +2.4% | 2 |
| on | hashrange | exact | 16 | repeated-same-keys | 0.833 | 0.815 | -2.1% | 10 |
| on | hashrange | exact | 16 | other-keys-same-process | 11.325 | 10.044 | -11.3% | 2 |
| on | hashrange | exact | 64 | first-after-reopen | 123.907 | 123.403 | -0.4% | 2 |
| on | hashrange | exact | 64 | repeated-same-keys | 2.212 | 2.469 | +11.7% | 10 |
| on | hashrange | exact | 64 | other-keys-same-process | 41.032 | 34.617 | -15.6% | 2 |
| on | hashrange | bounded | 16 | first-after-reopen | 95.410 | 94.721 | -0.7% | 2 |
| on | hashrange | bounded | 16 | repeated-same-keys | 1.507 | 1.560 | +3.5% | 10 |
| on | hashrange | bounded | 16 | other-keys-same-process | 11.891 | 10.065 | -15.4% | 2 |
| on | hashrange | bounded | 64 | first-after-reopen | 125.680 | 122.993 | -2.1% | 2 |
| on | hashrange | bounded | 64 | repeated-same-keys | 4.787 | 4.861 | +1.6% | 10 |
| on | hashrange | bounded | 64 | other-keys-same-process | 42.901 | 35.452 | -17.4% | 2 |

The paired JSON retains every sample, minimum, maximum, reported file cost, and write status observation.
Process reopen does not clear the OS file cache. This is not a maximum throughput measurement.
