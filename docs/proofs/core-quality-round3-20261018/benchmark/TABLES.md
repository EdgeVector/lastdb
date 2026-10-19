# Matched measurements

Round 3 source cleanup: PR217 reference 8c0531e6ca032bbc1e5b11379792c3a120f170a6 (reused accepted release 1b79500275f732f15b6405bd0d46d63fe853362790dcd76934cead37651b1c6c; no new reference build) versus candidate 894c459d1e235aa928973fc6f479182acf459ef3 (release 4e6a2aa9df6c71ebf4474618ea89557c54cd20d51a337ee61bbbf0f97ca5a732). Both use the current native packed disk engine and logical warm set. Fresh synthetic homes, unchanged V6 requests, two reversed-order rounds, preload on/off.

Each latency is in milliseconds. A positive change means a longer latency.

## Writes

| Preload | Key form | Caller condition | Ack before | Ack after | Ack change | Through flush before | Through flush after | Flush change | Reply modes |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| off | hash | new-key-after-reopen | 172.357 | 185.921 | +7.9% | 447.040 | 562.119 | +25.7% | queued → queued |
| off | hash | update-no-prior-value-read | 187.153 | 194.805 | +4.1% | 514.829 | 588.193 | +14.3% | queued → queued |
| off | hash | update-read-primed | 86.485 | 136.566 | +57.9% | 436.196 | 514.797 | +18.0% | queued → queued |
| off | hashrange | new-key-after-reopen | 219.829 | 222.664 | +1.3% | 480.128 | 475.871 | -0.9% | queued → queued |
| off | hashrange | update-no-prior-value-read | 253.501 | 251.543 | -0.8% | 596.931 | 591.269 | -0.9% | queued → queued |
| off | hashrange | update-read-primed | 178.186 | 186.429 | +4.6% | 490.095 | 508.242 | +3.7% | queued → queued |
| on | hash | new-key-after-reopen | 159.237 | 177.427 | +11.4% | 395.410 | 439.714 | +11.2% | queued → queued |
| on | hash | update-no-prior-value-read | 170.433 | 186.470 | +9.4% | 453.606 | 490.231 | +8.1% | queued → queued |
| on | hash | update-read-primed | 83.887 | 85.116 | +1.5% | 403.343 | 386.657 | -4.1% | queued → queued |
| on | hashrange | new-key-after-reopen | 210.344 | 221.343 | +5.2% | 454.648 | 470.754 | +3.5% | queued → queued |
| on | hashrange | update-no-prior-value-read | 238.335 | 248.381 | +4.2% | 572.964 | 569.355 | -0.6% | queued → queued |
| on | hashrange | update-read-primed | 224.302 | 233.013 | +3.9% | 537.147 | 559.471 | +4.2% | queued → queued |

New-key and no-prior-value-read groups have two samples. Read-primed updates have ten samples.
A queued reply does not establish memory-only preparation or absence of a disk wait.

## Reads

| Preload | Key form | Shape | Width | Caller condition | Before | After | Change | Samples |
|---|---|---|---:|---|---:|---:|---:|---:|
| off | hash | exact | 1 | first-after-reopen | 4.100 | 3.358 | -18.1% | 2 |
| off | hash | exact | 1 | repeated-same-keys | 0.348 | 0.333 | -4.4% | 10 |
| off | hash | exact | 16 | first-after-reopen | 16.514 | 14.397 | -12.8% | 2 |
| off | hash | exact | 16 | repeated-same-keys | 1.706 | 1.578 | -7.5% | 10 |
| off | hash | exact | 64 | first-after-reopen | 42.808 | 35.677 | -16.7% | 2 |
| off | hash | exact | 64 | repeated-same-keys | 4.822 | 5.134 | +6.5% | 10 |
| off | hashrange | exact | 1 | first-after-reopen | 4.372 | 3.153 | -27.9% | 2 |
| off | hashrange | exact | 1 | repeated-same-keys | 0.371 | 0.304 | -18.3% | 10 |
| off | hashrange | exact | 1 | other-keys-same-process | 2.516 | 2.012 | -20.0% | 2 |
| off | hashrange | exact | 16 | first-after-reopen | 14.840 | 11.016 | -25.8% | 2 |
| off | hashrange | exact | 16 | repeated-same-keys | 0.897 | 0.816 | -9.0% | 10 |
| off | hashrange | exact | 16 | other-keys-same-process | 11.444 | 10.145 | -11.3% | 2 |
| off | hashrange | exact | 64 | first-after-reopen | 40.565 | 38.148 | -6.0% | 2 |
| off | hashrange | exact | 64 | repeated-same-keys | 2.427 | 2.256 | -7.0% | 10 |
| off | hashrange | exact | 64 | other-keys-same-process | 34.552 | 34.690 | +0.4% | 2 |
| off | hashrange | bounded | 16 | first-after-reopen | 11.500 | 11.514 | +0.1% | 2 |
| off | hashrange | bounded | 16 | repeated-same-keys | 1.826 | 1.879 | +2.9% | 10 |
| off | hashrange | bounded | 16 | other-keys-same-process | 9.272 | 9.099 | -1.9% | 2 |
| off | hashrange | bounded | 64 | first-after-reopen | 33.245 | 33.185 | -0.2% | 2 |
| off | hashrange | bounded | 64 | repeated-same-keys | 4.814 | 4.664 | -3.1% | 10 |
| off | hashrange | bounded | 64 | other-keys-same-process | 27.970 | 27.813 | -0.6% | 2 |
| on | hash | exact | 1 | first-after-reopen | 3.382 | 4.022 | +18.9% | 2 |
| on | hash | exact | 1 | repeated-same-keys | 0.299 | 0.393 | +31.4% | 10 |
| on | hash | exact | 16 | first-after-reopen | 15.348 | 16.472 | +7.3% | 2 |
| on | hash | exact | 16 | repeated-same-keys | 1.571 | 1.707 | +8.6% | 10 |
| on | hash | exact | 64 | first-after-reopen | 36.559 | 41.749 | +14.2% | 2 |
| on | hash | exact | 64 | repeated-same-keys | 4.716 | 4.926 | +4.5% | 10 |
| on | hashrange | exact | 1 | first-after-reopen | 93.181 | 95.165 | +2.1% | 2 |
| on | hashrange | exact | 1 | repeated-same-keys | 0.317 | 0.376 | +18.6% | 10 |
| on | hashrange | exact | 1 | other-keys-same-process | 1.765 | 1.941 | +10.0% | 2 |
| on | hashrange | exact | 16 | first-after-reopen | 99.961 | 98.810 | -1.2% | 2 |
| on | hashrange | exact | 16 | repeated-same-keys | 0.776 | 0.823 | +6.1% | 10 |
| on | hashrange | exact | 16 | other-keys-same-process | 9.660 | 10.003 | +3.5% | 2 |
| on | hashrange | exact | 64 | first-after-reopen | 123.049 | 123.647 | +0.5% | 2 |
| on | hashrange | exact | 64 | repeated-same-keys | 2.254 | 2.346 | +4.1% | 10 |
| on | hashrange | exact | 64 | other-keys-same-process | 33.347 | 34.031 | +2.1% | 2 |
| on | hashrange | bounded | 16 | first-after-reopen | 99.726 | 102.772 | +3.1% | 2 |
| on | hashrange | bounded | 16 | repeated-same-keys | 1.520 | 1.571 | +3.4% | 10 |
| on | hashrange | bounded | 16 | other-keys-same-process | 10.144 | 10.312 | +1.7% | 2 |
| on | hashrange | bounded | 64 | first-after-reopen | 126.496 | 127.205 | +0.6% | 2 |
| on | hashrange | bounded | 64 | repeated-same-keys | 5.106 | 5.027 | -1.6% | 10 |
| on | hashrange | bounded | 64 | other-keys-same-process | 36.747 | 36.145 | -1.6% | 2 |

The paired JSON retains every sample, minimum, maximum, reported file cost, and write status observation.
Process reopen does not clear the OS file cache. This is not a maximum throughput measurement.
