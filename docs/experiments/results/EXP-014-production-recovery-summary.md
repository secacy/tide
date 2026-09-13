# EXP-014 production recovery summary

Source commit: `469947dfb0833320fd8fbfd4c650601bfbd33e46`; race: `True`; samples: 30; excluded: 0.

Analyzer SHA-256: `6ae54cd1f10446510fa3417e13654501a076989ed4698e8fa60bf23cf8634b97`.

Run elapsed includes initial capture/admission and cleanup; it is not fault-to-recovery latency or an ASR benchmark.

| Case | n | Complete | Attempts min–max | Run ms min–max | PCM peak bytes max |
| --- | ---: | --- | ---: | ---: | ---: |
| normal | 3 | True | 1–1 | 5.1–9.2 | 3200 |
| worker_before_checkpoint | 3 | True | 2–2 | 16.7–19.3 | 3200 |
| worker_after_checkpoint | 3 | True | 2–2 | 17.8–19.7 | 3200 |
| websocket_checkpoint_lost | 3 | True | 2–2 | 19.5–20.6 | 3200 |
| end_closure_lost | 3 | True | 2–2 | 16.6–17.0 | 3200 |
| buffer_gap_continue | 3 | False | 2–2 | 352.1–353.3 | 1280 |
| end_tail_budget | 3 | False | 10–11 | 154.9–157.1 | 1600 |
| fallback_budget | 3 | False | 6–6 | 229.2–229.6 | 1280 |
| unsupported_worker | 3 | False | 1–1 | 4.9–7.2 | 1600 |
| invalid_checkpoint | 3 | False | 1–1 | 4.7–5.3 | 1600 |

All returned audio is partitioned into committed results and explicit gaps; all Gateways drained and Worker RPC counts reached zero.
