# EXP-015 continuous recovery summary

Source: `986565dabc98c557a124bf6b78178c93e358869f`; race: True; samples: 8; excluded: 0.

| Case | Complete | Attempts | 503 | Fault→budget ms | Budget→Ready ms | Ready→caught up ms | Recovery / stop ms | PCM peak B | Gap samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| healthy_40ms | True | 1 | 0 | — | — | — | — | 16000 | 0 |
| healthy_40ms | True | 1 | 0 | — | — | — | — | 16000 | 0 |
| recover_40ms | True | 21 | 19 | 1.3 | 2066.5 | 1949.8 | 4016.4 | 89600 | 0 |
| recover_40ms | True | 21 | 19 | 1.6 | 2071.1 | 1950.0 | 4021.1 | 89600 | 0 |
| recover_80ms | False | 21 | 19 | 3.7 | 2070.3 | — | 10001.6 | 96000 | 16000 |
| recover_80ms | False | 21 | 19 | 1.8 | 2075.5 | — | 10002.7 | 96000 | 16000 |
| recover_100ms | False | 21 | 19 | 1.7 | 2061.8 | — | 10001.5 | 99200 | 48000 |
| recover_100ms | False | 21 | 19 | 1.8 | 2072.1 | — | 10002.6 | 102400 | 48000 |

Checkpoint catchup uses the production coordinator predicate. Failed rows retain terminal gaps. These are single-session synthetic-worker measurements, not ASR capacity or speech-quality evidence.
