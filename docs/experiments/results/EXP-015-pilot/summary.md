# EXP-015 continuous recovery summary

Source: `3296d2de248ae4379333df53c98c699a644db63e`; race: True; samples: 4; excluded: 0.

| Case | Complete | Attempts | 503 | Fault→budget ms | Budget→Ready ms | Ready→caught up ms | Recovery / stop ms | PCM peak B | Gap samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| healthy_40ms | True | 1 | 0 | — | — | — | — | 16000 | 0 |
| recover_40ms | True | 21 | 19 | 1.7 | 2058.7 | 1960.1 | 4018.8 | 86400 | 0 |
| recover_80ms | False | 21 | 19 | 1.4 | 2056.8 | — | 10001.4 | 92800 | 16000 |
| recover_100ms | False | 21 | 19 | 1.2 | 2051.5 | — | 10000.7 | 99200 | 48000 |

Checkpoint catchup uses the production coordinator predicate. Failed rows retain terminal gaps. These are single-session synthetic-worker measurements, not ASR capacity or speech-quality evidence.
