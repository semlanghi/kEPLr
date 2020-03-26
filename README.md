# kEPLr


#Running an experiment

```
./scripts/server/start.sh brokers workload cs nc gr
```

where
brokers - nr_of_brokers,
worload - experiment/topic, i.e., W1,W2,W3, or W4.
cs - initial chunk size
nc = number of chunks to create
gr = chunk growth (0=const chunk size)
