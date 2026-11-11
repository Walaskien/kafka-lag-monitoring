# Runbook

Every alert links here. Format: symptom, what to check, what to do.
Commands assume the demo stack. On a real cluster run the same `kafka-*.sh` tools from any host with the Kafka distribution and a reachable bootstrap server.

```console
# shorthand used below
alias kcg='docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092'
alias kt='docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092'
```

## First question for any lag alert: who is consuming?

Lag alone does not say what is wrong. Three signals do:

| Group members | Committed offsets | Lag | Meaning | Alert |
|---|---|---|---|---|
| 0 | frozen | grows | the process is gone | `KafkaConsumerGroupNoMembers` |
| 1+ | frozen | grows | alive in the group, not working | `KafkaConsumerStuck` |
| 1+ | move | grows | works, but slower than the producers | `KafkaConsumerLagGrowing` |

The alert expressions are exclusive on purpose, so the page already says which row you are in. Alertmanager also drops `Stuck` and `LagGrowing` for a group that has `NoMembers`, and drops every consumer alert while the broker or the exporter alert fires.

## KafkaConsumerGroupNoMembers

**Symptom:** the group has no members and its lag is above 0. Producers keep writing, nobody reads.

**Check**
1. `kcg --describe --group orders-processor` prints "has no active members" and `-` in the CONSUMER-ID column. The LAG column is the backlog per partition.
2. Is the consumer deployment running at all? Container or pod status, restart count, exit code (137 is a kill, often OOM).
3. Last lines of the consumer log: a crash on a bad message, failed connection to the broker, authorization error.
4. Was it a rebalance gap? A member that restarts every few seconds shows `members` flapping between 0 and 1 in the dashboard.

**Do:** start the consumer (fix the crash cause first if it restarts in a loop). It resumes from the committed offset, so the backlog drains by itself. Check that the drain speed is higher than the produce rate, otherwise continue with `KafkaConsumerLagGrowing`. Do not reset offsets unless the business agrees to skip or replay messages.

## KafkaConsumerStuck

**Symptom:** the group is still joined, lag is above 0 and the committed offsets have not moved for over a minute.

**Check**
1. `kcg --describe --group orders-processor --members --verbose` shows the member and its assigned partitions, so the process is heartbeating.
2. The consumer log: silent, or the same message id repeated? A message that always fails and is retried in place blocks its partition. Compare which partitions are frozen: all of them (process hung, downstream dependency down) or one (poison message).
3. Thread dump or py-spy of the consumer; what it waits on (database lock, HTTP call without timeout, `max.poll.interval.ms` close to expiry).
4. In the dashboard, "Lag by partition": one partition growing alone means a poison message or a hot key.

**Do:** all partitions frozen: unblock the dependency, restart the consumer if it is hung. One partition: send the bad message to a dead-letter topic or skip it with `--reset-offsets --shift-by 1` on that partition (consumers stopped, see below). Add a timeout to whatever the consumer was waiting on.

## KafkaConsumerLagGrowing

**Symptom:** lag above 100 and growing, the group has members and its offsets move, but slower than the producers write.

**Check**
1. Dashboard "Produce vs consume rate": the gap between the two lines is how fast the backlog grows.
2. `kcg --describe --group orders-processor`: is the work spread evenly? Members with no partition in the output mean more consumers than partitions.
3. Time per message in the consumer: a new slow query, a slower downstream service, CPU limit throttling.
4. Did the produce rate jump (batch job, retry storm)? Then this is a capacity question, not a consumer problem.

**Do:** if the consumers are slow because of a dependency, fix that, adding consumers will not help. If they are busy and healthy, add consumers up to the number of partitions (`kt --describe --topic orders` shows it). More consumers than partitions do nothing. If the partitions are the limit, raise them with `kt --alter --topic orders --partitions 6`. Partitions can only be increased, and messages with the same key may land on a different partition afterwards, so check ordering needs first.

## KafkaBrokerMetricsMissing

**Symptom:** the JMX target of the broker is down or has no series.

**Check**
1. Is the broker container or service running? `docker compose ps`, `systemctl status kafka`.
2. Broker log: out of memory, disk full on the log directory, controller quorum problem (KRaft).
3. If the broker is up but the target is not: the java agent port (7071 here) is blocked or the agent did not start, see the first lines of the broker log.
4. `KafkaExporterDown` fires together with this one when the broker is gone, because the exporter cannot finish a scrape without the broker.

**Do:** restart the broker, free disk space or fix the quorum. Producers and consumers reconnect on their own. Afterwards confirm that group members come back and lag drains.

## KafkaExporterDown

**Symptom:** the kafka-exporter target is down. There is no lag data, so every consumer alert is blind.

**Check:** `KafkaBrokerMetricsMissing` also firing means the broker is the cause. If only the exporter is down: container status and logs, `--kafka.server` address, network path to the broker, ACLs for the exporter user (it needs Describe on topics and groups).

**Do:** restart the exporter, or fix the address or ACL.

## KafkaUnderReplicatedPartitions

**Symptom:** some partition has fewer in-sync replicas than its replication factor (for 2 minutes). Cannot happen on the single-broker demo, the rule is for multi-broker clusters.

**Check:** `kt --describe --under-replicated-partitions` lists them with Isr. A broker that is down or slow: disk, network, GC pauses. Compare with `kafka_log_size_bytes` and the broker logs of the follower.

**Do:** bring the follower back and wait for it to catch up. Do not move to `acks=1` to hide it. If it stays like this, `min.insync.replicas` plus `acks=all` will start rejecting writes.

## KafkaOfflinePartitions

**Symptom:** partitions without a leader; producers and consumers of those partitions block.

**Check:** `kt --describe --unavailable-partitions`, which brokers hold the replicas, whether they are up.

**Do:** bring a replica broker back. An unclean leader election loses data and is a last resort that needs an explicit decision.

## Resetting offsets

Only with all consumers of the group stopped (an active group refuses the reset). Always dry-run first.

```console
kcg --group orders-processor --topic orders --reset-offsets --to-latest --dry-run --export
kcg --group orders-processor --topic orders --reset-offsets --to-latest --execute
```

`--export` prints `topic,partition,new-offset` lines. The default table of Kafka 3.9 puts all rows on one line, which is hard to read. On an active group the tool answers "Assignments can only be reset if the group is inactive". The `--dry-run` forms were run against the demo (see [the incident](incident-consumer-stop.md)), `--execute` was not.

Other targets: `--to-earliest`, `--to-datetime 2026-11-11T10:00:00.000`, `--by-duration PT1H`, `--shift-by -500`, `--to-offset 1234` (with `--topic orders:2` for one partition). `--to-latest` throws the backlog away: use it only when the messages are worthless (metrics, cache refresh), never for orders or payments. After the reset, start the consumers and expect a one-off spike in `kafka:consumergroup_consume_rate:rate1m`, because Prometheus reads the jump of the committed offset as a counter step.

## Useful commands

```console
kcg --list
kcg --describe --group orders-processor            # offsets, lag per partition, owners
kcg --describe --group orders-processor --state    # Stable / Empty / PreparingRebalance
kt --describe --topic orders                       # partitions, leaders, replicas, Isr
docker compose exec -T kafka /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe --topic-list orders
```
