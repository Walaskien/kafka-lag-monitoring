# Incident: the consumer dies, lag grows, the alert fires, the consumer comes back

A recorded run of `scripts/scenario.sh` on the demo stack (Docker on a Mac, Colima). The producer writes 20 messages per second into topic `orders` (3 partitions), consumer group `orders-processor` has one consumer. The full output of the run is in [scenario-output.txt](scenario-output.txt). This page walks through the first part and then summarises the other three failures from the same run.

Timestamps are the local clock of the run. The `consume=` and `produce=` values in the snapshot lines are Prometheus `rate()` over one minute, so they trail reality by up to a minute (more on that at the end).

## 0. Before: a healthy group

Lag of a few messages, one member, consumed rate equal to produced rate, nothing firing:

```
13:35:15 lag=12 members=1 consume=15.1/s produce=15.1/s firing=-
GROUP            TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                  HOST            CLIENT-ID
orders-processor orders          0          487             495             8               rdkafka-404aed46-58e6-4e2c-ba2c-867886a85131 /172.19.0.6     rdkafka
orders-processor orders          1          178             180             2               rdkafka-404aed46-58e6-4e2c-ba2c-867886a85131 /172.19.0.6     rdkafka
orders-processor orders          2          398             405             7               rdkafka-404aed46-58e6-4e2c-ba2c-867886a85131 /172.19.0.6     rdkafka
KafkaConsumerGroupNoMembers inactive: yes
KafkaConsumerStuck inactive: yes
KafkaConsumerLagGrowing inactive: yes
```

## 1. The consumer is killed

`docker compose kill consumer` sends SIGKILL, like an OOM kill or a lost node, so the process cannot leave the group politely.

```
Killing kafkalag_consumer_1 ... 
Killing kafkalag_consumer_1 ... done
13:35:17 killed; the group still counts it as a member until session.timeout.ms (10 s)
13:35:22 lag=12 members=1 consume=16.8/s produce=16.8/s firing=-
13:35:27 lag=72 members=1 consume=17.4/s produce=18.4/s firing=-
13:35:32 lag=171 members=0 consume=17.1/s produce=20.0/s firing=-
13:35:37 lag=371 members=0 consume=13.5/s produce=20.0/s firing=-
13:35:43 lag=471 members=0 consume=11.7/s produce=20.0/s firing=-
13:35:48 lag=571 members=0 consume=9.9/s produce=20.0/s firing=-
```

In order:

1. For about ten seconds the group still lists the dead process as a member (`members=1`). The broker waits for `session.timeout.ms`, which the demo consumer sets to 10 s. With the librdkafka default of 45 s this window would be longer.
2. Then the broker drops the member and rebalances: `members=0`. Lag grows by about 100 per 5 s, which is the producer rate.
3. `consume=` falls slowly from 17 to 4 over the next minute. That is the one-minute window; the committed offsets stopped the moment the process died.

```
KafkaConsumerGroupNoMembers firing: yes
13:36:03 KafkaConsumerGroupNoMembers firing 46 s after the kill
KafkaConsumerGroupNoMembers in Alertmanager: yes
13:36:03 Alertmanager holds: KafkaConsumerGroupNoMembers
KafkaConsumerStuck inactive: yes
KafkaConsumerLagGrowing inactive: yes
```

`KafkaConsumerGroupNoMembers` fired 46 s after the kill (10 s session timeout, scrape and evaluation delay, then `for: 30s`) and reached Alertmanager. The other two consumer alerts stayed quiet: members is 0, so this is neither "stuck" nor "slow".

`kafka-consumer-groups.sh --describe` at that moment, the first thing the runbook asks for:

```
13:36:03 lag=871 members=0 consume=4.4/s produce=20.0/s firing=KafkaConsumerGroupNoMembers
Consumer group 'orders-processor' has no active members.
GROUP            TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
orders-processor orders          0          497             938             441             -               -               -
orders-processor orders          1          181             342             161             -               -               -
orders-processor orders          2          405             768             363             -               -               -
```

No active members, `-` in the CONSUMER-ID column, and 441, 161 and 363 messages waiting on the three partitions. `CURRENT-OFFSET` has not moved since the kill.

Resetting offsets is a decision about data, so the scenario only does dry runs: first "skip to the log end", then "replay the last 100 on each partition" (CSV lines: topic, partition, new offset). Nothing was changed.

```
13:36:05 offset reset, dry run: to latest (skips the whole backlog)
orders,0,951
orders,1,347
orders,2,779
13:36:06 offset reset, dry run: back 100 messages on every partition (replay)
orders,0,397
orders,1,81
orders,2,305
```

## 2. The consumer is started again

`docker compose start consumer`. It rejoins, takes over the committed offsets and reads the backlog as fast as it can.

```
Starting consumer ... 
Starting consumer ... done
13:36:13 lag=11 members=1 consume=20.0/s produce=20.0/s firing=-
13:36:18 lag=10 members=1 consume=21.1/s produce=20.0/s firing=-
13:36:23 lag=9 members=1 consume=22.9/s produce=20.0/s firing=-
sum(kafka:consumergroup_lag:sum) < 50.0: yes
KafkaConsumerGroupNoMembers inactive: yes
13:36:24 recovered 16 s after the start
13:36:24 lag=9 members=1 consume=22.9/s produce=20.0/s firing=-
GROUP            TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                  HOST            CLIENT-ID
orders-processor orders          0          1120            1126            6               rdkafka-4c525705-c344-48e0-94f3-caafdf8d8fe1 /172.19.0.6     rdkafka
orders-processor orders          1          408             411             3               rdkafka-4c525705-c344-48e0-94f3-caafdf8d8fe1 /172.19.0.6     rdkafka
orders-processor orders          2          917             921             4               rdkafka-4c525705-c344-48e0-94f3-caafdf8d8fe1 /172.19.0.6     rdkafka
```

A backlog of roughly 900 messages was gone by the first sample, about 6 s after the start. The script reports 16 s because it takes three 5 s samples before it checks. The alert resolved on its own, with no offset reset and no lost messages. This is the normal recovery for a dead consumer: restart, not reset. The drain is much faster than the 20 msg/s produce rate, which is why `consume=` stays above 20 for the next minute.

## The other three failures in the same run

| Failure | How it was made | Alert | Time to fire | Not triggered |
|---|---|---|---|---|
| Consumer slow | 100 ms per message: 10 msg/s consumed, 20 msg/s produced | `KafkaConsumerLagGrowing` | 139 s | `NoMembers`, `Stuck` |
| Consumer stuck | keeps polling (stays in the group) but takes no messages | `KafkaConsumerStuck` | 94 s | `NoMembers`, `LagGrowing` |
| Broker stopped | `docker compose stop kafka` | `KafkaBrokerMetricsMissing`, then `KafkaExporterDown` | 35 s | no consumer alert (no group data at all) |

The times include a poll interval of up to 3 s in the script. Why they differ:

- Stuck needs the committed-offset rate to fall to exactly 0 over a one-minute window before `for: 30s` starts, so about 90 s is the floor with these rules.
- Slow uses the trend of lag over two minutes plus `for: 1m`. This run came straight after the first failure, and the 2-minute window still held that failure's spike, so the trend turned positive late. I did not measure it on a quiet topic. A shorter window would be faster and a bit noisier.

Slow, first lines and the moment of the alert (`--describe` shows the group is joined and offsets are moving):

```
{"rate": 20.0, "process_ms": 100, "paused": false}
13:36:35 lag=49 members=1 consume=25.9/s produce=20.0/s firing=-
13:36:46 lag=150 members=1 consume=27.7/s produce=20.0/s firing=-
13:36:56 lag=252 members=1 consume=29.4/s produce=20.0/s firing=-
13:37:06 lag=354 members=1 consume=31.2/s produce=20.0/s firing=-
13:37:16 lag=455 members=1 consume=11.9/s produce=20.0/s firing=-
13:37:26 lag=557 members=1 consume=10.0/s produce=20.0/s firing=-
...
KafkaConsumerLagGrowing firing: yes
13:38:44 KafkaConsumerLagGrowing firing 139 s after the slowdown
KafkaConsumerGroupNoMembers inactive: yes
KafkaConsumerStuck inactive: yes
13:38:45 lag=1371 members=1 consume=9.8/s produce=20.0/s firing=KafkaConsumerLagGrowing
```

Stuck, where the committed rate goes to 0.0 while the member stays in the group:

```
{"rate": 20.0, "process_ms": 0, "paused": true}
13:39:09 lag=120 members=1 consume=37.2/s produce=20.0/s firing=-
13:39:19 lag=320 members=1 consume=35.4/s produce=20.0/s firing=-
13:39:29 lag=520 members=1 consume=33.6/s produce=20.0/s firing=-
13:39:39 lag=720 members=1 consume=31.8/s produce=20.0/s firing=-
13:39:49 lag=920 members=1 consume=3.4/s produce=20.0/s firing=-
13:39:59 lag=1120 members=1 consume=0.0/s produce=20.0/s firing=-
...
KafkaConsumerStuck firing: yes
13:40:33 KafkaConsumerStuck firing 94 s after the pause
KafkaConsumerGroupNoMembers inactive: yes
KafkaConsumerLagGrowing inactive: yes
13:40:33 lag=1820 members=1 consume=0.0/s produce=20.0/s firing=KafkaConsumerStuck
```

## Things in this run that are easy to miss

- `rate()` over a window carries the previous event for a minute. After the drain in step 2 the "consumed" value reads 26 to 31 msg/s in the first minute of the slow phase, while the consumer really does 10. The slow alert is still right, because it looks at the trend of lag and only needs the committed rate to be above 0.
- While the broker is down, `kafka-exporter` cannot finish a scrape, so the group metrics vanish (`lag=n/a members=n/a`). A lag alert cannot fire then, only the broker alerts can, which is why they exist.
- After the broker came back the consumer rejoined on its own and the lag was gone within seconds. The lag spike at the right edge of the dashboard screenshot in the README comes right after the restart. My reading is that the producer kept its messages in its local queue while the broker was away and sent them in one burst; I did not check the producer's queue to confirm it.
