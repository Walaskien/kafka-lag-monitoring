#!/usr/bin/env bash
# Runs the whole demo against a stack that is already up (make up):
#   1. consumer dies      -> lag grows, KafkaConsumerGroupNoMembers fires, restart, lag drains
#   2. consumer is slow   -> KafkaConsumerLagGrowing fires (members present, offsets move)
#   3. consumer is stuck  -> KafkaConsumerStuck fires (members present, offsets frozen)
#   4. broker goes away   -> KafkaBrokerMetricsMissing fires, comes back
# Every wait has a timeout, a failed wait stops the script with a non-zero exit code.
set -euo pipefail
cd "$(dirname "$0")/.."

read -r -a compose <<<"${COMPOSE:-docker compose}"
promq=(python3 tools/promq.py)
consumer_api=http://localhost:19102
group=orders-processor

step() { printf '\n== %s ==\n' "$*"; }
say() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }
control() { curl -fsS -X POST "$consumer_api/$1"; echo; }

describe() {
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$group" 2>&1 | grep -v '^$' || true
}

# Dry run only, nothing is changed. --export prints topic,partition,new-offset lines,
# the default table of Kafka 3.9 puts all rows on one line.
reset_dry_run() {
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group "$group" --topic orders --reset-offsets "$@" --dry-run --export 2>&1 | grep -v '^$' || true
}

# print the group state every $2 seconds, $1 times
sample() {
  local n=$1 every=$2 i
  for ((i = 1; i <= n; i++)); do
    sleep "$every"
    say "$("${promq[@]}" snapshot)"
  done
}

# seconds since the timestamp in $1
since() { echo $(($(date +%s) - $1)); }

not_firing() { "${promq[@]}" wait-alert "$1" --state inactive --timeout 1; }

on_error() {
  echo "scenario failed, container state:"
  "${compose[@]}" ps || true
  say "$("${promq[@]}" snapshot || true)"
}
trap on_error ERR

step "0. stack is ready"
"${promq[@]}" wait-value 'count(up{job=~"kafka-.*"} == 1)' '==' 2 --timeout 120
"${promq[@]}" wait-value 'kafka_consumergroup_members{consumergroup="orders-processor"}' '==' 1 --timeout 120
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '<' 50 --timeout 120
# the rates are over 1 minute, wait until they show a steady state instead of the start-up
"${promq[@]}" wait-value 'sum(kafka:consumergroup_consume_rate:rate1m)' '>' 15 --timeout 180
say "$("${promq[@]}" snapshot)"
describe
not_firing KafkaConsumerGroupNoMembers
not_firing KafkaConsumerStuck
not_firing KafkaConsumerLagGrowing

step "1a. consumer process is killed"
t0=$(date +%s)
"${compose[@]}" kill consumer
say "killed; the group still counts it as a member until session.timeout.ms (10 s)"
sample 6 5
"${promq[@]}" wait-alert KafkaConsumerGroupNoMembers --timeout 120
say "KafkaConsumerGroupNoMembers firing $(since "$t0") s after the kill"
"${promq[@]}" wait-am KafkaConsumerGroupNoMembers
say "Alertmanager holds: $("${promq[@]}" am-alerts)"
not_firing KafkaConsumerStuck
not_firing KafkaConsumerLagGrowing
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '>' 500 --timeout 60
say "$("${promq[@]}" snapshot)"
describe
say "offset reset, dry run: to latest (skips the whole backlog)"
reset_dry_run --to-latest
say "offset reset, dry run: back 100 messages on every partition (replay)"
reset_dry_run --shift-by -100

step "1b. consumer is started again"
t0=$(date +%s)
"${compose[@]}" start consumer
sample 3 5
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '<' 50 --timeout 120
"${promq[@]}" wait-alert KafkaConsumerGroupNoMembers --state inactive --timeout 60
say "recovered $(since "$t0") s after the start"
say "$("${promq[@]}" snapshot)"
describe

step "2. slow consumer: 100 ms per message is 10 msg/s against 20 msg/s produced"
t0=$(date +%s)
control process-ms/100
sample 6 10
"${promq[@]}" wait-alert KafkaConsumerLagGrowing --timeout 180
say "KafkaConsumerLagGrowing firing $(since "$t0") s after the slowdown"
not_firing KafkaConsumerGroupNoMembers
not_firing KafkaConsumerStuck
say "$("${promq[@]}" snapshot)"
describe
control process-ms/0
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '<' 50 --timeout 180
"${promq[@]}" wait-alert KafkaConsumerLagGrowing --state inactive --timeout 60
say "$("${promq[@]}" snapshot)"

step "3. stuck consumer: still in the group, takes nothing"
t0=$(date +%s)
control pause
sample 6 10
"${promq[@]}" wait-alert KafkaConsumerStuck --timeout 180
say "KafkaConsumerStuck firing $(since "$t0") s after the pause"
not_firing KafkaConsumerGroupNoMembers
not_firing KafkaConsumerLagGrowing
say "$("${promq[@]}" snapshot)"
describe
control resume
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '<' 50 --timeout 180
"${promq[@]}" wait-alert KafkaConsumerStuck --state inactive --timeout 60
say "$("${promq[@]}" snapshot)"

step "4. broker is stopped"
t0=$(date +%s)
"${compose[@]}" stop kafka
sample 3 10
"${promq[@]}" wait-alert KafkaBrokerMetricsMissing --timeout 120
say "KafkaBrokerMetricsMissing firing $(since "$t0") s after the stop"
"${promq[@]}" wait-alert KafkaExporterDown --timeout 120
"${promq[@]}" wait-am KafkaBrokerMetricsMissing
say "Alertmanager holds: $("${promq[@]}" am-alerts)"
"${compose[@]}" start kafka
"${promq[@]}" wait-alert KafkaBrokerMetricsMissing --state inactive --timeout 180
"${promq[@]}" wait-alert KafkaExporterDown --state inactive --timeout 180
"${promq[@]}" wait-value 'kafka_consumergroup_members{consumergroup="orders-processor"}' '==' 1 --timeout 180
"${promq[@]}" wait-value 'sum(kafka:consumergroup_lag:sum)' '<' 100 --timeout 180
say "$("${promq[@]}" snapshot)"

step "done"
