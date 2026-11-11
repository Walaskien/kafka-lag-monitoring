"""Demo consumer: reads a topic in a group. Speed (per-message delay) and pause are set over HTTP.

Pause keeps polling, so the process stays a group member but stops taking messages.
That is the "alive but stuck" case. Killing the container gives "no members".
"""
import logging
import os
import signal
import time

from confluent_kafka import Consumer

import common

log = logging.getLogger("consumer")
running = True


def stop(*_args):
    global running
    running = False


def main():
    cfg = common.settings(os.environ)
    common.wait_for_topic(cfg["bootstrap"], cfg["topic"])
    state = common.ControlState(process_ms=cfg["process_ms"])
    common.start_control_server(state, cfg["control_port"])
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg["bootstrap"],
            "group.id": cfg["group"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            # offsets are stored after the message is processed, not when it is handed over
            "enable.auto.offset.store": False,
            "session.timeout.ms": 10000,
        }
    )
    pause_applied = False
    assignment_changed = False

    def on_assign(_consumer, partitions):
        nonlocal assignment_changed
        log.info("assigned: %s", sorted(p.partition for p in partitions))
        assignment_changed = True

    def on_revoke(_consumer, partitions):
        log.info("revoked: %s", sorted(p.partition for p in partitions))

    consumer.subscribe([cfg["topic"]], on_assign=on_assign, on_revoke=on_revoke)

    done = 0
    last_age = 0.0
    last_log = time.monotonic()
    log.info("group %s on topic %s, control on :%s", cfg["group"], cfg["topic"], cfg["control_port"])
    try:
        while running:
            snap = state.snapshot()
            if snap["paused"] != pause_applied or (snap["paused"] and assignment_changed):
                assignment = consumer.assignment()
                if snap["paused"]:
                    consumer.pause(assignment)
                else:
                    consumer.resume(assignment)
                pause_applied = snap["paused"]
                assignment_changed = False
            msg = consumer.poll(0.5)
            if msg is not None and msg.error():
                log.warning("consumer error: %s", msg.error())
            elif msg is not None:
                if snap["process_ms"]:
                    time.sleep(snap["process_ms"] / 1000.0)
                try:
                    last_age = common.age_seconds(common.parse_message(msg.value()), time.time())
                except ValueError:
                    log.warning("skipping a message that is not ours at offset %s", msg.offset())
                consumer.store_offsets(msg)
                done += 1
            if time.monotonic() - last_log >= 5:
                log.info("processed=%d last_age=%.1fs state=%s", done, last_age, snap)
                last_log = time.monotonic()
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    main()
