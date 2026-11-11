"""Demo producer: sends messages to a topic at a rate that can be changed over HTTP."""
import logging
import os
import signal
import time

from confluent_kafka import KafkaException, Producer

import common

log = logging.getLogger("producer")
running = True


def stop(*_args):
    global running
    running = False


def main():
    cfg = common.settings(os.environ)
    common.wait_for_topic(cfg["bootstrap"], cfg["topic"])
    state = common.ControlState(rate=cfg["rate"])
    common.start_control_server(state, cfg["control_port"])
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = Producer({"bootstrap.servers": cfg["bootstrap"], "linger.ms": 5})
    counters = {"ok": 0, "failed": 0}

    def delivered(err, _msg):
        counters["failed" if err else "ok"] += 1

    pacer = common.Pacer()
    seq = 0
    last_log = time.monotonic()
    log.info("producing to %s at %s msg/s, control on :%s", cfg["topic"], cfg["rate"], cfg["control_port"])
    while running:
        snap = state.snapshot()
        if snap["paused"]:
            producer.poll(0)
            pacer.wait(0)
        else:
            key, value = common.make_message(seq)
            try:
                producer.produce(cfg["topic"], key=key, value=value, on_delivery=delivered)
                seq += 1
            except BufferError:
                producer.poll(0.5)
            except KafkaException as exc:
                log.error("produce failed: %s", exc)
                time.sleep(1)
            producer.poll(0)
            pacer.wait(snap["rate"])
        if time.monotonic() - last_log >= 5:
            log.info("sent=%d acked=%d failed=%d state=%s", seq, counters["ok"], counters["failed"], snap)
            last_log = time.monotonic()
    producer.flush(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    main()
