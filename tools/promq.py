#!/usr/bin/env python3
"""Small Prometheus / Alertmanager client for the scenario script (standard library only).

  promq.py query 'expr'                    print the first value, "none" when empty
  promq.py snapshot                        one line: lag, members, rates, firing alerts
  promq.py wait-alert NAME [--state ...]   wait until the alert is firing (or inactive)
  promq.py wait-value 'expr' OP NUMBER     wait until the first value satisfies the comparison
  promq.py am-alerts                       alert names Alertmanager currently holds
  promq.py wait-am NAME                    wait until Alertmanager holds the alert
"""
from __future__ import annotations

import argparse
import json
import operator
import sys
import time
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional

OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le, "==": operator.eq}
GROUP = "orders-processor"
TOPIC = "orders"


def get_json(url: str, timeout: float = 10.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def instant(prom: str, expr: str) -> List[dict]:
    url = prom.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    body = get_json(url)
    if body.get("status") != "success":
        raise RuntimeError("query failed: %s" % body)
    return body["data"]["result"]


def first_value(prom: str, expr: str) -> Optional[float]:
    result = instant(prom, expr)
    if not result:
        return None
    return float(result[0]["value"][1])


def alert_states(prom: str) -> Dict[str, str]:
    """alertname -> 'firing' or 'pending' for everything Prometheus currently has."""
    body = get_json(prom.rstrip("/") + "/api/v1/alerts")
    states: Dict[str, str] = {}
    for alert in body["data"]["alerts"]:
        name = alert["labels"]["alertname"]
        # firing wins when one rule has several instances
        if states.get(name) != "firing":
            states[name] = alert["state"]
    return states


def am_alert_names(am: str) -> List[str]:
    alerts = get_json(am.rstrip("/") + "/api/v2/alerts?active=true&silenced=false&inhibited=false")
    return sorted({a["labels"]["alertname"] for a in alerts})


def compare(value: Optional[float], op: str, threshold: float) -> bool:
    if value is None:
        return False
    return OPS[op](value, threshold)


def wait_until(
    check: Callable[[], bool],
    timeout: float,
    interval: float = 3.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    deadline = clock() + timeout
    while True:
        try:
            if check():
                return True
        except OSError:
            pass  # Prometheus restarting or not up yet, keep trying
        if clock() >= deadline:
            return False
        sleep(interval)


def fmt(value: Optional[float], digits: int = 0) -> str:
    return "n/a" if value is None else "%.*f" % (digits, value)


def snapshot_line(lag, members, consume, produce, firing: List[str]) -> str:
    return "lag=%s members=%s consume=%s/s produce=%s/s firing=%s" % (
        fmt(lag),
        fmt(members),
        fmt(consume, 1),
        fmt(produce, 1),
        ",".join(sorted(firing)) or "-",
    )


def snapshot(prom: str) -> str:
    lag = first_value(prom, 'sum(kafka:consumergroup_lag:sum{consumergroup="%s"})' % GROUP)
    members = first_value(prom, 'kafka_consumergroup_members{consumergroup="%s"}' % GROUP)
    consume = first_value(prom, 'sum(kafka:consumergroup_consume_rate:rate1m{consumergroup="%s"})' % GROUP)
    produce = first_value(prom, 'sum(kafka:topic_produce_rate:rate1m{topic="%s"})' % TOPIC)
    firing = [n for n, s in alert_states(prom).items() if s == "firing"]
    return snapshot_line(lag, members, consume, produce, firing)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--prom", default="http://localhost:19090")
    parser.add_argument("--am", default="http://localhost:19093")
    sub = parser.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("query")
    q.add_argument("expr")
    sub.add_parser("snapshot")
    wa = sub.add_parser("wait-alert")
    wa.add_argument("name")
    wa.add_argument("--state", choices=["firing", "inactive"], default="firing")
    wa.add_argument("--timeout", type=float, default=180)
    wv = sub.add_parser("wait-value")
    wv.add_argument("expr")
    wv.add_argument("op", choices=sorted(OPS))
    wv.add_argument("threshold", type=float)
    wv.add_argument("--timeout", type=float, default=120)
    sub.add_parser("am-alerts")
    wm = sub.add_parser("wait-am")
    wm.add_argument("name")
    wm.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args(argv)

    if args.cmd == "query":
        print(fmt(first_value(args.prom, args.expr), 3).replace("n/a", "none"))
    elif args.cmd == "snapshot":
        print(snapshot(args.prom))
    elif args.cmd == "am-alerts":
        print(" ".join(am_alert_names(args.am)) or "-")
    elif args.cmd == "wait-am":
        ok = wait_until(lambda: args.name in am_alert_names(args.am), args.timeout)
        print("%s in Alertmanager: %s" % (args.name, "yes" if ok else "TIMEOUT"))
        return 0 if ok else 1
    elif args.cmd == "wait-alert":

        def check() -> bool:
            firing = alert_states(args.prom).get(args.name) == "firing"
            return firing if args.state == "firing" else not firing

        ok = wait_until(check, args.timeout)
        print("%s %s: %s" % (args.name, args.state, "yes" if ok else "TIMEOUT"))
        return 0 if ok else 1
    elif args.cmd == "wait-value":
        def check() -> bool:
            return compare(first_value(args.prom, args.expr), args.op, args.threshold)

        ok = wait_until(check, args.timeout)
        print("%s %s %s: %s" % (args.expr, args.op, args.threshold, "yes" if ok else "TIMEOUT"))
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
