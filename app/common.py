"""Shared parts of the demo producer and consumer: pacing, runtime control, message format.

Nothing here imports confluent_kafka, so the unit tests run without a broker or the client library.
"""
from __future__ import annotations

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Mapping, Optional, Tuple

MAX_RATE = 5000.0
MAX_PROCESS_MS = 60000


class ControlState:
    """Values the demo changes while the process runs (rate, per-message delay, pause)."""

    def __init__(self, rate: float = 20.0, process_ms: int = 0, paused: bool = False) -> None:
        self._lock = threading.Lock()
        self._rate = rate
        self._process_ms = process_ms
        self._paused = paused

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {"rate": self._rate, "process_ms": self._process_ms, "paused": self._paused}

    def set_rate(self, value: float) -> None:
        if not 0 <= value <= MAX_RATE:
            raise ValueError("rate must be between 0 and %d" % MAX_RATE)
        with self._lock:
            self._rate = value

    def set_process_ms(self, value: int) -> None:
        if not 0 <= value <= MAX_PROCESS_MS:
            raise ValueError("process_ms must be between 0 and %d" % MAX_PROCESS_MS)
        with self._lock:
            self._process_ms = value

    def set_paused(self, flag: bool) -> None:
        with self._lock:
            self._paused = flag


_NUMBER = r"(\d+(?:\.\d+)?)"
_ROUTES = (
    ("GET", re.compile(r"^/state$"), "state"),
    ("POST", re.compile(r"^/rate/" + _NUMBER + "$"), "rate"),
    ("POST", re.compile(r"^/process-ms/(\d+)$"), "process_ms"),
    ("POST", re.compile(r"^/pause$"), "pause"),
    ("POST", re.compile(r"^/resume$"), "resume"),
)


def apply_command(state: ControlState, method: str, path: str) -> Tuple[int, Dict[str, object]]:
    """Run one control request against the state. Returns (http status, json body)."""
    path = path.split("?", 1)[0].rstrip("/") or "/"
    path_known = False
    for route_method, pattern, name in _ROUTES:
        match = pattern.match(path)
        if not match:
            continue
        path_known = True
        if route_method != method:
            continue
        try:
            if name == "rate":
                state.set_rate(float(match.group(1)))
            elif name == "process_ms":
                state.set_process_ms(int(match.group(1)))
            elif name == "pause":
                state.set_paused(True)
            elif name == "resume":
                state.set_paused(False)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, state.snapshot()
    if path_known:
        return 405, {"error": "method not allowed"}
    return 404, {"error": "unknown path"}


class _Handler(BaseHTTPRequestHandler):
    state: ControlState

    def _reply(self) -> None:
        status, body = apply_command(self.state, self.command, self.path)
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _reply
    do_POST = _reply

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass


def start_control_server(state: ControlState, port: int) -> ThreadingHTTPServer:
    handler = type("Handler", (_Handler,), {"state": state})
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class Pacer:
    """Keeps a loop at about `rate` iterations per second; the rate may change between calls."""

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._next: Optional[float] = None

    def wait(self, rate: float) -> None:
        now = self._clock()
        if rate <= 0:
            self._next = None
            self._sleep(0.2)
            return
        # after a long stall do not fire a burst to catch up, at most one second of it
        if self._next is None or self._next < now - 1.0:
            self._next = now
        self._next += 1.0 / rate
        delay = self._next - now
        if delay > 0:
            self._sleep(delay)


def make_message(seq: int, now: Optional[float] = None, keys: int = 24) -> Tuple[bytes, bytes]:
    """Key and value for message number `seq`. The key spreads messages over the partitions."""
    ts = time.time() if now is None else now
    key = "order-%d" % (seq % keys)
    value = json.dumps({"seq": seq, "ts": round(ts, 3)}, separators=(",", ":"))
    return key.encode(), value.encode()


def parse_message(raw: bytes) -> Dict[str, float]:
    data = json.loads(raw.decode())
    if not isinstance(data, dict) or "seq" not in data or "ts" not in data:
        raise ValueError("not a demo message")
    return data


def age_seconds(message: Mapping[str, float], now: float) -> float:
    return max(0.0, now - float(message["ts"]))


def topic_exists(metadata: object, topic: str) -> bool:
    """True when cluster metadata (confluent_kafka ClusterMetadata) lists the topic without error."""
    entry = getattr(metadata, "topics", {}).get(topic)
    return entry is not None and getattr(entry, "error", None) is None


def wait_for_topic(bootstrap: str, topic: str, timeout: float = 120.0) -> None:
    """Block until the topic is visible, so the apps can start together with the broker."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout
    while True:
        try:
            if topic_exists(admin.list_topics(topic, timeout=5), topic):
                return
        except Exception:  # broker not up yet, keep trying
            pass
        if time.monotonic() > deadline:
            raise TimeoutError("topic %s not found after %ds" % (topic, timeout))
        time.sleep(2)


def settings(env: Mapping[str, str]) -> Dict[str, object]:
    """Read the demo settings from environment variables."""
    return {
        "bootstrap": env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "topic": env.get("KAFKA_TOPIC", "orders"),
        "group": env.get("KAFKA_GROUP", "orders-processor"),
        "rate": float(env.get("RATE", "20")),
        "process_ms": int(env.get("PROCESS_MS", "0")),
        "control_port": int(env.get("CONTROL_PORT", "8000")),
    }
