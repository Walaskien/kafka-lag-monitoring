import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import promq


class Stub:
    """Serves canned JSON for a few Prometheus and Alertmanager paths."""

    def __init__(self, routes):
        routes_ref = routes

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = self.path.split("?")[0]
                if path not in routes_ref:
                    self.send_response(404)
                    self.end_headers()
                    return
                body = json.dumps(routes_ref[path]).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.server.server_port
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def vector(*values):
    result = [{"metric": {}, "value": [1700000000, str(v)]} for v in values]
    return {"status": "success", "data": {"resultType": "vector", "result": result}}


@pytest.fixture
def stub():
    servers = []

    def make(routes):
        s = Stub(routes)
        servers.append(s)
        return s.url

    yield make
    for s in servers:
        s.close()


def test_first_value(stub):
    url = stub({"/api/v1/query": vector(12.5, 3)})
    assert promq.first_value(url, "x") == 12.5


def test_first_value_empty(stub):
    url = stub({"/api/v1/query": vector()})
    assert promq.first_value(url, "x") is None


def test_failed_query_raises(stub):
    url = stub({"/api/v1/query": {"status": "error", "error": "bad"}})
    with pytest.raises(RuntimeError):
        promq.instant(url, "x")


def test_alert_states_firing_wins(stub):
    alerts = [
        {"labels": {"alertname": "A"}, "state": "pending"},
        {"labels": {"alertname": "A"}, "state": "firing"},
        {"labels": {"alertname": "B"}, "state": "pending"},
    ]
    url = stub({"/api/v1/alerts": {"status": "success", "data": {"alerts": alerts}}})
    assert promq.alert_states(url) == {"A": "firing", "B": "pending"}


def test_alertmanager_names_are_unique_and_sorted(stub):
    names = ["Z", "A", "Z"]
    url = stub({"/api/v2/alerts": [{"labels": {"alertname": n}} for n in names]})
    assert promq.am_alert_names(url) == ["A", "Z"]


@pytest.mark.parametrize(
    "value,op,threshold,expected",
    [(5, ">", 4, True), (4, ">", 4, False), (4, ">=", 4, True), (None, "<", 10, False), (0, "==", 0, True)],
)
def test_compare(value, op, threshold, expected):
    assert promq.compare(value, op, threshold) is expected


def test_wait_until_succeeds_after_a_few_tries():
    now = [0.0]
    calls = []

    def check():
        calls.append(now[0])
        return len(calls) == 3

    def sleep(seconds):
        now[0] += seconds

    assert promq.wait_until(check, timeout=60, interval=5, clock=lambda: now[0], sleep=sleep)
    assert calls == [0.0, 5.0, 10.0]


def test_wait_until_times_out():
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    assert not promq.wait_until(lambda: False, timeout=10, interval=5, clock=lambda: now[0], sleep=sleep)
    assert now[0] == 10


def test_wait_until_survives_connection_errors():
    now = [0.0]
    attempts = []

    def check():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionRefusedError("prometheus is restarting")
        return True

    def sleep(seconds):
        now[0] += seconds

    assert promq.wait_until(check, timeout=60, interval=1, clock=lambda: now[0], sleep=sleep)


def test_snapshot_line():
    line = promq.snapshot_line(1234.0, 0.0, 0.0, 20.04, ["KafkaConsumerGroupNoMembers"])
    assert line == "lag=1234 members=0 consume=0.0/s produce=20.0/s firing=KafkaConsumerGroupNoMembers"
    empty = promq.snapshot_line(None, None, None, None, [])
    assert empty == "lag=n/a members=n/a consume=n/a/s produce=n/a/s firing=-"


def test_cli_wait_alert_exit_code(stub, capsys):
    alerts = [{"labels": {"alertname": "A"}, "state": "firing"}]
    url = stub({"/api/v1/alerts": {"status": "success", "data": {"alerts": alerts}}})
    assert promq.main(["--prom", url, "wait-alert", "A", "--timeout", "1"]) == 0
    assert promq.main(["--prom", url, "wait-alert", "B", "--timeout", "0"]) == 1
    assert promq.main(["--prom", url, "wait-alert", "B", "--state", "inactive", "--timeout", "1"]) == 0
    assert "TIMEOUT" in capsys.readouterr().out
