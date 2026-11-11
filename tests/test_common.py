import json
from types import SimpleNamespace

import pytest

import common


def test_state_defaults_and_snapshot():
    state = common.ControlState(rate=20)
    assert state.snapshot() == {"rate": 20, "process_ms": 0, "paused": False}


@pytest.mark.parametrize("value", [-1, common.MAX_RATE + 1])
def test_rate_out_of_range(value):
    with pytest.raises(ValueError):
        common.ControlState().set_rate(value)


def test_process_ms_out_of_range():
    with pytest.raises(ValueError):
        common.ControlState().set_process_ms(common.MAX_PROCESS_MS + 1)


def test_commands_change_state():
    state = common.ControlState(rate=20)
    assert common.apply_command(state, "POST", "/rate/50.5")[0] == 200
    assert common.apply_command(state, "POST", "/process-ms/100")[0] == 200
    status, body = common.apply_command(state, "POST", "/pause")
    assert status == 200
    assert body == {"rate": 50.5, "process_ms": 100, "paused": True}
    assert common.apply_command(state, "POST", "/resume")[1]["paused"] is False


def test_state_is_readable_with_get_and_trailing_slash():
    state = common.ControlState(rate=5)
    assert common.apply_command(state, "GET", "/state/")[1]["rate"] == 5


def test_bad_requests():
    state = common.ControlState()
    assert common.apply_command(state, "POST", "/rate/999999")[0] == 400
    assert common.apply_command(state, "GET", "/rate/10")[0] == 405
    assert common.apply_command(state, "POST", "/nope")[0] == 404
    assert common.apply_command(state, "POST", "/rate/abc")[0] == 404


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.slept = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_pacer_holds_the_rate():
    fake = FakeClock()
    pacer = common.Pacer(fake.clock, fake.sleep)
    for _ in range(50):
        pacer.wait(10)
    assert fake.now - 100.0 == pytest.approx(5.0, abs=0.01)


def test_pacer_follows_a_rate_change():
    fake = FakeClock()
    pacer = common.Pacer(fake.clock, fake.sleep)
    for _ in range(10):
        pacer.wait(10)
    start = fake.now
    for _ in range(40):
        pacer.wait(20)
    assert fake.now - start == pytest.approx(2.0, abs=0.01)


def test_pacer_does_not_burst_after_a_stall():
    fake = FakeClock()
    pacer = common.Pacer(fake.clock, fake.sleep)
    pacer.wait(10)
    fake.now += 60  # process was frozen for a minute
    fake.slept.clear()
    pacer.wait(10)
    pacer.wait(10)
    assert fake.slept == [pytest.approx(0.1), pytest.approx(0.1)]


def test_pacer_rate_zero_only_sleeps():
    fake = FakeClock()
    pacer = common.Pacer(fake.clock, fake.sleep)
    pacer.wait(0)
    assert fake.slept == [0.2]


def test_message_roundtrip():
    key, value = common.make_message(7, now=1700000000.123456)
    assert key == b"order-7"
    msg = common.parse_message(value)
    assert msg == {"seq": 7, "ts": 1700000000.123}
    assert common.age_seconds(msg, 1700000002.123) == pytest.approx(2.0)


def test_message_keys_wrap_around():
    assert common.make_message(24)[0] == common.make_message(0)[0]
    assert common.make_message(25, keys=5)[0] == b"order-0"


def test_age_is_never_negative():
    assert common.age_seconds({"seq": 1, "ts": 50.0}, 49.0) == 0.0


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", json.dumps({"seq": 1}).encode()])
def test_parse_message_rejects_foreign_data(raw):
    with pytest.raises(ValueError):
        common.parse_message(raw)


def test_topic_exists():
    ok = SimpleNamespace(topics={"orders": SimpleNamespace(error=None)})
    broken = SimpleNamespace(topics={"orders": SimpleNamespace(error="UNKNOWN_TOPIC_OR_PART")})
    assert common.topic_exists(ok, "orders")
    assert not common.topic_exists(ok, "other")
    assert not common.topic_exists(broken, "orders")


def test_settings_defaults_and_overrides():
    cfg = common.settings({})
    assert cfg["bootstrap"] == "kafka:9092"
    assert cfg["topic"] == "orders"
    assert cfg["group"] == "orders-processor"
    assert cfg["rate"] == 20.0
    cfg = common.settings({"RATE": "5.5", "PROCESS_MS": "40", "KAFKA_TOPIC": "t"})
    assert (cfg["rate"], cfg["process_ms"], cfg["topic"]) == (5.5, 40, "t")
