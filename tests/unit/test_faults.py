import json

from pdp.simulator.faults import (
    CHECK_TYPES,
    FAULT_CHECK,
    FAULTS,
    FaultContext,
    inject,
    rebuild_envelope,
)


class FakeCursorResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        return FakeCursorResult((7,))

    def commit(self):
        pass


def test_every_fault_maps_to_a_known_check():
    assert set(FAULT_CHECK) == set(FAULTS)
    assert set(FAULT_CHECK.values()) <= set(CHECK_TYPES)


def test_loader_stall_stops_and_restarts_loader():
    calls, slept = [], []
    conn = FakeConn()
    ctx = FaultContext(
        settings=None,
        app_conn=conn,
        wh_conn=conn,
        compose=lambda *args: calls.append(args),
        sleep=slept.append,
    )
    fault_id = inject("loader_stall", ctx, minutes=3)
    assert fault_id == 7
    assert calls == [("stop", "loader"), ("start", "loader")] and slept == [180]
    assert "ops.fault_log" in conn.statements[0][0]


def test_rebuild_envelope_has_debezium_shape():
    row = {
        "op": "u",
        "before": {"txn_id": "t"},
        "after": {"txn_id": "t", "channel": "web"},
        "source_ts_ms": 1_700_000_000_000,
        "lsn": 123,
    }
    value = json.loads(rebuild_envelope("transactions", row))
    assert value["op"] == "u" and value["after"]["channel"] == "web"
    assert value["source"]["lsn"] == 123 and value["source"]["table"] == "transactions"


def test_fault_targets_name_known_faults():  # review I6: per-rule attribution
    from pdp.simulator.faults import FAULT_TARGET

    assert set(FAULT_TARGET) <= set(FAULTS)
    assert FAULT_TARGET["invalid_amount"] != FAULT_TARGET["invalid_tenor"]


def test_out_of_order_produces_an_unseen_older_version():  # review I6
    from pdp.simulator.faults import late_version

    first_update = {"op": "u", "lsn": 5000, "after": {"channel": "web"}}
    late = late_version(first_update)
    assert late["lsn"] == 4999 and late["after"] == first_update["after"]
    assert first_update["lsn"] == 5000  # the stored row is not modified
