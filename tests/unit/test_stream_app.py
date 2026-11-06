import json
from decimal import Decimal
from pathlib import Path

from pdp.stream.app import is_envelope, is_purchase, purchase_alert
from pdp.stream.features import AccountWindow

CREATE = json.loads(Path("tests/fixtures/debezium/create.json").read_text())


def test_envelope_and_purchase_filters():
    assert is_envelope(CREATE) and is_purchase(CREATE)
    assert not is_envelope(None) and not is_envelope({"op": "x"})
    assert not is_purchase({**CREATE, "after": {**CREATE["after"], "status": "declined"}})


def test_new_purchase_near_limit_raises_alert_with_latency():
    features, alert = purchase_alert(CREATE, AccountWindow(), Decimal("800000"), Decimal("0"),
                                     now_ms=CREATE["source"]["ts_ms"] + 1200)  # fmt: skip
    assert features.txn_count_10m == 1
    assert alert["rules"] == ["RR2_near_limit"]  # 750k > 80% of 800k; not > Rp 2 juta for RR3
    assert alert["latency_ms"] == 1200 and alert["amount"] == "750000.00"


def test_backfilled_purchase_updates_window_without_alert():
    # A historical purchase committed now (e.g. `pdp seed` after the stack is up) is not "new":
    # it was committed long after it was created.
    late_commit = CREATE["source"]["ts_ms"] + 2 * 3600 * 1000
    backfill = {**CREATE, "source": {**CREATE["source"], "ts_ms": late_commit}}
    window = AccountWindow()
    _, alert = purchase_alert(
        backfill, window, Decimal("800000"), Decimal("0"), now_ms=late_commit + 500
    )
    assert alert is None and len(window.events) == 1


def test_purchase_processed_late_after_an_outage_still_alerts():  # review I5
    # Committed on time but processed two hours later (the stream was down): still screened.
    two_hours_later = CREATE["source"]["ts_ms"] + 2 * 3600 * 1000
    _, alert = purchase_alert(
        CREATE, AccountWindow(), Decimal("800000"), Decimal("0"), now_ms=two_hours_later
    )
    assert alert is not None and alert["latency_ms"] == 2 * 3600 * 1000


def test_bad_events_are_skipped_and_counted_but_database_outages_are_not():  # review I4
    from decimal import InvalidOperation

    import psycopg

    from pdp.stream.app import SkipBadEvents

    skip = SkipBadEvents()
    assert skip(KeyError("device_id")) and skip(InvalidOperation()) and skip.total == 2
    assert not skip(psycopg.OperationalError("connection lost")) and skip.total == 2


def test_snapshot_purchase_updates_window_without_alert():
    window = AccountWindow()
    _, alert = purchase_alert({**CREATE, "op": "r"}, window, Decimal("1"), Decimal("0"), now_ms=0)
    assert alert is None and len(window.events) == 1


def test_redelivered_purchase_is_ignored():  # Kafka delivers at least once
    window = AccountWindow()
    now = CREATE["source"]["ts_ms"] + 1200
    _, first = purchase_alert(CREATE, window, Decimal("800000"), Decimal("0"), now_ms=now)
    features, again = purchase_alert(
        CREATE, window, Decimal("800000"), Decimal("0"), now_ms=now + 60_000
    )
    assert first is not None and again is None and features is None
    assert len(window.events) == 1


def test_alert_id_is_stable_per_purchase():
    now = CREATE["source"]["ts_ms"] + 1200
    _, a = purchase_alert(CREATE, AccountWindow(), Decimal("800000"), Decimal("0"), now_ms=now)
    _, b = purchase_alert(CREATE, AccountWindow(), Decimal("800000"), Decimal("0"), now_ms=now)
    assert a["alert_id"] == b["alert_id"]
