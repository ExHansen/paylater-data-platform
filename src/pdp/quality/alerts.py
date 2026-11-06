"""Record failed checks in ops.alerts and optionally post them to a chat webhook."""

from __future__ import annotations

import json
import logging
import urllib.request

log = logging.getLogger("pdp.alerts")


def alert_message(result) -> str:
    return (
        f"[{result.check_type}] {result.target}: observed {result.observed}, "
        f"expected {result.expected}"
    )


def raise_alert(conn, result, webhook: str | None = None) -> None:
    message = alert_message(result)
    conn.execute(
        "INSERT INTO ops.alerts (check_type, target, message) VALUES (%s, %s, %s)",
        (result.check_type, result.target, message),
    )
    if not webhook:
        return
    body = json.dumps({"text": f"pdp data-quality alert: {message}"}).encode()
    request = urllib.request.Request(
        webhook, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        urllib.request.urlopen(request, timeout=5).close()
    except Exception as exc:  # noqa: BLE001 - a broken webhook must not break the checks
        log.warning("alert webhook failed: %s", exc)
