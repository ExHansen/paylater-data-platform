"""Hand-written CDC history for dbt tests, as raw.cdc_* rows (Debezium encodings).

Scenario as of 2026-10-31 (WIB):
  T1  A1  3,000,000  tenor 3  created 2026-05-31, nothing paid
      -> DPD 123, outstanding 3,000,000
  T2  A2  1,200,000  tenor 3  created 2026-08-31, installment 1 paid
      -> current, outstanding 800,000
  T3  A3  6,000,000  tenor 6  created 2026-04-30, installments 1-2 paid
      -> installment 3 due 2026-07-30, DPD 93, outstanding 4,000,000
  T4  A2    500,000  tenor 1  created 2026-10-31 23:30 WIB (16:30 UTC)
      -> current, outstanding 500,000
  TWP90 = (3,000,000 + 4,000,000) / 8,300,000 = 0.8434
Edge cases: T2 has an update (channel app -> web) and a late re-delivery of its create event
(lower lsn, higher offset); repayment R3 is created then deleted; account A1's limit goes
5,000,000 -> 7,500,000 on 2026-08-01.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

AS_OF = "2026-10-31"
EXPECTED_TWP90 = Decimal("0.8434")
WIB = ZoneInfo("Asia/Jakarta")
EPOCH = date(1970, 1, 1)


def uid(name: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"pdp-fixture/{name}"))


def wib(y, m, d, hh=10, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=WIB)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def money(x) -> str:
    return f"{Decimal(x):.2f}"


SNAPSHOT_LSN = 1000


class Recorder:
    def __init__(self):
        self.lsn = 1000
        self.offsets: dict[str, int] = defaultdict(int)
        self.rows: dict[str, list[dict]] = defaultdict(list)

    def emit(self, table, op, pk_field, before, after, at: datetime) -> dict:
        if op == "r":  # Debezium stamps every snapshot record with the snapshot's LSN
            lsn = SNAPSHOT_LSN
        else:
            self.lsn += 10
            lsn = self.lsn
        key = (after or before)[pk_field]
        row = {
            "op": op,
            "pk": {pk_field: key},
            "before": before,
            "after": after,
            "source_ts_ms": int(at.timestamp() * 1000),
            "lsn": lsn,
        }
        self._append(table, row)
        return row

    def redeliver(self, table, row: dict) -> None:
        """The same change again (same lsn), at a later offset."""
        self._append(table, row)

    def _append(self, table, row):
        row = dict(row)
        row["kafka_topic"] = f"paylater.public.{table}"
        row["kafka_partition"] = 0
        row["kafka_offset"] = self.offsets[table]
        self.offsets[table] += 1
        self.rows[table].append(row)


def _users(r: Recorder) -> None:
    users = [
        ("U1", "Jawa Barat", "Bandung"),
        ("U2", "DKI Jakarta", "Jakarta Selatan"),
        ("U3", "Bali", "Denpasar"),
    ]
    for i, (name, province, city) in enumerate(users):
        created = wib(2026, 1, 5 + i)
        after = {
            "user_id": uid(name),
            "full_name": f"Fixture {name}",
            "nik": f"32010{i}1234567890",
            "phone": f"08123456789{i}",
            "province": province,
            "city": city,
            "kyc_status": "verified",
            "created_at": iso(created),
            "updated_at": iso(created),
        }
        r.emit("users", "r", "user_id", None, after, created)


def _accounts(r: Recorder) -> None:
    accounts = {
        "A1": ("U1", "5000000", "C"),
        "A2": ("U2", "10000000", "B"),
        "A3": ("U3", "8000000", "D"),
    }
    for name, (user, limit, grade) in accounts.items():
        created = wib(2026, 1, 10)
        after = {
            "account_id": uid(name),
            "user_id": uid(user),
            "credit_limit": money(limit),
            "status": "active",
            "risk_grade": grade,
            "created_at": iso(created),
            "updated_at": iso(created),
        }
        r.emit("credit_accounts", "r", "account_id", None, after, created)
    a1 = r.rows["credit_accounts"][0]["after"]
    changed = wib(2026, 8, 1)
    new = {**a1, "credit_limit": money("7500000"), "updated_at": iso(changed)}
    r.emit("credit_accounts", "u", "account_id", a1, new, changed)


def _merchants(r: Recorder) -> None:
    for name, category, province in (
        ("M1", "electronics", "DKI Jakarta"),
        ("M2", "fashion", "Jawa Barat"),
    ):
        created = wib(2025, 12, 1)
        after = {
            "merchant_id": uid(name),
            "name": f"Fixture {name}",
            "category": category,
            "province": province,
            "created_at": iso(created),
        }
        r.emit("merchants", "r", "merchant_id", None, after, created)


TXNS = {
    "T1": ("A1", "M1", "3000000", 3, (2026, 5, 31), []),
    "T2": ("A2", "M2", "1200000", 3, (2026, 8, 31), [(1, (2026, 9, 29))]),
    "T3": ("A3", "M1", "6000000", 6, (2026, 4, 30), [(1, (2026, 5, 29)), (2, (2026, 6, 29))]),
    "T4": ("A2", "M2", "500000", 1, (2026, 10, 31, 23, 30), []),
}
REPAYMENTS = {("T2", 1): "R1", ("T3", 1): "R2", ("T3", 2): "R4"}


def _transactions(r: Recorder) -> None:
    for name, (account, merchant, amount, tenor, created_parts, paid) in TXNS.items():
        created = wib(*created_parts)
        txn = {
            "txn_id": uid(name),
            "account_id": uid(account),
            "merchant_id": uid(merchant),
            "amount": money(amount),
            "tenor_months": tenor,
            "monthly_rate": "0.0300",
            "status": "approved",
            "channel": "app",
            "device_id": f"dev-{account}",
            "created_at": iso(created),
            "updated_at": iso(created),
        }
        create_row = r.emit("transactions", "c", "txn_id", None, txn, created)
        if name == "T2":
            later = wib(2026, 9, 1)
            r.emit(
                "transactions",
                "u",
                "txn_id",
                txn,
                {**txn, "channel": "web", "updated_at": iso(later)},
                later,
            )
            r.redeliver("transactions", create_row)  # late duplicate of v1 (Review Focus 3)
        base = (Decimal(amount) / tenor).quantize(Decimal("1"), rounding="ROUND_DOWN")
        interest = (Decimal(amount) * Decimal("0.03")).quantize(Decimal("1"))
        paid_at = {seq: wib(*parts, 9) for seq, parts in paid}
        for seq in range(1, tenor + 1):
            principal = base if seq < tenor else Decimal(amount) - base * (tenor - 1)
            due = created.date() + relativedelta(months=seq)
            inst = {
                "installment_id": uid(f"{name}-{seq}"),
                "txn_id": uid(name),
                "account_id": uid(account),
                "seq": seq,
                "due_date": (due - EPOCH).days,
                "principal": money(principal),
                "interest": money(interest),
                "status": "unpaid",
                "paid_at": None,
                "updated_at": iso(created),
            }
            r.emit("installments", "c", "installment_id", None, inst, created)
            if seq not in paid_at:
                continue
            at = paid_at[seq]
            paid_inst = {**inst, "status": "paid", "paid_at": iso(at), "updated_at": iso(at)}
            r.emit("installments", "u", "installment_id", inst, paid_inst, at)
            repayment = {
                "repayment_id": uid(REPAYMENTS[(name, seq)]),
                "installment_id": uid(f"{name}-{seq}"),
                "account_id": uid(account),
                "amount": money(principal + interest),
                "method": "va",
                "paid_at": iso(at),
            }
            r.emit("repayments", "c", "repayment_id", None, repayment, at)


def _deleted_repayment(r: Recorder) -> None:
    wrong = wib(2026, 9, 2)
    r3 = {
        "repayment_id": uid("R3"),
        "installment_id": uid("T1-1"),
        "account_id": uid("A1"),
        "amount": money("1090000"),
        "method": "retail",
        "paid_at": iso(wrong),
    }
    r.emit("repayments", "c", "repayment_id", None, r3, wrong)
    r.emit("repayments", "d", "repayment_id", r3, None, wib(2026, 9, 3))


def build_fixture() -> dict[str, list[dict]]:
    r = Recorder()
    _users(r)
    _accounts(r)
    _merchants(r)
    _transactions(r)
    _deleted_repayment(r)
    return dict(r.rows)


def load_fixture(conn) -> dict[str, int]:
    counts = {}
    for table, rows in build_fixture().items():
        for row in rows:
            conn.execute(
                f"INSERT INTO raw.cdc_{table} (op, pk, before, after, source_ts_ms, lsn, "
                "kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s)",
                (
                    row["op"],
                    json.dumps(row["pk"]),
                    None if row["before"] is None else json.dumps(row["before"]),
                    None if row["after"] is None else json.dumps(row["after"]),
                    row["source_ts_ms"],
                    row["lsn"],
                    row["kafka_topic"],
                    row["kafka_partition"],
                    row["kafka_offset"],
                ),
            )
        counts[table] = len(rows)
    return counts
