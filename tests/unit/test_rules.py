from decimal import Decimal

import pytest

from pdp.stream.features import Features
from pdp.stream.rules import evaluate, utilization


def feat(count10: int = 1, new_device: bool = False) -> Features:
    return Features(count10, Decimal(0), count10, Decimal(0), new_device)


@pytest.mark.parametrize(
    "f,amount,limit,exposure,expected",
    [
        (feat(count10=5), "10000", "20000000", "0", {"RR1_velocity"}),
        (feat(), "850000", "1000000", None, {"RR2_near_limit"}),
        (feat(new_device=True), "2500000", "20000000", "0", {"RR3_new_device_high"}),
        (feat(), "10000", "1000000", "960000", {"RR4_utilization"}),
        (feat(count10=4), "10000", "1000000", "100000", set()),
    ],
)
def test_rules(f, amount, limit, exposure, expected):
    got = evaluate(
        f, Decimal(amount), Decimal(limit), None if exposure is None else Decimal(exposure)
    )
    assert set(got) == expected


def test_rules_without_limit_skip_limit_rules():
    assert evaluate(feat(count10=1), Decimal("5000000"), None, None) == []


def test_utilization():
    assert utilization(Decimal("500"), Decimal("1000")) == 0.5
    assert (
        utilization(None, Decimal("1000")) is None
        and utilization(Decimal("1"), Decimal("0")) is None
    )
