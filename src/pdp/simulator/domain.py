"""PayLater domain: constants and the entities stored in the application database."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

TENORS = (1, 3, 6, 12)
TENOR_WEIGHTS = (0.4, 0.3, 0.2, 0.1)


@dataclass(frozen=True)
class Grade:
    monthly_rate: Decimal
    default_prob: float
    late_prob: float
    limit_range: tuple[int, int]


RISK_GRADES: dict[str, Grade] = {
    "A": Grade(Decimal("0.020"), 0.01, 0.05, (10_000_000, 20_000_000)),
    "B": Grade(Decimal("0.025"), 0.02, 0.08, (7_000_000, 12_000_000)),
    "C": Grade(Decimal("0.030"), 0.04, 0.12, (4_000_000, 8_000_000)),
    "D": Grade(Decimal("0.035"), 0.08, 0.18, (2_000_000, 5_000_000)),
    "E": Grade(Decimal("0.040"), 0.15, 0.25, (1_000_000, 3_000_000)),
}
GRADE_WEIGHTS = (0.15, 0.25, 0.30, 0.20, 0.10)

# Rough population shares (BPS) of the most populous provinces; the rest is pooled.
_PROVINCE_SHARES = (
    ("Jawa Barat", 0.180),
    ("Jawa Timur", 0.150),
    ("Jawa Tengah", 0.135),
    ("Sumatera Utara", 0.055),
    ("Banten", 0.045),
    ("DKI Jakarta", 0.040),
    ("Sulawesi Selatan", 0.033),
    ("Lampung", 0.033),
    ("Sumatera Selatan", 0.032),
    ("Riau", 0.025),
    ("Sumatera Barat", 0.021),
    ("Nusa Tenggara Timur", 0.020),
    ("Nusa Tenggara Barat", 0.020),
    ("Aceh", 0.019),
    ("Kalimantan Barat", 0.019),
    ("Bali", 0.016),
    ("Kalimantan Timur", 0.014),
    ("DI Yogyakarta", 0.013),
)
PROVINCES: tuple[tuple[str, float], ...] = _PROVINCE_SHARES + (
    ("Lainnya", 1.0 - sum(w for _, w in _PROVINCE_SHARES)),
)

CITIES: dict[str, tuple[str, ...]] = {
    "Jawa Barat": ("Bandung", "Bekasi", "Bogor", "Depok"),
    "Jawa Timur": ("Surabaya", "Malang", "Sidoarjo"),
    "Jawa Tengah": ("Semarang", "Solo", "Tegal"),
    "Sumatera Utara": ("Medan", "Binjai"),
    "Banten": ("Tangerang", "Serang", "Cilegon"),
    "DKI Jakarta": ("Jakarta Selatan", "Jakarta Barat", "Jakarta Timur", "Jakarta Utara"),
    "Sulawesi Selatan": ("Makassar", "Parepare"),
    "Lampung": ("Bandar Lampung", "Metro"),
    "Sumatera Selatan": ("Palembang", "Prabumulih"),
    "Riau": ("Pekanbaru", "Dumai"),
    "Sumatera Barat": ("Padang", "Bukittinggi"),
    "Nusa Tenggara Timur": ("Kupang", "Ende"),
    "Nusa Tenggara Barat": ("Mataram", "Bima"),
    "Aceh": ("Banda Aceh", "Lhokseumawe"),
    "Kalimantan Barat": ("Pontianak", "Singkawang"),
    "Bali": ("Denpasar", "Badung"),
    "Kalimantan Timur": ("Samarinda", "Balikpapan"),
    "DI Yogyakarta": ("Yogyakarta", "Sleman"),
    "Lainnya": ("Manado", "Jayapura", "Ambon", "Palu", "Kendari"),
}

MERCHANT_CATEGORIES = (
    "electronics",
    "fashion",
    "groceries",
    "travel",
    "health",
    "education",
    "home",
    "beauty",
    "food_delivery",
    "gadgets",
)
PAYMENT_METHODS = ("va", "ewallet", "retail")
CHANNELS = ("app", "web", "qr")
FIRST_NAMES = (
    "Agus", "Budi", "Citra", "Dewi", "Eko", "Fitri", "Gilang", "Hana", "Indra", "Joko",
    "Kartika", "Lestari", "Made", "Nur", "Putri", "Rizky", "Sari", "Taufik", "Wulan", "Yusuf",
)  # fmt: skip
LAST_NAMES = (
    "Santoso", "Wijaya", "Pratama", "Saputra", "Lestari", "Hidayat", "Kurniawan", "Siregar",
    "Nasution", "Putra", "Wibowo", "Rahmawati", "Setiawan", "Halim", "Gunawan",
)  # fmt: skip


@dataclass(frozen=True)
class User:
    user_id: UUID
    full_name: str
    nik: str
    phone: str
    province: str
    city: str
    kyc_status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class CreditAccount:
    account_id: UUID
    user_id: UUID
    credit_limit: Decimal
    status: str
    risk_grade: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Merchant:
    merchant_id: UUID
    name: str
    category: str
    province: str
    created_at: datetime


@dataclass(frozen=True)
class Transaction:
    txn_id: UUID
    account_id: UUID
    merchant_id: UUID
    amount: Decimal
    tenor_months: int
    monthly_rate: Decimal
    status: str
    channel: str
    device_id: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Installment:
    installment_id: UUID
    txn_id: UUID
    account_id: UUID
    seq: int
    due_date: date
    principal: Decimal
    interest: Decimal
    status: str
    paid_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True)
class Repayment:
    repayment_id: UUID
    installment_id: UUID
    account_id: UUID
    amount: Decimal
    method: str
    paid_at: datetime
