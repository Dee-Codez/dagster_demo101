"""Generate synthetic financial data into a SQLite database.

Usage:
    python scripts/seed_source.py [--customers N] [--out PATH]
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

SEGMENTS = ["retail", "premium", "corporate", "sme"]
COUNTRIES = ["GB", "DE", "FR", "NL", "IE", "US", "SG"]
ACCOUNT_TYPES = ["current", "savings", "investment", "credit"]
ACCOUNT_STATUSES = ["active", "dormant", "closed"]
TXN_TYPES = ["debit", "credit", "transfer", "fee", "refund"]
TXN_STATUSES = ["settled", "pending", "failed"]
EVENT_TYPES = ["fraud_alert", "aml_flag", "pep_match", "sanction_hit", "unusual_activity"]
SEVERITIES = ["low", "medium", "high", "critical"]

UTC = timezone.utc


def _rand_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def build_customers(n: int) -> pd.DataFrame:
    start = datetime(2015, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 1, tzinfo=UTC)
    rows = [
        {
            "customer_id": f"CUST{i:06d}",
            "full_name": fake.name(),
            "segment": random.choice(SEGMENTS),
            "onboarded_at": _rand_dt(start, end).isoformat(),
            "country": random.choice(COUNTRIES),
            "email": fake.email(),
        }
        for i in range(1, n + 1)
    ]
    return pd.DataFrame(rows)


def build_accounts(customers: pd.DataFrame) -> pd.DataFrame:
    rows = []
    account_id = 1
    for cust_id in customers["customer_id"]:
        for _ in range(random.randint(1, 3)):
            opened = fake.date_time_between(start_date="-8y", end_date="-1y", tzinfo=UTC)
            status = random.choices(ACCOUNT_STATUSES, weights=[70, 20, 10])[0]
            closed = (
                _rand_dt(opened, datetime.now(UTC)).isoformat()
                if status == "closed"
                else None
            )
            rows.append(
                {
                    "account_id": f"ACC{account_id:07d}",
                    "customer_id": cust_id,
                    "account_type": random.choice(ACCOUNT_TYPES),
                    "balance": round(random.uniform(-500, 150_000), 2),
                    "currency": random.choice(["GBP", "EUR", "USD"]),
                    "opened_at": opened.isoformat(),
                    "status": status,
                    "closed_at": closed,
                }
            )
            account_id += 1
    return pd.DataFrame(rows)


def build_transactions(accounts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    txn_id = 1
    active_accounts = accounts[accounts["status"] == "active"]["account_id"].tolist()
    for acc_id in active_accounts:
        for _ in range(random.randint(5, 60)):
            rows.append(
                {
                    "txn_id": f"TXN{txn_id:09d}",
                    "account_id": acc_id,
                    "amount": round(random.uniform(0.50, 25_000), 2),
                    "currency": random.choice(["GBP", "EUR", "USD"]),
                    "txn_type": random.choice(TXN_TYPES),
                    "txn_at": fake.date_time_between(
                        start_date="-3y", end_date="now", tzinfo=UTC
                    ).isoformat(),
                    "description": fake.bs(),
                    "status": random.choices(TXN_STATUSES, weights=[85, 10, 5])[0],
                }
            )
            txn_id += 1
    return pd.DataFrame(rows)


def build_risk_events(customers: pd.DataFrame) -> pd.DataFrame:
    rows = []
    event_id = 1
    sample = customers["customer_id"].sample(frac=0.15, random_state=42).tolist()
    for cust_id in sample:
        for _ in range(random.randint(1, 4)):
            occurred = fake.date_time_between(start_date="-2y", end_date="now", tzinfo=UTC)
            resolved = (
                _rand_dt(occurred, datetime.now(UTC)).isoformat()
                if random.random() < 0.6
                else None
            )
            rows.append(
                {
                    "event_id": f"EVT{event_id:07d}",
                    "customer_id": cust_id,
                    "event_type": random.choice(EVENT_TYPES),
                    "severity": random.choice(SEVERITIES),
                    "occurred_at": occurred.isoformat(),
                    "resolved_at": resolved,
                    "notes": fake.sentence(),
                }
            )
            event_id += 1
    return pd.DataFrame(rows)


def seed(n_customers: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{out_path}")

    customers = build_customers(n_customers)
    accounts = build_accounts(customers)
    transactions = build_transactions(accounts)
    risk_events = build_risk_events(customers)

    with engine.begin() as conn:
        customers.to_sql("customers", conn, if_exists="replace", index=False)
        accounts.to_sql("accounts", conn, if_exists="replace", index=False)
        transactions.to_sql("transactions", conn, if_exists="replace", index=False)
        risk_events.to_sql("risk_events", conn, if_exists="replace", index=False)

    print(f"Seeded {len(customers):,} customers, {len(accounts):,} accounts, "
          f"{len(transactions):,} transactions, {len(risk_events):,} risk events → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=2_000)
    # default resolves to data-platform/data/source.db (root CWD when make seed runs)
    _default_out = Path(__file__).parents[3] / "data" / "source.db"
    parser.add_argument("--out", type=Path, default=_default_out)
    args = parser.parse_args()
    seed(args.customers, args.out)
