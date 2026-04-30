"""
db.py — DuckDB + SQLite glue layer.

Provides:
  - get_sqlite_conn()  : sync sqlite3 connection to output/reconciled.sqlite
  - create_schema()    : creates all tables if not exist
  - write_shifts()     : upsert enriched shifts
  - write_transfers()  : upsert enriched transfers
  - write_reconciled() : upsert reconciled rows
  - write_flags()      : upsert flag rows

All money stored as INTEGER (paise) to avoid float rounding.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path("output/reconciled.sqlite")


def get_sqlite_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Return a sync sqlite3 Connection to *db_path*.

    Creates parent directories if needed.

    Input : db_path (Path)
    Output: sqlite3.Connection
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS shifts (
    log_id                      TEXT PRIMARY KEY,
    worker_id                   TEXT,
    worker_name                 TEXT,
    normalized_phone            TEXT,
    supervisor_id               TEXT,
    canonical_work_date         TEXT,
    hours                       TEXT,       -- stored as string to preserve Decimal
    vendor_app                  TEXT,
    entered_at_ist              TEXT,
    entry_lag_days              INTEGER,
    match_method                TEXT,
    match_score                 REAL,
    ambiguous_candidates        TEXT,
    hourly_rate_inr             TEXT,
    expected_pay_paise          INTEGER,
    rate_match_count            INTEGER,
    rate_effective_from         TEXT,
    rate_effective_to           TEXT,
    flag_rate_overlap           INTEGER,
    flag_timezone_boundary_risk INTEGER,
    flag_invalid_hours          INTEGER,
    flag_backdated_crosses_cycle INTEGER,
    flag_ambiguous_name_match   INTEGER,
    flag_rate_precision_anomaly INTEGER,
    review_reason               TEXT
);

CREATE TABLE IF NOT EXISTS transfers (
    utr                         TEXT PRIMARY KEY,
    worker_id                   TEXT,
    worker_name                 TEXT,
    normalized_phone            TEXT,
    amount_paise                INTEGER,
    transfer_ist                TEXT,
    transfer_month              TEXT,
    account_last4               TEXT,
    match_method                TEXT,
    match_score                 REAL,
    ambiguous_candidates        TEXT,
    flag_suspected_unit_error   INTEGER,
    flag_suspected_correction   INTEGER,
    flag_ambiguous_name_match   INTEGER,
    review_reason               TEXT
);

CREATE TABLE IF NOT EXISTS reconciled (
    worker_id           TEXT    NOT NULL,
    period              TEXT    NOT NULL,   -- YYYY-MM
    expected_paise      INTEGER NOT NULL,
    paid_paise          INTEGER NOT NULL,
    delta_paise         INTEGER NOT NULL,   -- paid - expected; negative = underpaid
    needs_manual_review INTEGER NOT NULL,
    review_reason       TEXT    NOT NULL,
    flag_no_matching_transfer  INTEGER DEFAULT 0,
    PRIMARY KEY (worker_id, period)
);

CREATE TABLE IF NOT EXISTS flags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id   TEXT    NOT NULL,
    flag        TEXT    NOT NULL,
    evidence    TEXT
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id       TEXT PRIMARY KEY,
    name            TEXT,
    phone           TEXT,
    state           TEXT,
    role            TEXT,
    seniority       TEXT,
    registered_on   TEXT
);
"""


def create_schema(conn: sqlite3.Connection) -> None:
    """
    Create all tables in the SQLite DB (idempotent — CREATE IF NOT EXISTS).

    Input : open sqlite3.Connection
    Output: None (side effect: tables created in DB)
    """
    conn.executescript(_SCHEMA_SQL)
    conn.commit()
    logger.info("Schema created/verified")


def _bool_to_int(v: object) -> int:
    """Convert bool/None to SQLite-friendly int."""
    if v is None:
        return 0
    return int(bool(v))


def write_shifts(conn: sqlite3.Connection, shifts: pd.DataFrame) -> None:
    """
    Upsert enriched shifts into the shifts table.

    Input : open Connection, enriched shifts DataFrame
    Output: None (side effect: rows inserted/replaced in DB)
    """
    rows = []
    for _, r in shifts.iterrows():
        rows.append((
            str(r.get("log_id", "")),
            str(r.get("worker_id", "")) if r.get("worker_id") else None,
            str(r.get("worker_name", "")),
            str(r.get("normalized_phone", "")) if r.get("normalized_phone") else None,
            str(r.get("supervisor_id", "")),
            str(r.get("canonical_work_date", "")),
            str(r.get("hours", "")),
            str(r.get("vendor_app", "")),
            str(r.get("entered_at_ist", "")) if r.get("entered_at_ist") is not None else None,
            int(r["entry_lag_days"]) if r.get("entry_lag_days") is not None else None,
            str(r.get("match_method", "unmatched")),
            float(r["match_score"]) if r.get("match_score") is not None else None,
            str(r.get("ambiguous_candidates", "")) if r.get("ambiguous_candidates") else None,
            str(r.get("hourly_rate_inr", "")) if r.get("hourly_rate_inr") is not None else None,
            int(r["expected_pay_paise"]) if r.get("expected_pay_paise") is not None else 0,
            int(r.get("rate_match_count", 0)),
            str(r.get("effective_from", "")) if r.get("effective_from") is not None else None,
            str(r.get("effective_to", "")) if r.get("effective_to") is not None else None,
            _bool_to_int(r.get("flag_rate_overlap")),
            _bool_to_int(r.get("flag_timezone_boundary_risk")),
            _bool_to_int(r.get("flag_invalid_hours")),
            _bool_to_int(r.get("flag_backdated_crosses_cycle")),
            _bool_to_int(r.get("flag_ambiguous_name_match")),
            _bool_to_int(r.get("flag_rate_precision_anomaly")),
            str(r.get("review_reason", "")),
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO shifts VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""",
        rows,
    )
    conn.commit()
    logger.info("Wrote %d shift rows", len(rows))


def write_transfers(conn: sqlite3.Connection, transfers: pd.DataFrame) -> None:
    """
    Upsert enriched bank_transfers into the transfers table.

    Input : open Connection, enriched transfers DataFrame
    Output: None (side effect: rows in DB)
    """
    rows = []
    for _, r in transfers.iterrows():
        rows.append((
            str(r.get("utr", "")),
            str(r.get("worker_id", "")) if r.get("worker_id") else None,
            str(r.get("worker_name", "")),
            str(r.get("normalized_phone", "")) if r.get("normalized_phone") else None,
            int(r["amount_paise"]),
            str(r.get("transfer_ist", "")) if r.get("transfer_ist") is not None else None,
            str(r.get("transfer_month", "")) if r.get("transfer_month") else None,
            str(r.get("account_last4", "")),
            str(r.get("match_method", "unmatched")),
            float(r["match_score"]) if r.get("match_score") is not None else None,
            str(r.get("ambiguous_candidates", "")) if r.get("ambiguous_candidates") else None,
            _bool_to_int(r.get("flag_suspected_unit_error")),
            _bool_to_int(r.get("flag_suspected_correction")),
            _bool_to_int(r.get("flag_ambiguous_name_match")),
            str(r.get("review_reason", "")),
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO transfers VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""",
        rows,
    )
    conn.commit()
    logger.info("Wrote %d transfer rows", len(rows))


def write_reconciled(conn: sqlite3.Connection, reconciled: pd.DataFrame) -> None:
    """
    Upsert monthly reconciled rows.

    Input : open Connection, reconciled DataFrame with columns:
            worker_id, period, expected_paise, paid_paise, delta_paise,
            needs_manual_review, review_reason, flag_no_matching_transfer
    Output: None
    """
    rows = []
    for _, r in reconciled.iterrows():
        rows.append((
            str(r["worker_id"]),
            str(r["period"]),
            int(r["expected_paise"]),
            int(r["paid_paise"]),
            int(r["delta_paise"]),
            _bool_to_int(r["needs_manual_review"]),
            str(r.get("review_reason", "")),
            _bool_to_int(r.get("flag_no_matching_transfer", False)),
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO reconciled VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    logger.info("Wrote %d reconciled rows", len(rows))


def write_flags(conn: sqlite3.Connection, flags_rows: list[dict]) -> None:
    """
    Write flag evidence rows to the flags table (idempotent — clears old rows first).

    Input : open Connection, list of {worker_id, flag, evidence} dicts
    Output: None
    """
    conn.execute("DELETE FROM flags")
    conn.executemany(
        "INSERT INTO flags (worker_id, flag, evidence) VALUES (?,?,?)",
        [(r["worker_id"], r["flag"], r.get("evidence")) for r in flags_rows],
    )
    conn.commit()
    logger.info("Wrote %d flag rows", len(flags_rows))


def write_workers(conn: sqlite3.Connection, workers: pd.DataFrame) -> None:
    """
    Upsert canonical workers into the workers table.
    """
    rows = []
    for _, r in workers.iterrows():
        rows.append((
            str(r.get("worker_id", "")),
            str(r.get("name", "")),
            str(r.get("phone", "")),
            str(r.get("state", "")),
            str(r.get("role", "")),
            str(r.get("seniority", "")),
            str(r.get("registered_on", ""))
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO workers VALUES (
            ?,?,?,?,?,?,?
        )""",
        rows,
    )
    conn.commit()
    logger.info("Wrote %d worker rows", len(rows))
