"""
normalize.py — Phone normalization, timestamp conversion to IST, lag computation.

Input : raw strings from ingest DataFrames
Output: normalized strings / datetimes (no side effects on disk)
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta, timezone

import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException
from dateutil import parser as dtparser

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# vendor_app → timezone assumption
VENDOR_TZ: dict[str, str] = {
    "vendor_b_v1.0": "UTC",     # entered_at is UTC
    "vendor_a_v2.3": "IST",     # entered_at is already +05:30
}


# ---------------------------------------------------------------------------
# Phone normalization
# ---------------------------------------------------------------------------

def normalize_phone(raw: str | None) -> str | None:
    """
    Normalize an Indian mobile number to a bare 10-digit string.

    Strategy:
      1. Try phonenumbers.parse(raw, "IN") for structured parse.
      2. On failure, fall back to extracting the last 10 digits via regex.
      3. Return None if fewer than 10 digits are found.

    Input : raw phone string (any format: +91-..., 0 91..., spaced, etc.)
    Output: 10-digit string or None
    """
    if raw is None:
        return None
    raw_str = str(raw).strip()

    # Attempt structured parse first
    try:
        parsed = phonenumbers.parse(raw_str, "IN")
        national = str(parsed.national_number)
        if len(national) == 10:
            return national
    except NumberParseException:
        pass

    # Regex fallback: strip non-digits, take last 10
    digits = re.sub(r"\D", "", raw_str)
    if len(digits) < 10:
        return None
    return digits[-10:]


# ---------------------------------------------------------------------------
# Timestamp → IST
# ---------------------------------------------------------------------------

def to_ist(raw_ts: str, vendor_app: str | None = None) -> pd.Timestamp | None:
    """
    Parse *raw_ts* and return a timezone-aware Timestamp in IST.

    vendor_b_v1.0 timestamps are UTC; vendor_a_v2.3 are +05:30.
    If the string already carries explicit TZ info, that takes precedence.

    Input : raw timestamp string, optional vendor_app name
    Output: pd.Timestamp (tz=Asia/Kolkata) or None on parse failure
    """
    if not raw_ts or pd.isna(raw_ts):
        return None
    try:
        ts = pd.Timestamp(raw_ts)
        if ts.tzinfo is None:
            # Use vendor heuristic
            if vendor_app and VENDOR_TZ.get(vendor_app) == "UTC":
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_localize("Asia/Kolkata")
        return ts.tz_convert("Asia/Kolkata")
    except Exception as exc:
        logger.warning("to_ist failed for %r: %s", raw_ts, exc)
        return None


# ---------------------------------------------------------------------------
# Entry-lag computation
# ---------------------------------------------------------------------------

def compute_entry_lag(entered_at_ist: pd.Timestamp | None, work_date: date) -> int | None:
    """
    Compute how many calendar days after work_date the shift was entered.

    entry_lag_days = (entered_at_IST.date() - work_date).days

    Input : entered_at_ist (tz-aware IST Timestamp or None), work_date
    Output: integer (may be negative for pre-dated entries) or None
    """
    if entered_at_ist is None:
        return None
    return (entered_at_ist.date() - work_date).days


# ---------------------------------------------------------------------------
# Enrich DataFrames
# ---------------------------------------------------------------------------

def enrich_supervisor_logs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add normalized_phone, entered_at_ist, entry_lag_days to supervisor_logs.

    Does NOT overwrite work_date — canonical_work_date = work_date column.

    Input : supervisor_logs DataFrame (from ingest)
    Output: enriched copy with extra columns
    """
    df = df.copy()
    df["normalized_phone"] = df["worker_phone"].apply(normalize_phone)
    df["entered_at_ist"] = df.apply(
        lambda r: to_ist(r["entered_at"], r.get("vendor_app")), axis=1
    )
    df["canonical_work_date"] = pd.to_datetime(df["work_date"]).dt.date
    df["entry_lag_days"] = df.apply(
        lambda r: compute_entry_lag(r["entered_at_ist"], r["canonical_work_date"]),
        axis=1,
    )
    return df


def enrich_bank_transfers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add normalized_phone, transfer_ist to bank_transfers.

    Input : bank_transfers DataFrame (from ingest)
    Output: enriched copy
    """
    df = df.copy()
    df["normalized_phone"] = df["worker_phone"].apply(normalize_phone)
    # Transfers are always IST timestamps (no vendor ambiguity)
    df["transfer_ist"] = df["transfer_timestamp"].apply(lambda ts: to_ist(ts))
    df["transfer_month"] = df["transfer_ist"].apply(
        lambda ts: ts.strftime("%Y-%m") if ts is not None else None
    )
    return df
