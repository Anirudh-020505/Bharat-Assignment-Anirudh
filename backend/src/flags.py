"""
flags.py — Pure flag detection functions, one per bug.

Each function takes the relevant row/DataFrame and returns bool (or annotated Series).
Easy to unit test in isolation.

Compose these in reconcile.py.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bug 1: suspected_unit_error
# ---------------------------------------------------------------------------

MIN_PLAUSIBLE_PAISE = 5_000  # ₹50 — below this we suspect rupees-not-paise


def flag_suspected_unit_error(amount_paise: Decimal) -> bool:
    """
    Return True if amount_paise is implausibly low (< ₹50 = 5000 paise).

    Input : amount_paise (Decimal) from bank_transfers
    Output: bool — True means suspected stored-in-rupees instead of paise
    """
    return amount_paise < Decimal(str(MIN_PLAUSIBLE_PAISE))


def apply_unit_error_flag(transfers: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_suspected_unit_error' bool column to transfers DataFrame.

    Input : bank_transfers DataFrame with amount_paise (Decimal)
    Output: copy with flag column added
    """
    transfers = transfers.copy()
    transfers["flag_suspected_unit_error"] = transfers["amount_paise"].apply(
        flag_suspected_unit_error
    )
    return transfers


# ---------------------------------------------------------------------------
# Bug 9: suspected_correction  (mid-day / off-batch transfer)
# ---------------------------------------------------------------------------

_BATCH_HOUR = 23  # Normal payroll batch fires at 23:59 IST


def flag_suspected_correction(transfer_ist: pd.Timestamp | None) -> bool:
    """
    Return True when a transfer fires at an unusual (non-batch) hour.

    Normal payroll batch transfers all have timestamp 23:59:00 IST.
    Mid-day transfers (e.g. 11:00 IST) are likely manual corrections or
    adjustments. Flag for ops review — do NOT exclude from totals.

    Input : transfer_ist — tz-aware IST Timestamp or None
    Output: bool
    """
    if transfer_ist is None:
        return False
    return transfer_ist.hour < _BATCH_HOUR


def apply_correction_flag(transfers: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_suspected_correction' bool column to transfers.

    Input : bank_transfers DataFrame with transfer_ist (Timestamp | None)
    Output: copy with flag column added
    """
    transfers = transfers.copy()
    transfers["flag_suspected_correction"] = transfers["transfer_ist"].apply(
        flag_suspected_correction
    )
    return transfers


# ---------------------------------------------------------------------------
# Bug 2: rate_overlap
# ---------------------------------------------------------------------------

def flag_rate_overlap(rate_match_count: int) -> bool:
    """
    Return True when multiple wage_rates rows matched a shift before tiebreaking.

    Input : rate_match_count (int) — COUNT(*) from DuckDB window
    Output: bool
    """
    return rate_match_count > 1


def apply_rate_overlap_flag(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_rate_overlap' bool column.

    Input : shifts DataFrame with rate_match_count (int)
    Output: copy with flag column
    """
    shifts = shifts.copy()
    shifts["flag_rate_overlap"] = shifts["rate_match_count"].apply(flag_rate_overlap)
    return shifts


# ---------------------------------------------------------------------------
# Bug 3: timezone_boundary_risk
# ---------------------------------------------------------------------------

def flag_timezone_boundary_risk(
    entered_at_ist: pd.Timestamp | None,
    work_date: date,
    effective_boundaries: list[date],
    vendor_app: str | None,
) -> bool:
    """
    Return True when vendor_b (UTC) entered_at crosses a wage_rates boundary.

    Logic:
    - Only vendor_b_v1.0 has UTC ambiguity.
    - Convert to IST; if IST date != work_date AND one of the effective_boundaries
      falls between IST date and work_date, flag it.

    Input : entered_at_ist   — tz-aware IST Timestamp or None
            work_date        — canonical date column (authoritative)
            effective_boundaries — sorted list of effective_from dates from wage_rates
            vendor_app       — e.g. "vendor_b_v1.0"
    Output: bool
    """
    if vendor_app != "vendor_b_v1.0":
        return False
    if entered_at_ist is None:
        return False
    entered_date = entered_at_ist.date()
    if entered_date == work_date:
        return False
    lo = min(entered_date, work_date)
    hi = max(entered_date, work_date)
    for boundary in effective_boundaries:
        if lo < boundary <= hi:
            return True
    return False


def apply_tz_boundary_flag(
    shifts: pd.DataFrame, wage_rates: pd.DataFrame
) -> pd.DataFrame:
    """
    Add 'flag_timezone_boundary_risk' bool column to shifts.

    Input : shifts (with entered_at_ist, canonical_work_date, vendor_app)
            wage_rates (with effective_from dates)
    Output: shifts copy with flag column
    """
    boundaries = sorted(wage_rates["effective_from"].dropna().unique().tolist())
    boundary_dates: list[date] = [
        d if isinstance(d, date) else date.fromisoformat(str(d)) for d in boundaries
    ]

    shifts = shifts.copy()
    shifts["flag_timezone_boundary_risk"] = shifts.apply(
        lambda r: flag_timezone_boundary_risk(
            r.get("entered_at_ist"),
            r["canonical_work_date"],
            boundary_dates,
            r.get("vendor_app"),
        ),
        axis=1,
    )
    return shifts


# ---------------------------------------------------------------------------
# Bug 4: invalid_hours
# ---------------------------------------------------------------------------

def flag_invalid_hours(hours: Decimal) -> bool:
    """
    Return True if hours is not in valid range (0, 24].

    Input : hours (Decimal) from supervisor_logs
    Output: bool — True means exclude from pay calc, still emit row
    """
    return not (Decimal("0") < hours <= Decimal("24"))


def apply_invalid_hours_flag(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_invalid_hours' bool column.

    Input : shifts DataFrame with hours (Decimal)
    Output: copy with flag column
    """
    shifts = shifts.copy()
    shifts["flag_invalid_hours"] = shifts["hours"].apply(flag_invalid_hours)
    return shifts


# ---------------------------------------------------------------------------
# Bug 5: backdated_crosses_cycle
# ---------------------------------------------------------------------------

def flag_backdated_crosses_cycle(
    entry_lag_days: int | None, work_date: date, entered_at_ist: pd.Timestamp | None
) -> bool:
    """
    Return True when entered_at is >7 days after work_date AND crosses a calendar month.

    Backdating rule: lag > 7 days AND month(work_date) != month(entered_at_IST).

    Input : entry_lag_days (int or None)
            work_date      (date)
            entered_at_ist (Timestamp | None)
    Output: bool
    """
    if entry_lag_days is None or entered_at_ist is None:
        return False
    if entry_lag_days <= 7:
        return False
    entered_date = entered_at_ist.date()
    return (work_date.year, work_date.month) != (entered_date.year, entered_date.month)


def apply_backdated_flag(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_backdated_crosses_cycle' bool column.

    Input : shifts (with entry_lag_days, canonical_work_date, entered_at_ist)
    Output: copy with flag column
    """
    shifts = shifts.copy()
    shifts["flag_backdated_crosses_cycle"] = shifts.apply(
        lambda r: flag_backdated_crosses_cycle(
            r.get("entry_lag_days"),
            r["canonical_work_date"],
            r.get("entered_at_ist"),
        ),
        axis=1,
    )
    return shifts


# ---------------------------------------------------------------------------
# Bug 6: no_matching_transfer  (evaluated at reconciled level)
# ---------------------------------------------------------------------------

def flag_no_matching_transfer(expected_paise: Decimal, paid_paise: Decimal) -> bool:
    """
    Return True when a worker had expected pay in a period but received nothing.

    Input : expected_paise (Decimal), paid_paise (Decimal)
    Output: bool
    """
    return expected_paise > Decimal("0") and paid_paise == Decimal("0")


# ---------------------------------------------------------------------------
# Bug 7: ambiguous_name_match
# ---------------------------------------------------------------------------

def flag_ambiguous_name_match(ambiguous_candidates: str | None) -> bool:
    """
    Return True when fuzzy resolution found >1 candidate above threshold.

    Input : ambiguous_candidates — JSON string or None from identity.py
    Output: bool
    """
    if not ambiguous_candidates:
        return False
    try:
        candidates = json.loads(ambiguous_candidates)
        return len(candidates) > 1
    except (json.JSONDecodeError, TypeError):
        return False


def apply_ambiguous_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_ambiguous_name_match' bool column.

    Input : df with ambiguous_candidates column
    Output: copy with flag column
    """
    df = df.copy()
    df["flag_ambiguous_name_match"] = df["ambiguous_candidates"].apply(
        flag_ambiguous_name_match
    )
    return df


# ---------------------------------------------------------------------------
# Bug 8: rate_precision_anomaly
# ---------------------------------------------------------------------------

def flag_rate_precision_anomaly(hourly_rate_inr: Decimal | None) -> bool:
    """
    Return True if hourly_rate_inr is non-integer (e.g., 450.33).

    Use the value as-is in calculations; just flag for review.

    Input : hourly_rate_inr (Decimal | None)
    Output: bool
    """
    if hourly_rate_inr is None:
        return False
    return hourly_rate_inr != hourly_rate_inr.to_integral_value()


def apply_rate_precision_flag(shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'flag_rate_precision_anomaly' bool column.

    Input : shifts with hourly_rate_inr (Decimal | None)
    Output: copy with flag column
    """
    shifts = shifts.copy()
    shifts["flag_rate_precision_anomaly"] = shifts["hourly_rate_inr"].apply(
        flag_rate_precision_anomaly
    )
    return shifts


# ---------------------------------------------------------------------------
# Compose review_reason string
# ---------------------------------------------------------------------------

SHIFT_FLAGS = [
    ("rate_overlap", "flag_rate_overlap"),
    ("timezone_boundary_risk", "flag_timezone_boundary_risk"),
    ("invalid_hours", "flag_invalid_hours"),
    ("backdated_crosses_cycle", "flag_backdated_crosses_cycle"),
    ("ambiguous_name_match", "flag_ambiguous_name_match"),
    ("rate_precision_anomaly", "flag_rate_precision_anomaly"),
]

TRANSFER_FLAGS = [
    ("suspected_unit_error", "flag_suspected_unit_error"),
    ("suspected_correction", "flag_suspected_correction"),
    ("ambiguous_name_match", "flag_ambiguous_name_match"),
]

RECONCILED_FLAGS = [
    ("no_matching_transfer", "flag_no_matching_transfer"),
]


def build_review_reason(row: pd.Series, flag_map: list[tuple[str, str]]) -> str:
    """
    Build comma-separated review_reason string from active flag columns.

    Input : row (pd.Series), flag_map (list of (label, column_name) tuples)
    Output: comma-separated string of active flag labels or ""
    """
    active = [label for label, col in flag_map if row.get(col, False)]
    return ",".join(active)
