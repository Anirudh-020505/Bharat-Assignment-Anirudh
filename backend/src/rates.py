"""
rates.py — Effective-dated wage rate lookup via DuckDB.

Uses a window function to handle rate_overlap: most-recent effective_from wins.
All money math uses Python Decimal, prec=28, ROUND_HALF_UP.

Input : shifts DataFrame (with worker_id, canonical_work_date, hours),
        workers DataFrame, wage_rates DataFrame
Output: shifts DataFrame enriched with hourly_rate_inr (Decimal),
        expected_pay_paise (Decimal), match_count (int), rate_overlap flag
"""

from __future__ import annotations

import decimal
import logging
from decimal import ROUND_HALF_UP, Decimal

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Set global Decimal precision as documented in DECISIONS.md
decimal.getcontext().prec = 28


# ---------------------------------------------------------------------------
# DuckDB effective-dated join
# ---------------------------------------------------------------------------

_RATE_SQL = """
WITH ranked_rates AS (
  SELECT
    s.log_id,
    s.worker_id,
    CAST(s.canonical_work_date AS DATE) AS canonical_work_date,
    CAST(s.hours AS DOUBLE) AS hours,
    CAST(r.hourly_rate_inr AS DOUBLE) AS hourly_rate_inr,
    r.effective_from,
    r.effective_to,
    ROW_NUMBER() OVER (
      PARTITION BY s.log_id
      ORDER BY r.effective_from DESC
    ) AS rn,
    COUNT(*) OVER (PARTITION BY s.log_id) AS match_count
  FROM shifts s
  JOIN workers w USING (worker_id)
  JOIN wage_rates r
    ON r.role    = w.role
   AND r.state   = w.state
   AND r.seniority = w.seniority
   AND r.effective_from <= CAST(s.canonical_work_date AS DATE)
   AND (r.effective_to IS NULL OR TRY_CAST(r.effective_to AS DATE) >= CAST(s.canonical_work_date AS DATE))
)
SELECT * FROM ranked_rates WHERE rn = 1
"""


def lookup_rates(
    shifts: pd.DataFrame,
    workers: pd.DataFrame,
    wage_rates: pd.DataFrame,
) -> pd.DataFrame:
    """
    Perform effective-dated rate lookup for all shifts.

    Args:
        shifts     : supervisor_logs enriched with worker_id, canonical_work_date, hours
        workers    : canonical worker registry
        wage_rates : wage rates with effective dates

    Returns:
        shifts copy with added columns:
          hourly_rate_inr  (Decimal)
          expected_pay_paise (Decimal)
          rate_match_count (int)   — >1 means overlap existed
          rate_effective_from (date)
          rate_effective_to   (date | None)
    """
    con = duckdb.connect()

    # Register DataFrames — DuckDB needs string effective_to for NULL handling
    wr = wage_rates.copy()
    wr["effective_to"] = wr["effective_to"].apply(
        lambda v: str(v) if v is not None else None
    )
    wr["hourly_rate_inr"] = wr["hourly_rate_inr"].apply(float)

    shifts_reg = shifts.copy()
    shifts_reg["canonical_work_date"] = shifts_reg["canonical_work_date"].apply(str)
    shifts_reg["hours"] = shifts_reg["hours"].apply(float)

    con.register("shifts", shifts_reg)
    con.register("workers", workers)
    con.register("wage_rates", wr)

    try:
        result = con.execute(_RATE_SQL).df()
    except Exception as exc:
        logger.error("DuckDB rate lookup failed: %s", exc)
        raise
    finally:
        con.close()

    # Merge result back onto shifts
    shifts = shifts.copy()
    rate_cols = result[["log_id", "hourly_rate_inr", "effective_from",
                         "effective_to", "match_count"]]
    shifts = shifts.merge(rate_cols, on="log_id", how="left")

    # Convert to Decimal, compute expected_pay_paise
    shifts["hourly_rate_inr"] = shifts["hourly_rate_inr"].apply(
        lambda v: Decimal(str(v)) if pd.notna(v) else None
    )
    shifts["rate_match_count"] = shifts["match_count"].fillna(0).astype(int)
    shifts.drop(columns=["match_count"], inplace=True)

    def compute_expected(row: pd.Series) -> Decimal:
        """Return expected paise or Decimal('0') for invalid hours."""
        hours = Decimal(str(row["hours"]))
        rate = row["hourly_rate_inr"]
        if rate is None:
            return Decimal("0")
        # Bug 4: invalid_hours → set to 0, don't compute against bad input
        if hours <= 0 or hours > 24:
            return Decimal("0")
        raw = hours * rate * Decimal("100")
        return raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    shifts["expected_pay_paise"] = shifts.apply(compute_expected, axis=1)
    return shifts
