"""
reconcile.py — Core orchestrator. Builds output/reconciled.sqlite and final_reconciled.csv.

Execution order:
  1. ingest.load_all()
  2. normalize (enrich phones, timestamps)
  3. identity.resolve_identities() for both logs and transfers
  4. rates.lookup_rates() for enriched shifts
  5. Apply all flags (flags.py)
  6. Aggregate expected pay per worker-month
  7. Aggregate paid per worker-month
  8. Join → delta, review_reason, needs_manual_review
  9. Write to SQLite + CSV

Run: python -m src.reconcile
"""

from __future__ import annotations

import csv
import logging
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd

from src import ingest, normalize, identity, rates, flags, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
CSV_OUT = OUTPUT_DIR / "final_reconciled.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_decimal(v: object) -> Decimal:
    """Convert any numeric-like to Decimal safely."""
    if isinstance(v, Decimal):
        return v
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return Decimal("0")
    return Decimal(str(v))


def _build_flag_evidence(shifts: pd.DataFrame, transfers: pd.DataFrame) -> list[dict]:
    """
    Build flag evidence rows for the flags table.

    Input : enriched shifts + transfers DataFrames with flag columns
    Output: list of {worker_id, flag, evidence} dicts
    """
    rows: list[dict] = []

    for flag_col, label in [
        ("flag_rate_overlap", "rate_overlap"),
        ("flag_timezone_boundary_risk", "timezone_boundary_risk"),
        ("flag_invalid_hours", "invalid_hours"),
        ("flag_backdated_crosses_cycle", "backdated_crosses_cycle"),
        ("flag_ambiguous_name_match", "ambiguous_name_match"),
        ("flag_rate_precision_anomaly", "rate_precision_anomaly"),
    ]:
        flagged = shifts[shifts.get(flag_col, pd.Series(False, index=shifts.index)).astype(bool)]
        for _, r in flagged.iterrows():
            rows.append({
                "worker_id": r.get("worker_id") or "UNMATCHED",
                "flag": label,
                "evidence": f"log_id={r['log_id']} work_date={r['canonical_work_date']}",
            })

    for flag_col, label in [
        ("flag_suspected_unit_error", "suspected_unit_error"),
        ("flag_ambiguous_name_match", "ambiguous_name_match"),
    ]:
        flagged = transfers[transfers.get(flag_col, pd.Series(False, index=transfers.index)).astype(bool)]
        for _, r in flagged.iterrows():
            rows.append({
                "worker_id": r.get("worker_id") or "UNMATCHED",
                "flag": label,
                "evidence": f"utr={r['utr']} amount_paise={r['amount_paise']}",
            })

    return rows


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(data_dir: Path = DATA_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    """
    Full reconciliation pipeline. Idempotent — re-running produces same output.

    Input : data_dir (Path to 4 CSVs), output_dir (Path for outputs)
    Output: None (side effects: reconciled.sqlite + final_reconciled.csv created)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Ingest ────────────────────────────────────────────────────
    logger.info("Step 1: Ingesting CSVs")
    loaded = ingest.load_all(data_dir)
    supervisor_logs = loaded["supervisor_logs"]
    bank_transfers = loaded["bank_transfers"]
    wage_rates = loaded["wage_rates"]
    workers = loaded["workers"]
    rejections = loaded["rejections"]
    logger.info("Rejections: %d", len(rejections))

    # ── Step 2: Normalize ─────────────────────────────────────────────────
    logger.info("Step 2: Normalizing timestamps + phones")
    supervisor_logs = normalize.enrich_supervisor_logs(supervisor_logs)
    bank_transfers = normalize.enrich_bank_transfers(bank_transfers)

    # ── Step 3: Identity resolution ───────────────────────────────────────
    logger.info("Step 3: Resolving identities")
    # Logs: we'll use phone first; fuzzy needs state+role from workers
    # Temporarily attach state/role from workers via phone lookup for fuzzy prefilter
    phone_idx = identity.build_phone_index(workers)
    worker_lookup = workers.set_index("worker_id")[["state", "role"]].to_dict("index")

    def enrich_for_fuzzy(row: pd.Series) -> pd.Series:
        """Pre-attach state/role to shift rows for fuzzy fallback."""
        phone = row.get("normalized_phone")
        if phone and phone in phone_idx:
            wid = phone_idx[phone]
            if wid in worker_lookup:
                row = row.copy()
                row["state"] = worker_lookup[wid]["state"]
                row["role"] = worker_lookup[wid]["role"]
        return row

    supervisor_logs = supervisor_logs.apply(enrich_for_fuzzy, axis=1)
    bank_transfers = bank_transfers.apply(enrich_for_fuzzy, axis=1)

    # Ensure state/role columns exist even if all unmatched
    for col in ["state", "role"]:
        if col not in supervisor_logs.columns:
            supervisor_logs[col] = ""
        if col not in bank_transfers.columns:
            bank_transfers[col] = ""

    supervisor_logs = identity.resolve_identities(supervisor_logs, workers)
    bank_transfers = identity.resolve_identities(bank_transfers, workers)

    # ── Step 4: Rate lookup ───────────────────────────────────────────────
    logger.info("Step 4: Effective-dated rate lookup")
    # Only include rows that have a worker_id for rate join
    matched_logs = supervisor_logs[supervisor_logs["worker_id"].notna()].copy()
    if matched_logs.empty:
        logger.warning("No matched logs for rate lookup!")
        shifts_with_rates = supervisor_logs.copy()
        for col in ["hourly_rate_inr", "expected_pay_paise", "rate_match_count",
                    "effective_from", "effective_to"]:
            shifts_with_rates[col] = None
    else:
        shifts_with_rates = rates.lookup_rates(matched_logs, workers, wage_rates)
        # Re-attach unmatched logs with null rates
        unmatched_logs = supervisor_logs[supervisor_logs["worker_id"].isna()].copy()
        for col in ["hourly_rate_inr", "expected_pay_paise", "rate_match_count",
                    "effective_from", "effective_to"]:
            unmatched_logs[col] = None
        shifts_with_rates = pd.concat([shifts_with_rates, unmatched_logs], ignore_index=True)

    # Fill nulls
    shifts_with_rates["rate_match_count"] = shifts_with_rates["rate_match_count"].fillna(0).astype(int)
    shifts_with_rates["expected_pay_paise"] = shifts_with_rates["expected_pay_paise"].apply(
        lambda v: _safe_decimal(v)
    )

    # ── Step 5: Apply flags ───────────────────────────────────────────────
    logger.info("Step 5: Applying flags")
    shifts_with_rates = flags.apply_rate_overlap_flag(shifts_with_rates)
    shifts_with_rates = flags.apply_tz_boundary_flag(shifts_with_rates, wage_rates)
    shifts_with_rates = flags.apply_invalid_hours_flag(shifts_with_rates)
    shifts_with_rates = flags.apply_backdated_flag(shifts_with_rates)
    shifts_with_rates = flags.apply_ambiguous_flag(shifts_with_rates)
    shifts_with_rates = flags.apply_rate_precision_flag(shifts_with_rates)

    # Shift review_reason
    shifts_with_rates["review_reason"] = shifts_with_rates.apply(
        lambda r: flags.build_review_reason(r, flags.SHIFT_FLAGS), axis=1
    )

    bank_transfers = flags.apply_unit_error_flag(bank_transfers)
    bank_transfers = flags.apply_correction_flag(bank_transfers)
    bank_transfers = flags.apply_ambiguous_flag(bank_transfers)
    bank_transfers["review_reason"] = bank_transfers.apply(
        lambda r: flags.build_review_reason(r, flags.TRANSFER_FLAGS), axis=1
    )

    # ── Step 6 & 7: Aggregate expected and paid per worker-month ──────────
    logger.info("Step 6-7: Aggregating per worker-month")

    # Add work_month column
    shifts_with_rates["work_month"] = shifts_with_rates["canonical_work_date"].apply(
        lambda d: str(d)[:7] if d else None
    )

    # Aggregate expected pay (exclude invalid_hours rows from sum)
    valid_shifts = shifts_with_rates[~shifts_with_rates["flag_invalid_hours"]].copy()
    expected_agg = (
        valid_shifts[valid_shifts["worker_id"].notna()]
        .groupby(["worker_id", "work_month"])["expected_pay_paise"]
        .apply(lambda vals: sum(vals, Decimal("0")))
        .reset_index()
        .rename(columns={"work_month": "period", "expected_pay_paise": "expected_paise"})
    )

    # Aggregate paid (use amount_paise from transfers, by worker_id + transfer_month)
    # Exclude suspected unit error from aggregation? No — flag only, use value as-is
    paid_agg = (
        bank_transfers[bank_transfers["worker_id"].notna()]
        .groupby(["worker_id", "transfer_month"])["amount_paise"]
        .apply(lambda vals: sum(vals, Decimal("0")))
        .reset_index()
        .rename(columns={"transfer_month": "period", "amount_paise": "paid_paise"})
    )

    # Full outer join on worker_id + period
    reconciled = expected_agg.merge(paid_agg, on=["worker_id", "period"], how="outer")
    reconciled["expected_paise"] = reconciled["expected_paise"].apply(
        lambda v: _safe_decimal(v) if v is not None else Decimal("0")
    )
    reconciled["paid_paise"] = reconciled["paid_paise"].apply(
        lambda v: _safe_decimal(v) if v is not None else Decimal("0")
    )
    reconciled["delta_paise"] = reconciled["paid_paise"] - reconciled["expected_paise"]

    # ── Step 8: Build review flags per reconciled row ─────────────────────
    logger.info("Step 8: Building reconciled flags")

    # Collect per-worker-period shift flags
    shift_flag_cols = [
        "flag_rate_overlap", "flag_timezone_boundary_risk", "flag_invalid_hours",
        "flag_backdated_crosses_cycle", "flag_ambiguous_name_match", "flag_rate_precision_anomaly",
    ]
    shift_flags_agg = (
        shifts_with_rates[shifts_with_rates["worker_id"].notna()]
        .groupby(["worker_id", "work_month"])[shift_flag_cols]
        .any()
        .reset_index()
        .rename(columns={"work_month": "period"})
    )

    # Collect transfer flags per worker-period
    transfer_flag_cols = ["flag_suspected_unit_error", "flag_ambiguous_name_match"]
    transfer_flags_agg = (
        bank_transfers[bank_transfers["worker_id"].notna()]
        .groupby(["worker_id", "transfer_month"])[transfer_flag_cols]
        .any()
        .reset_index()
        .rename(columns={"transfer_month": "period"})
    )

    reconciled = reconciled.merge(shift_flags_agg, on=["worker_id", "period"], how="left")
    reconciled = reconciled.merge(
        transfer_flags_agg, on=["worker_id", "period"], how="left", suffixes=("", "_t")
    )

    # Merge ambiguous from both sources
    if "flag_ambiguous_name_match_t" in reconciled.columns:
        reconciled["flag_ambiguous_name_match"] = (
            reconciled["flag_ambiguous_name_match"].fillna(False)
            | reconciled["flag_ambiguous_name_match_t"].fillna(False)
        )
        reconciled.drop(columns=["flag_ambiguous_name_match_t"], inplace=True)

    # Fill NaN flags
    for col in shift_flag_cols + transfer_flag_cols:
        if col in reconciled.columns:
            reconciled[col] = reconciled[col].fillna(False)

    # no_matching_transfer flag
    reconciled["flag_no_matching_transfer"] = reconciled.apply(
        lambda r: flags.flag_no_matching_transfer(r["expected_paise"], r["paid_paise"]),
        axis=1,
    )

    # Build review_reason: combine all flags
    all_recon_flags = [
        ("suspected_unit_error", "flag_suspected_unit_error"),
        ("rate_overlap", "flag_rate_overlap"),
        ("timezone_boundary_risk", "flag_timezone_boundary_risk"),
        ("invalid_hours", "flag_invalid_hours"),
        ("backdated_crosses_cycle", "flag_backdated_crosses_cycle"),
        ("no_matching_transfer", "flag_no_matching_transfer"),
        ("ambiguous_name_match", "flag_ambiguous_name_match"),
        ("rate_precision_anomaly", "flag_rate_precision_anomaly"),
    ]
    reconciled["review_reason"] = reconciled.apply(
        lambda r: flags.build_review_reason(r, all_recon_flags), axis=1
    )
    
    # Ensure discrepancies without specific flags are still marked and explainable
    def _finalize_review(r):
        reason = r["review_reason"]
        if r["delta_paise"] != 0 and not reason:
            return "unexplained_discrepancy"
        return reason

    reconciled["review_reason"] = reconciled.apply(_finalize_review, axis=1)
    
    # Needs review if there's any reason (including unexplained_discrepancy)
    reconciled["needs_manual_review"] = reconciled["review_reason"].apply(bool)

    # ── Step 9: Write to SQLite ───────────────────────────────────────────
    logger.info("Step 9: Writing to SQLite")
    db_path = output_dir / "reconciled.sqlite"
    conn = db.get_sqlite_conn(db_path)
    db.create_schema(conn)
    db.write_shifts(conn, shifts_with_rates)
    db.write_transfers(conn, bank_transfers)
    db.write_reconciled(conn, reconciled)
    db.write_workers(conn, workers)

    flag_evidence = _build_flag_evidence(shifts_with_rates, bank_transfers)
    db.write_flags(conn, flag_evidence)
    conn.close()

    # ── Step 10: Write CSV ────────────────────────────────────────────────
    logger.info("Step 10: Writing final_reconciled.csv")
    csv_cols = [
        "worker_id", "period", "expected_paise", "paid_paise", "delta_paise",
        "needs_manual_review", "review_reason",
    ]
    reconciled[csv_cols].to_csv(CSV_OUT, index=False)

    # ── Summary ───────────────────────────────────────────────────────────
    underpaid = reconciled[reconciled["delta_paise"] < Decimal("0")]
    overpaid = reconciled[reconciled["delta_paise"] > Decimal("0")]
    unit_err = bank_transfers["flag_suspected_unit_error"].sum()

    logger.info("=" * 60)
    logger.info("RECONCILIATION COMPLETE")
    logger.info("  Workers reconciled:   %d", reconciled["worker_id"].nunique())
    logger.info("  Worker-month rows:    %d", len(reconciled))
    logger.info("  Underpaid rows:       %d", len(underpaid))
    logger.info("  Overpaid rows:        %d", len(overpaid))
    logger.info("  Unit error transfers: %d", unit_err)
    logger.info("  Output: %s", db_path)
    logger.info("  Output: %s", CSV_OUT)
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
