"""
api/main.py — FastAPI serving reconciled.sqlite.

Endpoints:
  GET /api/reconciled     — paginated, filtered reconciled rows
  GET /api/workers/{id}   — full worker profile + shifts + transfers + monthly recon
  GET /api/summary        — aggregate stats + flag breakdown
  GET /api/export         — streaming CSV with same filters as /api/reconciled

Uses sync sqlite3 (not aiosqlite). FastAPI handles sync routes via threadpool.
Money fields returned as strings (from Decimal-stored integers).
"""

from __future__ import annotations

import csv
import io
import logging
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, Generator

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)

DB_PATH = Path("output/reconciled.sqlite")

app = FastAPI(
    title="Bharat Intelligence Wage Reconciliation API",
    description="Reconciliation pipeline for 12,000 rural field workers.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:*", "https://*.lovable.app", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    """
    Open sync sqlite3 connection to reconciled.sqlite.

    Raises 503 if DB not yet generated.
    """
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Database not yet generated. Run: python -m src.reconcile",
        )
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def paise_to_inr_str(paise: int | None) -> str:
    """Convert paise integer to INR Decimal string with 2dp."""
    if paise is None:
        return "0.00"
    d = Decimal(str(paise)) / Decimal("100")
    return str(d.quantize(Decimal("0.01")))


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class ReconciledItem(BaseModel):
    worker_id: str
    worker_name: str | None = None
    state: str | None = None
    role: str | None = None
    period: str
    expected_paise: int
    paid_paise: int
    delta_paise: int
    needs_manual_review: bool
    review_reason: str
    flag_no_matching_transfer: bool

    # Convenience INR strings
    expected_inr: str
    paid_inr: str
    delta_inr: str


class PaginatedReconciled(BaseModel):
    items: list[ReconciledItem]
    total: int
    page: int
    page_size: int


class WorkerShift(BaseModel):
    log_id: str
    canonical_work_date: str | None
    hours: str | None
    vendor_app: str | None
    entered_at_ist: str | None
    entry_lag_days: int | None
    expected_pay_paise: int | None
    review_reason: str | None
    match_method: str | None


class WorkerTransfer(BaseModel):
    utr: str
    amount_paise: int
    transfer_ist: str | None
    transfer_month: str | None
    review_reason: str | None


class MonthlyRecon(BaseModel):
    period: str
    expected_paise: int
    paid_paise: int
    delta_paise: int
    needs_manual_review: bool
    review_reason: str


class WorkerProfile(BaseModel):
    worker: dict[str, Any]
    shifts: list[WorkerShift]
    transfers: list[WorkerTransfer]
    monthly_reconciliation: list[MonthlyRecon]


class SummaryResponse(BaseModel):
    total_workers_reconciled: int
    workers_owed_money: int
    workers_overpaid: int
    total_underpaid_inr: str
    total_overpaid_inr: str
    flag_breakdown: dict[str, int]
    confidence_levels: dict[str, str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/reconciled", response_model=PaginatedReconciled)
def get_reconciled(
    state: str | None = Query(None),
    role: str | None = Query(None),
    period: str | None = Query(None, description="YYYY-MM"),
    needs_review: bool | None = Query(None),
    delta_sign: str | None = Query(None, pattern="^(neg|pos|zero)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
) -> PaginatedReconciled:
    """
    Return paginated reconciled worker-month rows with optional filters.

    Filters: state, role, period (YYYY-MM), needs_review, delta_sign (neg/pos/zero).
    """
    conn = get_db()
    try:
        # Build WHERE clauses
        clauses: list[str] = []
        params: list[Any] = []

        # Join workers to fetch name, state, and role
        join = "LEFT JOIN workers w ON r.worker_id = w.worker_id"
        if state:
            clauses.append("w.state = ?")
            params.append(state)
        if role:
            clauses.append("w.role = ?")
            params.append(role)

        if period:
            clauses.append("r.period = ?")
            params.append(period)
        if needs_review is not None:
            clauses.append("r.needs_manual_review = ?")
            params.append(1 if needs_review else 0)
        if delta_sign == "neg":
            clauses.append("r.delta_paise < 0")
        elif delta_sign == "pos":
            clauses.append("r.delta_paise > 0")
        elif delta_sign == "zero":
            clauses.append("r.delta_paise = 0")

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        count_sql = f"SELECT COUNT(*) FROM reconciled r {join} {where}"
        total = conn.execute(count_sql, params).fetchone()[0]

        offset = (page - 1) * page_size
        data_sql = f"""
            SELECT r.*, w.name as worker_name, w.state, w.role 
            FROM reconciled r {join} {where}
            ORDER BY r.worker_id, r.period
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(data_sql, params + [page_size, offset]).fetchall()

        items = []
        for row in rows:
            d = dict(row)
            items.append(
                ReconciledItem(
                    worker_id=d["worker_id"],
                    worker_name=d.get("worker_name"),
                    state=d.get("state"),
                    role=d.get("role"),
                    period=d["period"],
                    expected_paise=d["expected_paise"],
                    paid_paise=d["paid_paise"],
                    delta_paise=d["delta_paise"],
                    needs_manual_review=bool(d["needs_manual_review"]),
                    review_reason=d["review_reason"] or "",
                    flag_no_matching_transfer=bool(d.get("flag_no_matching_transfer", 0)),
                    expected_inr=paise_to_inr_str(d["expected_paise"]),
                    paid_inr=paise_to_inr_str(d["paid_paise"]),
                    delta_inr=paise_to_inr_str(d["delta_paise"]),
                )
            )
        return PaginatedReconciled(items=items, total=total, page=page, page_size=page_size)
    finally:
        conn.close()


@app.get("/api/workers/{worker_id}", response_model=WorkerProfile)
def get_worker(worker_id: str) -> WorkerProfile:
    """
    Return full worker profile: canonical data + all shifts + transfers + monthly recon.
    """
    conn = get_db()
    try:
        worker_rows = conn.execute(
            "SELECT * FROM shifts WHERE worker_id = ? LIMIT 1", [worker_id]
        ).fetchall()
        if not worker_rows:
            raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

        worker_dict: dict[str, Any] = {"worker_id": worker_id}

        shifts_raw = conn.execute(
            "SELECT * FROM shifts WHERE worker_id = ? ORDER BY canonical_work_date",
            [worker_id],
        ).fetchall()
        shifts_out = [
            WorkerShift(
                log_id=dict(r)["log_id"],
                canonical_work_date=dict(r).get("canonical_work_date"),
                hours=dict(r).get("hours"),
                vendor_app=dict(r).get("vendor_app"),
                entered_at_ist=dict(r).get("entered_at_ist"),
                entry_lag_days=dict(r).get("entry_lag_days"),
                expected_pay_paise=dict(r).get("expected_pay_paise"),
                review_reason=dict(r).get("review_reason"),
                match_method=dict(r).get("match_method"),
            )
            for r in shifts_raw
        ]

        transfers_raw = conn.execute(
            "SELECT * FROM transfers WHERE worker_id = ? ORDER BY transfer_ist",
            [worker_id],
        ).fetchall()
        transfers_out = [
            WorkerTransfer(
                utr=dict(r)["utr"],
                amount_paise=dict(r)["amount_paise"],
                transfer_ist=dict(r).get("transfer_ist"),
                transfer_month=dict(r).get("transfer_month"),
                review_reason=dict(r).get("review_reason"),
            )
            for r in transfers_raw
        ]

        recon_raw = conn.execute(
            "SELECT * FROM reconciled WHERE worker_id = ? ORDER BY period",
            [worker_id],
        ).fetchall()
        monthly_out = [
            MonthlyRecon(
                period=dict(r)["period"],
                expected_paise=dict(r)["expected_paise"],
                paid_paise=dict(r)["paid_paise"],
                delta_paise=dict(r)["delta_paise"],
                needs_manual_review=bool(dict(r)["needs_manual_review"]),
                review_reason=dict(r)["review_reason"] or "",
            )
            for r in recon_raw
        ]

        return WorkerProfile(
            worker=worker_dict,
            shifts=shifts_out,
            transfers=transfers_out,
            monthly_reconciliation=monthly_out,
        )
    finally:
        conn.close()


@app.get("/api/summary")
def get_summary() -> dict:
    """
    Return aggregate stats and flag breakdown.
    """
    conn = get_db()
    try:
        total_workers = conn.execute(
            "SELECT COUNT(DISTINCT worker_id) FROM reconciled"
        ).fetchone()[0]

        workers_owed = conn.execute(
            "SELECT COUNT(DISTINCT worker_id) FROM reconciled WHERE delta_paise < 0"
        ).fetchone()[0]

        workers_over = conn.execute(
            "SELECT COUNT(DISTINCT worker_id) FROM reconciled WHERE delta_paise > 0"
        ).fetchone()[0]

        total_under_paise = conn.execute(
            "SELECT COALESCE(SUM(ABS(delta_paise)), 0) FROM reconciled WHERE delta_paise < 0"
        ).fetchone()[0]

        total_over_paise = conn.execute(
            "SELECT COALESCE(SUM(delta_paise), 0) FROM reconciled WHERE delta_paise > 0"
        ).fetchone()[0]

        flag_rows = conn.execute(
            "SELECT flag, COUNT(*) as cnt FROM flags GROUP BY flag"
        ).fetchall()
        flag_breakdown = {r["flag"]: r["cnt"] for r in flag_rows}

        confidence_map = {
            "suspected_unit_error": "HIGH",
            "suspected_correction": "MEDIUM",
            "rate_overlap": "HIGH",
            "timezone_boundary_risk": "MEDIUM",
            "invalid_hours": "HIGH",
            "backdated_crosses_cycle": "HIGH",
            "no_matching_transfer": "MEDIUM",
            "ambiguous_name_match": "MEDIUM",
            "rate_precision_anomaly": "HIGH",
            "unexplained_discrepancy": "LOW",
        }

        # Delta Distribution Buckets
        dist_sql = """
            SELECT 
                CASE 
                    WHEN delta_paise < -100000 THEN '< -₹1000'
                    WHEN delta_paise >= -100000 AND delta_paise < -10000 THEN '-₹1000 to -₹100'
                    WHEN delta_paise >= -10000 AND delta_paise < 0 THEN '-₹100 to <₹0'
                    WHEN delta_paise = 0 THEN 'Exact Match (₹0)'
                    WHEN delta_paise > 0 AND delta_paise <= 10000 THEN '>₹0 to ₹100'
                    WHEN delta_paise > 10000 AND delta_paise <= 100000 THEN '₹100 to ₹1000'
                    ELSE '> ₹1000'
                END as bucket,
                COUNT(*) as cnt
            FROM reconciled
            GROUP BY 1
            ORDER BY MIN(delta_paise)
        """
        dist_rows = conn.execute(dist_sql).fetchall()
        # The frontend might expect an array for Recharts, or a dict. I'll provide an array.
        delta_distribution = [{"bucket": r["bucket"], "count": r["cnt"]} for r in dist_rows]

        return {
            "total_workers_reconciled": total_workers,
            "workers_owed_money": workers_owed,
            "workers_overpaid": workers_over,
            "total_underpaid_inr": paise_to_inr_str(total_under_paise),
            "total_overpaid_inr": paise_to_inr_str(total_over_paise),
            "flag_breakdown": flag_breakdown,
            "confidence_levels": confidence_map,
            "delta_distribution": delta_distribution,
        }
    finally:
        conn.close()



@app.get("/api/export")
def export_csv(
    state: str | None = Query(None),
    role: str | None = Query(None),
    period: str | None = Query(None),
    needs_review: bool | None = Query(None),
    delta_sign: str | None = Query(None),
) -> StreamingResponse:
    """
    Stream filtered reconciled rows as CSV. Uses csv module + generator, not pandas.
    """
    def generate() -> Generator[bytes, None, None]:
        conn = get_db()
        try:
            clauses: list[str] = []
            params: list[Any] = []
            join = ""
            if state or role:
                join = "JOIN (SELECT worker_id, state, role FROM shifts GROUP BY worker_id) w ON r.worker_id = w.worker_id"
                if state:
                    clauses.append("w.state = ?")
                    params.append(state)
                if role:
                    clauses.append("w.role = ?")
                    params.append(role)
            if period:
                clauses.append("r.period = ?")
                params.append(period)
            if needs_review is not None:
                clauses.append("r.needs_manual_review = ?")
                params.append(1 if needs_review else 0)
            if delta_sign == "neg":
                clauses.append("r.delta_paise < 0")
            elif delta_sign == "pos":
                clauses.append("r.delta_paise > 0")
            elif delta_sign == "zero":
                clauses.append("r.delta_paise = 0")

            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            sql = f"SELECT r.* FROM reconciled r {join} {where} ORDER BY r.worker_id, r.period"

            buf = io.StringIO()
            writer = csv.writer(buf)

            first = True
            for row in conn.execute(sql, params):
                d = dict(row)
                if first:
                    # Add INR columns
                    header = list(d.keys()) + ["expected_inr", "paid_inr", "delta_inr"]
                    writer.writerow(header)
                    first = False
                    yield buf.getvalue().encode()
                    buf.seek(0)
                    buf.truncate()

                d["expected_inr"] = paise_to_inr_str(d["expected_paise"])
                d["paid_inr"] = paise_to_inr_str(d["paid_paise"])
                d["delta_inr"] = paise_to_inr_str(d["delta_paise"])
                writer.writerow(list(d.values()))
                yield buf.getvalue().encode()
                buf.seek(0)
                buf.truncate()
        finally:
            conn.close()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=reconciled_export.csv"},
    )
