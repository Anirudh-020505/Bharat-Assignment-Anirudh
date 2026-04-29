"""
ingest.py — Load all 4 CSVs, validate each row against pydantic models.
Input : data/ directory path
Output: (supervisor_logs_df, bank_transfers_df, wage_rates_df, workers_df, rejections_list)
Side effects: none (read-only)
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SupervisorLog(BaseModel):
    log_id: str
    worker_name: str
    worker_phone: str
    supervisor_id: str
    work_date: date
    hours: Decimal
    vendor_app: str
    entered_at: str  # kept as raw string; normalised in normalize.py

    @field_validator("work_date", mode="before")
    @classmethod
    def parse_work_date(cls, v: Any) -> date:
        """Accept ISO date strings and date objects."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(str(v).strip())

    @field_validator("hours", mode="before")
    @classmethod
    def coerce_hours(cls, v: Any) -> Decimal:
        """Coerce numeric-like strings to Decimal."""
        return Decimal(str(v))


class BankTransfer(BaseModel):
    utr: str
    worker_phone: str
    worker_name: str
    amount_paise: Decimal
    transfer_timestamp: str  # raw string; TZ-normalised in normalize.py
    account_last4: str

    @field_validator("amount_paise", mode="before")
    @classmethod
    def coerce_paise(cls, v: Any) -> Decimal:
        return Decimal(str(v))

    @field_validator("account_last4", mode="before")
    @classmethod
    def coerce_last4(cls, v: Any) -> str:
        return str(v).strip().zfill(4)


class WageRate(BaseModel):
    role: str
    state: str
    effective_from: date
    effective_to: date | None
    hourly_rate_inr: Decimal
    seniority: str

    @field_validator("effective_from", mode="before")
    @classmethod
    def parse_eff_from(cls, v: Any) -> date:
        return date.fromisoformat(str(v).strip())

    @field_validator("effective_to", mode="before")
    @classmethod
    def parse_eff_to(cls, v: Any) -> date | None:
        if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
            return None
        return date.fromisoformat(str(v).strip())

    @field_validator("hourly_rate_inr", mode="before")
    @classmethod
    def coerce_rate(cls, v: Any) -> Decimal:
        return Decimal(str(v))


class Worker(BaseModel):
    worker_id: str
    name: str
    phone: str
    state: str
    role: str
    seniority: str
    registered_on: date

    @field_validator("registered_on", mode="before")
    @classmethod
    def parse_reg_date(cls, v: Any) -> date:
        return date.fromisoformat(str(v).strip())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_rows(
    df: pd.DataFrame,
    model: type[BaseModel],
    source_name: str,
) -> tuple[list[dict], list[dict]]:
    """
    Validate each row of *df* against *model*.

    Returns:
        valid_rows   — list of model.model_dump() dicts
        rejections   — list of {source, row_index, row_data, reason}
    """
    valid_rows: list[dict] = []
    rejections: list[dict] = []
    for idx, row in df.iterrows():
        raw = row.where(pd.notna(row), None).to_dict()
        try:
            validated = model(**raw)
            valid_rows.append(validated.model_dump())
        except (ValidationError, Exception) as exc:
            rejections.append(
                {
                    "source": source_name,
                    "row_index": idx,
                    "row_data": raw,
                    "reason": str(exc),
                }
            )
    return valid_rows, rejections


def load_all(data_dir: str | Path = "data") -> dict:
    """
    Load all 4 CSVs from *data_dir*, validate with pydantic, return dict.

    Returns:
        {
          "supervisor_logs": pd.DataFrame,
          "bank_transfers":  pd.DataFrame,
          "wage_rates":      pd.DataFrame,
          "workers":         pd.DataFrame,
          "rejections":      list[dict],
        }
    """
    data_dir = Path(data_dir)
    rejections: list[dict] = []

    # ---- supervisor_logs ------------------------------------------------
    sl_raw = pd.read_csv(
        data_dir / "supervisor_logs.csv",
        dtype=str,
        keep_default_na=False,
    )
    sl_raw.columns = [c.strip() for c in sl_raw.columns]
    sl_valid, sl_rej = _validate_rows(sl_raw, SupervisorLog, "supervisor_logs")
    rejections.extend(sl_rej)
    supervisor_logs = pd.DataFrame(sl_valid)

    # ---- bank_transfers -------------------------------------------------
    bt_raw = pd.read_csv(
        data_dir / "bank_transfers.csv",
        dtype=str,
        keep_default_na=False,
    )
    bt_raw.columns = [c.strip() for c in bt_raw.columns]
    bt_valid, bt_rej = _validate_rows(bt_raw, BankTransfer, "bank_transfers")
    rejections.extend(bt_rej)
    bank_transfers = pd.DataFrame(bt_valid)

    # ---- wage_rates -----------------------------------------------------
    wr_raw = pd.read_csv(
        data_dir / "wage_rates.csv",
        dtype=str,
        keep_default_na=False,
    )
    wr_raw.columns = [c.strip() for c in wr_raw.columns]
    wr_valid, wr_rej = _validate_rows(wr_raw, WageRate, "wage_rates")
    rejections.extend(wr_rej)
    wage_rates = pd.DataFrame(wr_valid)

    # ---- workers --------------------------------------------------------
    w_raw = pd.read_csv(
        data_dir / "workers.csv",
        dtype=str,
        keep_default_na=False,
    )
    w_raw.columns = [c.strip() for c in w_raw.columns]
    w_valid, w_rej = _validate_rows(w_raw, Worker, "workers")
    rejections.extend(w_rej)
    workers = pd.DataFrame(w_valid)

    logger.info(
        "Loaded rows — logs:%d  transfers:%d  rates:%d  workers:%d  rejections:%d",
        len(supervisor_logs),
        len(bank_transfers),
        len(wage_rates),
        len(workers),
        len(rejections),
    )

    return {
        "supervisor_logs": supervisor_logs,
        "bank_transfers": bank_transfers,
        "wage_rates": wage_rates,
        "workers": workers,
        "rejections": rejections,
    }
