"""
identity.py — Worker identity resolution.

Primary: normalized phone → workers.worker_id (exact match).
Fallback: rapidfuzz fuzzy name match, pre-filtered by state AND role,
          threshold=85, picks highest score.

Input : enriched supervisor_logs/bank_transfers DataFrames + workers DataFrame
Output: same DataFrames with added columns:
          worker_id, match_method, match_score,
          ambiguous_candidates (JSON str or None)
"""

from __future__ import annotations

import json
import logging

import pandas as pd
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = 85


# ---------------------------------------------------------------------------
# Build lookup indexes from workers
# ---------------------------------------------------------------------------

def build_phone_index(workers: pd.DataFrame) -> dict[str, str]:
    """
    Map normalized_phone → worker_id (exact, unique).

    If multiple workers share a phone (data quality issue), first encountered wins.

    Input : workers DataFrame with columns [worker_id, phone]
    Output: dict {10-digit-phone: worker_id}
    """
    from src.normalize import normalize_phone

    index: dict[str, str] = {}
    for _, row in workers.iterrows():
        phone = normalize_phone(str(row["phone"]))
        if phone and phone not in index:
            index[phone] = row["worker_id"]
    return index


def _fuzzy_candidates(
    name: str,
    state: str,
    role: str,
    workers: pd.DataFrame,
    threshold: int = FUZZY_THRESHOLD,
) -> list[dict]:
    """
    Find workers matching state+role, score names with token_sort_ratio.

    Pre-filter by state AND role to reduce O(N²) → O(N/k).

    Input : name, state, role (from shift/transfer row), workers DataFrame
    Output: list of {worker_id, name, score} above threshold, sorted desc
    """
    subset = workers[
        (workers["state"] == state) & (workers["role"] == role)
    ]
    results = []
    for _, w in subset.iterrows():
        score = fuzz.token_sort_ratio(name.upper(), str(w["name"]).upper())
        if score >= threshold:
            results.append(
                {"worker_id": w["worker_id"], "name": w["name"], "score": score}
            )
    return sorted(results, key=lambda x: x["score"], reverse=True)


# ---------------------------------------------------------------------------
# Resolve a single row
# ---------------------------------------------------------------------------

def resolve_row(
    normalized_phone: str | None,
    worker_name: str,
    state: str,
    role: str,
    phone_index: dict[str, str],
    workers: pd.DataFrame,
) -> dict:
    """
    Resolve a single log/transfer row to a worker_id.

    Returns dict with:
      worker_id           : str | None
      match_method        : "phone" | "fuzzy_name" | "unmatched"
      match_score         : float | None
      ambiguous_candidates: JSON string of candidates list | None
    """
    # --- phone primary match ---
    if normalized_phone and normalized_phone in phone_index:
        return {
            "worker_id": phone_index[normalized_phone],
            "match_method": "phone",
            "match_score": 100.0,
            "ambiguous_candidates": None,
        }

    # --- fuzzy fallback ---
    candidates = _fuzzy_candidates(worker_name, state, role, workers)

    if not candidates:
        return {
            "worker_id": None,
            "match_method": "unmatched",
            "match_score": None,
            "ambiguous_candidates": None,
        }

    best = candidates[0]
    ambiguous = None
    if len(candidates) > 1:
        ambiguous = json.dumps([c for c in candidates[:5]])  # cap at 5

    return {
        "worker_id": best["worker_id"],
        "match_method": "fuzzy_name",
        "match_score": float(best["score"]),
        "ambiguous_candidates": ambiguous,
    }


# ---------------------------------------------------------------------------
# Batch resolve — enrich full DataFrame
# ---------------------------------------------------------------------------

def resolve_identities(
    df: pd.DataFrame,
    workers: pd.DataFrame,
    phone_col: str = "normalized_phone",
    name_col: str = "worker_name",
) -> pd.DataFrame:
    """
    Apply identity resolution to every row of *df*.

    Adds columns: worker_id, match_method, match_score, ambiguous_candidates.

    Input : df (supervisor_logs or bank_transfers, enriched with normalized_phone)
            workers DataFrame with [worker_id, name, phone, state, role]
    Output: df copy with 4 extra columns
    """
    phone_index = build_phone_index(workers)
    df = df.copy()

    results = []
    for _, row in df.iterrows():
        phone = row.get(phone_col)
        name = str(row.get(name_col, ""))

        # For logs, we can use state/role from the resolved worker if available.
        # Since logs don't carry state/role directly, we attempt phone first;
        # for fuzzy fallback we need state/role — we'll leave blanks produce "unmatched".
        state = str(row.get("state", ""))
        role = str(row.get("role", ""))

        # If phone match succeeds, we can back-fill state/role from workers
        if phone and phone in phone_index:
            wid = phone_index[phone]
            wrow = workers[workers["worker_id"] == wid]
            if not wrow.empty:
                state = wrow.iloc[0]["state"]
                role = wrow.iloc[0]["role"]

        res = resolve_row(phone, name, state, role, phone_index, workers)
        results.append(res)

    res_df = pd.DataFrame(results, index=df.index)
    df["worker_id"] = res_df["worker_id"]
    df["match_method"] = res_df["match_method"]
    df["match_score"] = res_df["match_score"]
    df["ambiguous_candidates"] = res_df["ambiguous_candidates"]

    unmatched = (df["match_method"] == "unmatched").sum()
    fuzzy = (df["match_method"] == "fuzzy_name").sum()
    logger.info(
        "Identity resolution: phone=%d  fuzzy=%d  unmatched=%d",
        (df["match_method"] == "phone").sum(),
        fuzzy,
        unmatched,
    )
    return df
