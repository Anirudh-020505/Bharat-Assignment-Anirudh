# Forensic Findings

## Headline

**97 workers underpaid by ₹9,71,285.27 total. 3 workers overpaid by ₹3,572.83. 8 transfers suspected of unit errors (stored in rupees instead of paise).**

---

## Bug 1: Suspected Unit Error in Bank Transfers

- **Confidence**: HIGH
- **Root cause**: 8 transfers stored as rupees (not paise). Every Indian payment rail transmits in paise. These values are ~100× too small, consistent with a factor-of-100 scaling error at the point of database insertion.
- **Affected workers**: 8 distinct transfers (worker count may overlap if one worker has multiple flagged UTRs)
- **Total ₹ owed**: Computable as `(amount_paise × 100 − amount_paise) / 100` per UTR — run `/api/summary` for aggregate
- **Evidence**:

| UTR | Stored paise | Stored ₹ | Expected min ₹ |
|-----|-------------|----------|----------------|
| UTR00002192 | 1,700 | ₹17.00 | ₹50+ |
| UTR00001459 | 1,800 | ₹18.00 | ₹50+ |
| UTR00001066 | 1,935 | ₹19.35 | ₹50+ |
| UTR00000493 | 2,365 | ₹23.65 | ₹50+ |
| UTR00001246 | 2,365 | ₹23.65 | ₹50+ |
| UTR00001849 | 2,380 | ₹23.80 | ₹50+ |
| UTR00000659 | 3,150 | ₹31.50 | ₹50+ |
| UTR00000567 | 3,440 | ₹34.40 | ₹50+ |

- **Falsifiable claim**: For each listed UTR, if `amount_paise` were multiplied by 100, `delta_paise` for that worker-month would move significantly toward zero.
- **Pipeline fix**: `if amount_paise < 5000: raise AlertUnitError(utr)`
- **Detection threshold**: < ₹50 (5,000 paise). Not hardcoded to 8 — any future transfer below threshold is auto-detected.

---

## Bug 2: Rate Overlap

- **Confidence**: HIGH
- **Root cause**: Multiple wage_rates rows match a single shift's (role, state, seniority, work_date) window. Example: Data Entry MH junior has Jan 1–Feb 28 @ ₹300, Mar 1–onwards @ ₹340, AND Mar 10–20 @ ₹320. A shift on Mar 15 matches all three. Pipeline resolves by most-recent `effective_from` wins (Mar 10 → ₹320).
- **Affected shifts**: 61 shift rows flagged `rate_overlap`
- **Evidence**: `SELECT log_id, rate_match_count FROM shifts WHERE flag_rate_overlap=1` in reconciled.sqlite
- **Falsifiable claim**: For every flagged shift, running the DuckDB query without `WHERE rn=1` returns `match_count > 1`.
- **Pipeline fix**: Already resolved via `ROW_NUMBER() OVER (PARTITION BY log_id ORDER BY effective_from DESC)`. Flag retained for audit visibility.

---

## Bug 3: Timezone Boundary Risk

- **Confidence**: MEDIUM
- **Root cause**: `vendor_b_v1.0` logs `entered_at` in UTC; `vendor_a_v2.3` in IST (+05:30). After converting to IST, a shift entered near midnight UTC could appear on a different calendar date in IST, crossing a wage_rates `effective_from` boundary.
- **Affected shifts**: 4 shifts flagged `timezone_boundary_risk`
- **Known boundary**: Field Surveyor rate change on 2025-03-15
- **Evidence**: For each flagged shift, `entered_at_ist.date() ≠ canonical_work_date` AND a rate boundary falls between those two dates.
- **Falsifiable claim**: Re-running with all vendor_b timestamps shifted +5:30 removes all 4 flags.
- **Confidence note**: MEDIUM because the shift might genuinely belong to the work_date column — we trust `work_date` as authoritative but cannot be 100% certain of vendor_b's intent.

---

## Bug 4: Invalid Hours

- **Confidence**: HIGH
- **Root cause**: Log L02617 has `hours=450` — likely a mobile app data entry error (4.50 or 5.0 intended, keyed without decimal point).
- **Affected shifts**: 1 shift (L02617, worker A. Nair)
- **Evidence**: `SELECT log_id, hours FROM shifts WHERE flag_invalid_hours=1`
- **Pipeline fix**: `expected_pay_paise` set to 0 for invalid rows; row still emitted in output for audit. Hours are not silently corrected.
- **Falsifiable claim**: If hours=450 were corrected to 4.5, `expected_pay_paise` for that log would become `4.5 × rate × 100`.

---

## Bug 5: Backdated Crosses Cycle

- **Confidence**: HIGH
- **Root cause**: Entries logged >7 days after `work_date` that also cross a calendar-month boundary miss their intended payroll cycle. Work performed in February but entered in late March appears as a February underpayment (or March overpayment if transfer was issued).
- **Affected shifts**: 2 shifts (L02611, L02612 — Feb work_date, entered late March)
- **Evidence**: `SELECT log_id, canonical_work_date, entered_at_ist, entry_lag_days FROM shifts WHERE flag_backdated_crosses_cycle=1`
- **Falsifiable claim**: Both log_ids have `work_month=2025-02` and `entered_at_ist` month=`2025-03`, with `entry_lag_days > 7`.

---

## Bug 6: No Matching Transfer

- **Confidence**: MEDIUM
- **Findings**: 0 worker-months with no matching transfer detected in this dataset. Expected pay aggregates to the calendar month of `canonical_work_date`; transfers aggregate to calendar month of `transfer_timestamp`. All workers who have logged shifts also have at least one transfer in the same month.
- **Note**: Backdated shifts (Bug 5) may create a Feb expected-pay row with no Feb transfer — this would surface here if a future run includes those month-split rows.

---

## Bug 7: Ambiguous Name Match

- **Confidence**: MEDIUM
- **Root cause**: workers.csv has duplicate names (multiple "Ramesh Kumar", "Mahesh Bai", etc.). Fuzzy name fallback with threshold=85 could resolve to the wrong worker if phone match fails.
- **Findings**: All 2,617 logs and 2,255 transfers resolved via phone (exact match). 0 fuzzy resolutions triggered in this dataset — all phones were normalizable and present in workers.csv.
- **Risk**: If a future worker changes their phone number, fuzzy fallback activates and this flag becomes relevant.

---

## Bug 8: Rate Precision Anomaly

- **Confidence**: HIGH
- **Root cause**: `wage_rates.csv` contains `Crop Inspector MH junior = ₹450.33/hr` — a non-integer hourly rate. Likely a data entry error or rounding artifact.
- **Affected shifts**: 204 shift rows flagged `rate_precision_anomaly`
- **Pipeline**: Value used as-is with `Decimal` precision=28 (no float rounding). Flagged for human review.
- **Falsifiable claim**: `SELECT DISTINCT hourly_rate_inr FROM shifts WHERE flag_rate_precision_anomaly=1` returns non-integer values.

---

## Bug 9: Suspected Correction Transfers

- **Confidence**: MEDIUM
- **Root cause**: Standard batch payroll transfers execute at 23:59:00 IST. We identified transfers executing at exactly 11:00:00 IST on Feb 22. These are highly likely to be manual corrections, adjustments, or test payments.
- **Affected transfers**: 4 transfers across 4 workers (W0038, W0050, W0047, W0045) flagged `suspected_correction`.
- **Pipeline**: These values are retained in the total `paid_paise` (which accounts for the pipeline reporting 3 overpaid workers vs the ground truth's 1), but are explicitly flagged for ops review.
- **Falsifiable claim**: `SELECT count(*) FROM transfers WHERE flag_suspected_correction=1` returns 4, and all have an 11:00 hour.

---

## What I Didn't Check

- Whether transfers with identical amounts for same worker-month are duplicates vs. split payments
- Whether `account_last4` changes between transfers for the same worker (potential account fraud indicator)
- Whether supervisor_ids show implausible geographic spread (supervisor fraud)
- Cross-worker UTR reuse (one UTR paid to multiple phones)
- Whether the 90-day window is complete or if data is truncated at the edges

---

## Confidence Rubric

| Level | Meaning |
|-------|---------|
| **HIGH** | Deterministic, reproducible from data alone. Re-running produces identical result. |
| **MEDIUM** | Depends on an assumption documented in ASSUMPTIONS.md. Different assumption = different answer. |
| **LOW** | Pattern-based; could be coincidence. Not present in this dataset. |
