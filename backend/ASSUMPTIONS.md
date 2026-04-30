# Assumptions

Ranked by blast radius (highest impact if wrong, first).

---

## 1. Rate Overlap Tiebreaker: Most Recent `effective_from` Wins

**Blast radius**: HIGH — changes which rate applies to every overlapping shift.

**Assumption**: When multiple wage_rates rows match a shift's (role, state, seniority, work_date), the row with the most recent `effective_from` is the correct rate to apply.

**Known case**: Data Entry MH junior on Mar 15 could be ₹300 (Jan 1 window), ₹340 (Mar 1 window), or ₹320 (Mar 10–20 window). We pick ₹320 (most recent effective_from = Mar 10).

**If wrong**: If "most specific window wins" (shortest date range), then the Mar 10–20 window (₹320) wins either way. But for other overlaps, the result would differ. Expected-pay numbers would shift; 61 flagged shifts could get different rates.

**Alternative**: "Longest-standing rate wins" (earliest effective_from) — would pick Jan 1 windows, significantly lower pay for many workers.

---

## 2. `work_date` Is Authoritative Over `entered_at`

**Blast radius**: HIGH — flips the timezone bug analysis.

**Assumption**: The `work_date` column records when work was actually performed. `entered_at` is the timestamp when the supervisor logged the shift (which may be later, or on a different day due to TZ conversion).

**Used for**: All payroll period calculations use `canonical_work_date = work_date`. `entered_at_IST` is used only for backdating and timezone-flag detection.

**If wrong**: If `entered_at_IST` were authoritative for work date, workers near UTC midnight on vendor_b would have their shifts attributed to a different calendar day, potentially a different pay rate.

---

## 3. Sub-₹50 Transfers Are Unit Errors, Not Legitimate Adjustments

**Blast radius**: MEDIUM — affects underpayment count for 8 workers.

**Assumption**: No legitimate wage payment for rural field workers in India is below ₹50 (5,000 paise). The 8 detected transfers are data entry errors (stored in rupees instead of paise).

**If wrong**: If some transfers represent partial refunds, deductions, or legitimate micro-adjustments, we overstate the underpayment for those workers.

**Mitigation**: Flag is `suspected_unit_error`, not `confirmed`. Human review required before any recovery action.

---

## 4. Backdated Logs Should Be Paid in Work-Date's Payroll Cycle

**Blast radius**: MEDIUM — affects whether L02611/L02612 are Feb underpayment or March bonus.

**Assumption**: The correct payroll period for a shift is determined by `canonical_work_date`, not `entered_at`. A Feb shift entered in March should have been paid in the Feb payroll run.

**If wrong**: If the business rule is "pay in the cycle when entry was received," then L02611/L02612 should be in March expected pay — they would be correctly paid (or overpaid) rather than showing as Feb underpayment.

---

## 5. Phone Numbers Are Stable for 90 Days

**Blast radius**: MEDIUM — affects identity resolution for any worker who changed numbers.

**Assumption**: A worker's `phone` in workers.csv was their phone for the entire 90-day reconciliation window.

**If wrong**: Shifts logged under an old number would fail phone match, fall through to fuzzy name match, and potentially resolve to the wrong worker.

**Detection**: Impossible without historical phone records.

---

## 6. Transfers Map to Work via Worker-ID and Month, Not 1:1 to Shifts

**Blast radius**: MEDIUM — affects reconciliation granularity.

**Assumption**: Transfers are batched monthly (most have `23:59:00` timestamps). One transfer covers a worker's entire monthly wage — not a 1:1 mapping to individual shifts.

**Evidence**: Transfer timestamps cluster at month-end (mostly `23:59:00`). Individual shift counts don't match transfer counts.

**If wrong**: If transfers are 1:1 to shifts, month-grain aggregation hides per-shift mismatch. We'd need a different join key.

---

## 7. Mid-day (Non-Batch) Transfers Are Corrections, Not Invalid Data

**Blast radius**: MEDIUM — affects the total overpaid/underpaid figures.

**Assumption**: Normal payroll transfers are batched at `23:59:00`. We observed a few transfers at `11:00:00`. We assume these are legitimate manual corrections or ad-hoc payments, and thus include them in the total `paid_paise` sum, rather than dropping them as invalid.

**If wrong**: If these mid-day transfers are actually duplicates or test transactions that shouldn't be counted, our pipeline currently counts them, leading to an inflation of "overpaid" workers (e.g., pipeline says 3 overpaid, ground truth says 1, mostly because of these mid-day transfers).

**Mitigation**: We flag them as `suspected_correction` so human reviewers can easily audit and exclude them if necessary.

---

## 8. `seniority` in `workers.csv` Reflects the Entire 90-Day Window

**Blast radius**: LOW — affects rate lookup for any worker promoted/demoted mid-period.

**Assumption**: A worker's current seniority (junior/senior) applied for all 90 days. If a worker was promoted from junior to senior on Feb 15, their pre-promotion shifts would be looked up at the wrong (higher) rate.

**Detection**: No promotion history available in input data.

---

## 9. Wage Rates in `wage_rates.csv` Are Complete and Non-Overlapping by Design (Except Known Cases)

**Blast radius**: LOW — affects correctness of "no rate match" cases.

**Assumption**: Every worker-shift combination has exactly one intended rate. Known overlaps are data errors (not intentional dual rates).

**If wrong**: Some overlaps might be intentional (e.g., probationary rates). The `rate_overlap` flag would then produce false positives.
