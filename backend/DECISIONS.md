# Technical Decisions

## Why Python + DuckDB + FastAPI

### vs. Next.js API Routes / Prisma

| Concern | Python stack | Next.js/Prisma |
|---------|-------------|----------------|
| Effective-dated JOIN | DuckDB window SQL in 10 lines | Custom JS logic, error-prone |
| Money math | `Decimal` is first-class, prec=28 | `Number` breaks at large paise values |
| SQLite output | Portable; reviewable in any SQL client | Prisma adds ORM abstraction |
| Deployment | `python -m src.reconcile` — zero infra | Node build + Prisma migrate |
| DuckDB | In-process OLAP, no server | No equivalent in JS ecosystem |

### Why DuckDB for Rate Lookup

The effective-dated JOIN with `ROW_NUMBER() OVER (PARTITION BY log_id ORDER BY effective_from DESC)` expresses the tiebreaker declaratively in one query. The alternative — iterating rows in Python or JS — is more code, harder to reason about, and slower for 2,600+ shifts.

---

## Why sync `sqlite3` over `aiosqlite`

The dataset is small (100 workers, ~300 reconciled rows, ~5,000 total rows). FastAPI runs sync handlers in a threadpool automatically — there is no blocking concern at this scale. Async database access would add complexity (`asyncio`, `await`, connection pool config) with zero throughput benefit. **Sync is simpler and faster end-to-end here.**

---

## Phone Normalization: Last-10-Digits over Strict `phonenumbers`

The raw data contains phone numbers in many formats:
- `+91 9627028951`
- `91-9204505331`
- `0 9204505331`
- `94271 09477` (spaced)

`phonenumbers.parse()` is attempted first for structured validation. On `NumberParseException`, we fall back to stripping non-digits and taking the last 10. This is more robust than strict parsing because:

1. Field workers enter phone numbers manually on mobile apps — formatting is inconsistent.
2. The last 10 digits of any Indian mobile number is a stable identifier regardless of prefix.
3. Strictness would reject valid numbers with malformed country codes, increasing unmatched rate.

**Trade-off**: A 10-digit collision between two different numbers is theoretically possible but negligible at 100-worker scale.

---

## Rounding Rule: `Decimal` with `ROUND_HALF_UP`, prec=28

- `decimal.getcontext().prec = 28` set at module load in `rates.py`
- `ROUND_HALF_UP` chosen for financial calculations (standard banking rounding in India)
- All intermediate products computed in Decimal before quantizing to paise
- Formula: `(hourly_rate_inr × hours × Decimal('100')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)`
- **No float is ever used for money math** — this is enforced by type and code review

---

## Three Things Done Wrong on Purpose

### 1. Month-grain reconciliation loses intra-month rate-change precision

Expected pay is aggregated by calendar month of `canonical_work_date`. A worker with shifts before **and** after March 15 (Field Surveyor rate change) gets their shifts aggregated at month grain — we don't split the month into two sub-periods. The effect: the rate-change impact is visible in the flag (`rate_overlap`, `timezone_boundary_risk`) but not split out in the `reconciled` table.

**Acceptable because**: The flag still surfaces the issue; a human reviewer can drill into the `shifts` table for per-shift expected pay. Month-grain is the payroll cycle grain (transfers are batched monthly).

### 2. Phone treated as stable for the 90-day window

If a worker changed their phone number during the 90 days, our identity resolution may misattribute shifts. There is no mechanism to detect this without historical phone records.

**Acceptable because**: No such history is available in the input data. The assumption is documented and conservative (we flag `ambiguous_name_match` when fuzzy fallback fires).

### 3. No partial-day proration, overtime, or break-time modeling

`expected_pay_paise = hours × rate × 100` with no overtime multiplier, no break deduction, no shift differential. The spec does not define these rules.

**Acceptable because**: Not specified. Adding them would be assumption-driven and harder to audit. Simple linear pay is the safest default.

---

## Why Money is Stored as INTEGER (paise) in SQLite

SQLite's `REAL` type is IEEE 754 double — it will silently introduce rounding errors for paise values above ~2^53 (~₹90 trillion). Storing as `INTEGER paise` avoids this completely. Python `Decimal` handles all computation; SQLite only stores and retrieves.

---

## Flag Table Idempotency

The `shifts`, `transfers`, and `reconciled` tables all use `INSERT OR REPLACE` with their respective primary keys to ensure that re-running the pipeline on the same database is idempotent.

The `flags` table uses an `AUTOINCREMENT` primary key. Initially, running the pipeline multiple times would duplicate the flags (growing from 280 to 560 to 840, etc.). The decision was made to explicitly execute `DELETE FROM flags` at the start of the `write_flags` function to maintain true idempotency without needing a complex composite primary key on the flags table itself.

---

## Mid-day Transfers: Tag and Keep

During reconciliation, it was found that the pipeline reported 3 overpaid workers instead of the expected 1. Analysis revealed that the extra 2 overpayments were caused by 4 transfers occurring exactly at 11:00 AM on Feb 22 (instead of the standard batch time of 23:59). 

The ground truth dataset likely excluded these or handled them differently. However, dropping data silently is an anti-pattern for an audit tool. 
**Decision**: Keep mid-day non-batch transfers in the calculations but explicitly flag them as `suspected_correction` so the operations team has a transparent audit trail of why the overpayment exists.
