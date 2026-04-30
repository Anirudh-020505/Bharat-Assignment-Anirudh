# AI Usage Log

Honest accounting of where AI (Claude) was used in this project and where human judgment overrode it.

---

## What AI Generated

### DuckDB Window Query (rates.py)
Used Claude to generate the effective-dated `ROW_NUMBER() OVER (PARTITION BY log_id ORDER BY effective_from DESC)` window query. Verified output against hand-traced cases: Data Entry MH junior on Mar 15 correctly picks ₹320 (effective_from=Mar 10 wins over Mar 1 and Jan 1). Query passed validation.

### Pydantic v2 Model Patterns (ingest.py)
AI generated the `@field_validator` + `model_validator` structure for pydantic v2. I cross-checked against pydantic v2 migration docs — the `mode="before"` parameter placement was initially wrong (AI generated v1 syntax); corrected manually.

### FastAPI Streaming Response (api/main.py)
AI generated the `StreamingResponse` + `csv.writer` generator pattern for `/api/export`. I verified it doesn't load full result into memory by tracing the generator execution path.

### Flag Function Skeletons (flags.py)
AI generated the initial pure-function skeletons. I added the `timezone_boundary_risk` boundary-crossing logic (lo/hi range check) after the initial version only checked `entered_date > effective_date` (wrong — needed to check if boundary is between the two dates).

---

## Where I Overrode AI

### Rejected aiosqlite
AI initially suggested `aiosqlite` for the FastAPI endpoints. I rejected this — the dataset is small (100 workers), FastAPI handles sync routes via threadpool, and `aiosqlite` would add complexity with zero throughput benefit. Used `sqlite3` throughout.

### Unit Error Threshold
AI initially suggested hardcoding `count=8` in the detection logic. I built the threshold detector (`amount_paise < 5000`) instead — count is programmatically derived, future-proof, and not hardcoded.

### Phone Normalization
AI's first draft used `phonenumbers.parse()` only and raised on exception, rejecting ~15% of logs. I added the last-10-digits regex fallback, which correctly resolves all phone formats present in the data (0 prefix, 91- prefix, spaced digits, etc.).

### Fuzzy Pre-filter
AI's initial fuzzy name matching was O(N²) — comparing every log name against all workers. I added the state+role pre-filter (subset workers by state AND role before fuzzy scoring), reducing candidates from 100 to ~5–10 per lookup.

### Decimal Enforcement
AI used `float` for paise arithmetic in an early draft of `rates.py`. I replaced all money math with `Decimal`, set `prec=28`, and documented `ROUND_HALF_UP` in DECISIONS.md.

---

## What AI Got Wrong (Caught and Fixed)

- **DuckDB VARCHAR vs DATE cast error**: AI generated `effective_to >= CAST(... AS DATE)` without casting `effective_to` itself. DuckDB threw a `BinderException`. Fixed with `TRY_CAST(r.effective_to AS DATE)`.
- **Pydantic v1 syntax in v2 context**: `@validator` instead of `@field_validator`. Fixed to pydantic v2 API.
- **Timezone boundary check**: Initial version checked only `entered_ist > boundary`; should check if boundary falls *between* `entered_date` and `work_date`. Rewrote the range check.
- **Missing API fields**: AI originally missed adding `state`, `role`, and `worker_name` to the `/api/reconciled` payload, causing the frontend to render blank columns. Added a `LEFT JOIN` on the `workers` table to fix.
- **Missing flags for unclassified discrepancies**: Rows with a pay mismatch (e.g., `-₹1800`) but no matching anomaly pattern were left with a blank reason. Fixed by adding a fallback `unexplained_discrepancy` flag.
- **Invalid Hours misinterpretation**: AI initially assumed a shift with 450 hours was a monetary unit error (rupees instead of paise). Realized it was a time entry error (e.g., 4.5 hrs entered without a decimal). Flagged as `invalid_hours`.
- **Flag Table Duplication**: AI wrote `INSERT INTO flags` without clearing old data, which broke the idempotency guarantee. Running the script twice inflated the flag count from 280 to 560 (exactly 2x). Fixed with a `DELETE FROM flags` wipe before insertion.

---

## What Was My Work (Not AI)

- Identifying the state+role pre-filter optimization for fuzzy matching
- The decision to store money as INTEGER paise in SQLite (not REAL)
- The "3 things done wrong on purpose" framing in DECISIONS.md
- Ranking assumptions by blast radius in ASSUMPTIONS.md
- The idempotency design: delete `output/` and re-run produces identical results
- All SQL queries in api/main.py (JOIN patterns for filter endpoints)
