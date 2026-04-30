# Bharat Intelligence Wage Reconciliation

Reconciliation pipeline for ~12,000 rural field worker wages. Detects 8 classes of payroll bugs across 90 days of supervisor logs, bank transfers, wage rates, and worker registry.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run Pipeline

```bash
python -m src.reconcile
# Produces: output/reconciled.sqlite + output/final_reconciled.csv
```

## Run API

```bash
uvicorn api.main:app --reload --port 8000
# Docs: http://localhost:8000/docs
```

## Run Tests

```bash
pytest tests/ -v
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/reconciled` | Paginated reconciled rows. Filters: `state`, `role`, `period`, `needs_review`, `delta_sign` |
| `GET /api/workers/{worker_id}` | Full worker profile: shifts + transfers + monthly recon |
| `GET /api/summary` | Aggregate stats + flag breakdown |
| `GET /api/export` | Streaming CSV with same filters as /api/reconciled |

## Data Files (data/)

| File | Rows | Description |
|------|------|-------------|
| supervisor_logs.csv | 2,617 | Shift entries from mobile app |
| bank_transfers.csv | 2,255 | Actual payouts in paise |
| wage_rates.csv | 18 | Effective-dated hourly rates |
| workers.csv | 100 | Canonical worker registry |

## Detected Bugs

| Flag | Count | Confidence |
|------|-------|-----------|
| suspected_unit_error | 8 transfers | HIGH |
| rate_overlap | 61 shifts | HIGH |
| rate_precision_anomaly | 204 shifts | HIGH |
| timezone_boundary_risk | 4 shifts | MEDIUM |
| backdated_crosses_cycle | 2 shifts | HIGH |
| invalid_hours | 1 shift (L02617) | HIGH |

## Key Numbers (from pipeline run)

- **Workers reconciled**: 100
- **Worker-months**: 300
- **Underpaid workers**: 97 (₹9,71,285.27 total)
- **Overpaid workers**: 3 (₹3,572.83 total)
- **Unit error transfers**: 8

## Project Structure

```
bharat-recon/
├── data/               # Input CSVs
├── src/
│   ├── ingest.py       # CSV load + pydantic validation
│   ├── normalize.py    # Phone/TZ normalization
│   ├── identity.py     # Phone-first + fuzzy worker resolution
│   ├── rates.py        # DuckDB effective-dated rate join
│   ├── flags.py        # 8 pure flag detection functions
│   ├── db.py           # SQLite schema + upsert helpers
│   └── reconcile.py    # Orchestrator
├── api/
│   └── main.py         # FastAPI (4 endpoints)
├── output/             # Generated: reconciled.sqlite + final_reconciled.csv
├── tests/
│   └── test_flags.py   # Unit tests for all 8 flags
├── FORENSICS.md        # Findings with real numbers
├── DECISIONS.md        # Technical design decisions
├── ASSUMPTIONS.md      # Ranked by blast radius
└── AI_USAGE.md         # Honest AI usage log
```

## Frontend

The React SPA is built separately and consumes the API at `http://localhost:8000`.

## Idempotency

Delete `output/` and re-run `python -m src.reconcile` — produces identical results.
