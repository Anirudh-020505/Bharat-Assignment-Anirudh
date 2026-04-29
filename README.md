# Bharat Intelligence Payroll Reconciliation

This repository contains the full-stack payroll reconciliation system for Bharat Intelligence. It consists of a FastAPI + DuckDB backend and a React (Vite) frontend.

## Quick Start (Run Both at Once)

You can run both the frontend and backend simultaneously with a single command from the root directory:

```bash
# 1. Install all dependencies (Frontend + Backend + Root)
npm run install:all

# 2. Start both the frontend and backend concurrently
npm run dev
```

This will spin up:
- **Backend**: FastAPI server running on `http://127.0.0.1:8000`
- **Frontend**: Vite dev server running on `http://localhost:5173` (or whichever port Vite selects)

---

## Project Structure

- `/backend` - The Python reconciliation pipeline and FastAPI server.
  - Powered by DuckDB for effective-dated joining and Python `Decimal` for precision.
  - Generates the `reconciled.sqlite` database and exposes data to the frontend.
- `/frontend` - The React + Vite dashboard.
  - Visualizes wage theft, highlights discrepancies, and allows operations teams to triage flagged bank transfers and shifts.

## Documentation

For a deep dive into the engineering methodology, AI usage, and forensic findings, please see the markdown files located in the `backend/` directory:
- `backend/DECISIONS.md` - Why DuckDB and SQLite were used.
- `backend/FORENSICS.md` - Ground truth evaluation of wage discrepancies.
- `backend/ASSUMPTIONS.md` - System assumptions and edge cases.
- `backend/AI_USAGE.md` - Breakdown of AI vs Human implementation.
