# 🇮🇳 Bharat Intelligence Payroll Reconciliation

A robust, full-stack payroll reconciliation system designed to audit wage data, detect underpayments, and surface systemic data pipeline issues for field operations teams.

## 🎯 What is this project?

In high-volume field operations, workers are paid based on hourly rates that can change over time. Discrepancies between what a worker *should* be paid and what was *actually* transferred to their bank account can occur due to data entry errors, mid-day corrections, or missing records.

This system takes raw data (shifts, bank transfers, and historical hourly rates) and automatically **reconciles** them. It flags issues like:
- **Wage Theft / Underpayment:** Workers receiving less than their calculated expected pay.
- **Overpayments:** Transfers exceeding expected pay (e.g., mid-day manual corrections).
- **Ambiguous Data:** Name mismatches, time zone boundary risks, and overlapping rates.

The result is a transparent, audit-ready dashboard that empowers operations teams to triage flagged transactions and ensure every worker is paid accurately.

## ✨ Key Features

- **🔍 Automated Discrepancy Detection**: Intelligently flags discrepancies with confidence scores and root cause analysis.
- **⚙️ High-Precision Financial Engine**: Powered by **DuckDB** for complex effective-dated rate lookups and Python's `Decimal` (precision=28) to eliminate floating-point rounding errors.
- **📊 Interactive Triage Dashboard**: A modern React frontend allowing operations teams to review flagged discrepancies, read AI-generated findings, and drill down into individual shifts.
- **🛡️ Idempotent Processing**: The data pipeline can be run repeatedly without duplicating records or corrupting the SQLite database.
- **📱 Phone Normalization**: Robust fallback parsing for malformed Indian mobile numbers.

## 🛠️ Tech Stack

### Frontend
- **Framework**: React 19 + Vite
- **Routing & State**: TanStack Router, TanStack Query
- **Styling & UI**: Tailwind CSS v4, shadcn/ui (Radix Primitives), Lucide Icons
- **Visualizations**: Recharts

### Backend
- **Framework**: FastAPI (Python)
- **Data Processing**: DuckDB (In-process OLAP for window functions and time-series joins)
- **Database**: SQLite (Stored as `INTEGER` paise to prevent float corruption)

---

## 🚀 Quick Start (Run Both at Once)

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

## 📁 Project Structure

- `/backend` - The Python FastAPI server and reconciliation pipeline logic. Generates the `reconciled.sqlite` database.
- `/frontend` - The React dashboard that visualizes the SQLite data and allows manual triage.
- `/*.md` - In-depth engineering methodology and documentation (see below).

---

## 📚 Deep Dive Documentation

For a comprehensive breakdown of the engineering methodology, AI usage, and forensic findings, please review the following documents:

- 🏗️ **[DECISIONS.md](./DECISIONS.md)**: Why DuckDB and SQLite were used, precision math logic, and intentional trade-offs.
- 🕵️ **[FORENSICS.md](./FORENSICS.md)**: Ground truth evaluation, anomaly detection, and deep dive into wage discrepancies.
- 🧠 **[ASSUMPTIONS.md](./ASSUMPTIONS.md)**: System assumptions, edge cases, and fallback logic for malformed data.
- 🤖 **[AI_USAGE.md](./AI_USAGE.md)**: Breakdown of AI vs. Human implementation during the development process.
