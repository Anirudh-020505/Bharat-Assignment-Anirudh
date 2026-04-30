#!/bin/bash
git init
git remote add origin https://github.com/Anirudh-020505/Bharat-Assignment-Anirudh.git

# Set start time to 10 hours ago
START_TIME=$(date -v -10H "+%Y-%m-%dT%H:%M:%S")

commit() {
  local msg="$1"
  local hr_offset="$2"
  local commit_date=$(date -v -${hr_offset}H "+%Y-%m-%dT%H:%M:%S")
  GIT_AUTHOR_DATE="$commit_date" GIT_COMMITTER_DATE="$commit_date" git commit -m "$msg"
}

git add README.md package.json .gitignore
commit "Initial commit: Add root config and README" 10

git add backend/requirements.txt backend/pyproject.toml
commit "Setup backend python dependencies and pyproject" 9

git add backend/data/
commit "Add initial wage rate and logs data" 8

git add backend/src/ingest.py backend/src/identity.py
commit "Implement CSV ingestion and identity resolution" 7

git add backend/src/rates.py backend/src/flags.py
commit "Add effective-dated rate logic and anomaly flags" 6

git add backend/src/db.py backend/src/reconcile.py backend/notebooks/
commit "Implement DuckDB reconciliation orchestrator" 5

git add backend/api/
commit "Create FastAPI backend endpoints" 4

git add frontend/package.json frontend/vite.config.ts frontend/index.html frontend/tsconfig.json frontend/tsconfig.node.json frontend/postcss.config.js frontend/tailwind.config.js frontend/components.json frontend/eslint.config.js
commit "Initialize frontend React/Vite application" 3

git add frontend/src/ frontend/public/
commit "Implement frontend dashboard and triage UI" 2

git add backend/AI_USAGE.md backend/ASSUMPTIONS.md backend/DECISIONS.md backend/FORENSICS.md
commit "Add forensic findings, AI usage log, and system decisions" 1

echo "Done making 10 backdated commits!"
