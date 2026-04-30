// ============================================================================
// API client — talks to the FastAPI backend at VITE_API_URL (default :8000).
// All money fields are strings (paise as Decimal). Keep them as strings here;
// only convert via the BigInt helpers in lib/money.ts at render time.
// ============================================================================
import axios from "axios";

export const API_BASE =
  (import.meta.env.VITE_API_URL as string | undefined) ?? "http://localhost:8000";

export const http = axios.create({
  baseURL: API_BASE,
  timeout: 30_000,
});

// ---- Types mirroring the backend contract -----------------------------------

export type Confidence = "HIGH" | "MEDIUM" | "LOW";

export type FlagKey =
  | "suspected_unit_error"
  | "rate_overlap"
  | "timezone_boundary_risk"
  | "invalid_hours"
  | "backdated_crosses_cycle"
  | "no_matching_transfer"
  | "ambiguous_name_match"
  | "rate_precision_anomaly";

export interface SummaryResponse {
  total_workers_reconciled: number;
  workers_owed_money: number;
  workers_overpaid: number;
  total_underpaid_inr: string;
  total_overpaid_inr: string;
  flag_breakdown: Record<FlagKey, number>;
  confidence_levels: Record<FlagKey, Confidence>;
  delta_distribution: { bucket: string; count: number }[];
  // Optional extras the UI may show if the backend provides them:
  transfers_flagged?: number;
  last_refreshed_at?: string;
}

export interface ReconciledItem {
  worker_id: string;
  worker_name: string;
  state: string;
  role: string;
  period: string; // e.g. "2025-03"
  expected_paise: string;
  paid_paise: string;
  delta_paise: string;
  needs_manual_review: boolean;
  review_reason: string; // comma-separated FlagKeys
  confidence: Confidence;
}

export interface ReconciledResponse {
  items: ReconciledItem[];
  total: number;
  page: number;
  page_size: number;
}

export interface ReconciledFilters {
  state?: string;
  role?: string;
  period?: string;
  flag?: string;            // single flag for filtering (e.g. suspected_unit_error)
  needs_review?: boolean;
  delta_sign?: "neg" | "pos" | "zero";
  confidence?: Confidence;
  page?: number;
  page_size?: number;
}

export interface WorkerShift {
  shift_id: string;
  date: string;
  start: string;
  end: string;
  hours: string;
  rate_paise: string;
  flags?: FlagKey[];
}
export interface WorkerTransfer {
  transfer_id: string;
  utr: string;
  amount_paise: string;
  date: string;
  flags?: FlagKey[];
}
export interface MonthlyReconciliation {
  period: string;
  expected_paise: string;
  paid_paise: string;
  delta_paise: string;
  flags: FlagKey[];
}
export interface WorkerDetail {
  worker_id: string;
  worker_name: string;
  phone?: string;
  state: string;
  role: string;
  seniority?: string;
  registered_on?: string;
  shifts: WorkerShift[];
  transfers: WorkerTransfer[];
  monthly_reconciliation: MonthlyReconciliation[];
  flag_evidence?: Partial<
    Record<
      FlagKey,
      { explanation: string; log_ids?: string[]; utrs?: string[]; claim?: string }
    >
  >;
}

// ---- Endpoint helpers -------------------------------------------------------

export async function fetchSummary(): Promise<SummaryResponse> {
  const { data } = await http.get<SummaryResponse>("/api/summary");
  return data;
}

function buildReconciledParams(f: ReconciledFilters): Record<string, string | number | boolean> {
  const p: Record<string, string | number | boolean> = {
    page: f.page ?? 1,
    page_size: f.page_size ?? 50,
  };
  if (f.state) p.state = f.state;
  if (f.role) p.role = f.role;
  if (f.period) p.period = f.period;
  if (f.flag) p.flag = f.flag;
  if (f.needs_review) p.needs_review = true;
  if (f.delta_sign) p.delta_sign = f.delta_sign;
  if (f.confidence) p.confidence = f.confidence;
  return p;
}

export async function fetchReconciled(f: ReconciledFilters): Promise<ReconciledResponse> {
  const { data } = await http.get<ReconciledResponse>("/api/reconciled", {
    params: buildReconciledParams(f),
  });
  return data;
}

export async function fetchWorker(id: string): Promise<WorkerDetail> {
  const { data } = await http.get<WorkerDetail>(`/api/workers/${id}`);
  return data;
}

/** Build an absolute URL for the CSV export — used by an <a download> link. */
export function exportCsvUrl(f: ReconciledFilters): string {
  const params = new URLSearchParams();
  const p = buildReconciledParams(f);
  for (const [k, v] of Object.entries(p)) params.set(k, String(v));
  return `${API_BASE}/api/export?${params.toString()}`;
}
