import type { FlagKey, Confidence } from "./api";

export const FLAG_LABELS: Record<FlagKey, string> = {
  suspected_unit_error: "Suspected Unit Error",
  rate_overlap: "Rate Overlap",
  timezone_boundary_risk: "Timezone Boundary Risk",
  invalid_hours: "Invalid Hours",
  backdated_crosses_cycle: "Backdated Crosses Cycle",
  no_matching_transfer: "No Matching Transfer",
  ambiguous_name_match: "Ambiguous Name Match",
  rate_precision_anomaly: "Rate Precision Anomaly",
};

export const FLAG_DESCRIPTIONS: Record<FlagKey, string> = {
  suspected_unit_error:
    "Transfer amount appears to be in rupees while the system expected paise (or vice versa). Often a 100× discrepancy.",
  rate_overlap:
    "Two or more rate cards apply to the same shift window. Reconciler had to pick one — verify the chosen rate.",
  timezone_boundary_risk:
    "Shift crosses a timezone boundary or DST event; hours may be over- or under-counted by the source system.",
  invalid_hours:
    "Recorded hours fall outside plausible bounds (negative, >24, or end before start).",
  backdated_crosses_cycle:
    "A shift or correction was backdated into a payroll cycle that has already been paid out.",
  no_matching_transfer:
    "Expected payout for this period has no corresponding bank transfer / UTR.",
  ambiguous_name_match:
    "Transfer payee name is similar but not identical to the worker on record (possible namesake or typo).",
  rate_precision_anomaly:
    "Rate has unusual precision (e.g. fractional paise) suggesting an upstream rounding bug.",
};

export const CONFIDENCE_TONE: Record<Confidence, "positive" | "warning" | "muted"> = {
  HIGH: "positive",
  MEDIUM: "warning",
  LOW: "muted",
};

/** Parse the comma-separated review_reason string into typed flags. */
export function parseFlags(reason: string): FlagKey[] {
  if (!reason) return [];
  return reason
    .split(",")
    .map((s) => s.trim())
    .filter((s): s is FlagKey => s in FLAG_LABELS);
}
