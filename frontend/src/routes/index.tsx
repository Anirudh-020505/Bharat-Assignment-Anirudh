import { createFileRoute, Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { fetchSummary, type FlagKey } from "@/lib/api";
import { paiseToInr } from "@/lib/money";
import { AppHeader } from "@/components/AppHeader";
import { KPICard } from "@/components/KPICard";
import { ConfidenceBadge } from "@/components/ConfidenceBadge";
import { DeltaHistogram } from "@/components/DeltaHistogram";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { FLAG_LABELS, FLAG_DESCRIPTIONS } from "@/lib/flags";
import { AlertTriangle, ArrowDownCircle, ArrowUpCircle, Users, Banknote, ChevronRight } from "lucide-react";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Dashboard — Bharat Recon" },
      {
        name: "description",
        content:
          "Overview of wage reconciliation: workers owed money, bug breakdown, and delta distribution.",
      },
    ],
  }),
  component: DashboardPage,
});

function DashboardPage() {
  const { data, isLoading, isError, error, dataUpdatedAt } = useQuery({
    queryKey: ["summary"],
    queryFn: fetchSummary,
  });

  const lastRefreshed = useMemo(
    () => (dataUpdatedAt ? new Date(dataUpdatedAt) : null),
    [dataUpdatedAt],
  );

  return (
    <div className="min-h-screen">
      <AppHeader lastRefreshed={lastRefreshed} />
      <main className="mx-auto max-w-[1400px] space-y-6 px-6 py-6">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Reconciliation Overview</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            ~12,000 payouts across 90 days · 8 detector categories ·{" "}
            <span className="text-warning">spot-check before trusting any aggregate</span>
          </p>
        </div>

        {isError ? (
          <Card className="border-negative/40 bg-negative-bg p-4 text-sm">
            Failed to load summary: {(error as Error).message}. Is the API up at the configured
            base URL?
          </Card>
        ) : null}

        {/* KPI ROW ============================================================ */}
        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {isLoading || !data ? (
            <>
              {[0, 1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-32 rounded-lg" />
              ))}
            </>
          ) : (
            <>
              <KPICard
                tone="negative"
                title="Workers Owed Money"
                value={data.workers_owed_money.toLocaleString("en-IN")}
                subtitle={`Total underpaid: ${paiseToInr(BigInt(Math.round(Number(data.total_underpaid_inr) * 100)))}`}
                icon={<ArrowDownCircle className="h-4 w-4" />}
              />
              <KPICard
                tone="warning"
                title="Workers Overpaid"
                value={data.workers_overpaid.toLocaleString("en-IN")}
                subtitle={`Total overpaid: ${paiseToInr(BigInt(Math.round(Number(data.total_overpaid_inr) * 100)))}`}
                icon={<ArrowUpCircle className="h-4 w-4" />}
              />
              <KPICard
                tone="info"
                title="Transfers Flagged"
                value={(
                  data.transfers_flagged ??
                  Object.values(data.flag_breakdown).reduce((s, n) => s + n, 0)
                ).toLocaleString("en-IN")}
                subtitle="Across all 8 detectors"
                icon={<AlertTriangle className="h-4 w-4" />}
              />
              <KPICard
                tone="neutral"
                title="Total Reconciled"
                value={data.total_workers_reconciled.toLocaleString("en-IN")}
                subtitle="Workers in this run"
                icon={<Users className="h-4 w-4" />}
              />
            </>
          )}
        </section>

        {/* BUG BREAKDOWN ====================================================== */}
        <section className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <Card className="p-5 lg:col-span-2">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-base font-semibold">Bug Breakdown</h2>
                <p className="text-xs text-muted-foreground">
                  Click a row to triage workers affected by that detector.
                </p>
              </div>
              <Banknote className="h-4 w-4 text-muted-foreground" />
            </div>
            {isLoading || !data ? (
              <div className="space-y-2">
                {Array.from({ length: 6 }).map((_, i) => (
                  <Skeleton key={i} className="h-12 rounded-md" />
                ))}
              </div>
            ) : (
              <ul className="divide-y divide-border">
                {(Object.keys(data.flag_breakdown) as FlagKey[])
                  .sort((a, b) => data.flag_breakdown[b] - data.flag_breakdown[a])
                  .map((flag) => (
                    <li key={flag}>
                      <Link
                        to="/triage"
                        search={{ flag, page: 1 }}
                        className="group flex items-center justify-between gap-4 py-3 transition-colors hover:bg-accent/40"
                      >
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium">{FLAG_LABELS[flag]}</span>
                            <ConfidenceBadge level={data.confidence_levels[flag]} />
                          </div>
                          <p className="mt-0.5 truncate text-xs text-muted-foreground">
                            {FLAG_DESCRIPTIONS[flag]}
                          </p>
                        </div>
                        <div className="font-mono text-lg font-semibold tabular-nums">
                          {data.flag_breakdown[flag].toLocaleString("en-IN")}
                        </div>
                        <ChevronRight className="h-4 w-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
                      </Link>
                    </li>
                  ))}
              </ul>
            )}
          </Card>

          <Card className="p-5">
            <h2 className="mb-3 text-base font-semibold">Confidence Mix</h2>
            <p className="mb-4 text-xs text-muted-foreground">
              How much we trust each detector category.
            </p>
            {isLoading || !data ? (
              <Skeleton className="h-40" />
            ) : (
              <div className="space-y-3">
                {(["HIGH", "MEDIUM", "LOW"] as const).map((lvl) => {
                  const count = (Object.keys(data.confidence_levels) as FlagKey[]).filter(
                    (k) => data.confidence_levels[k] === lvl,
                  ).length;
                  return (
                    <div key={lvl} className="flex items-center justify-between">
                      <ConfidenceBadge level={lvl} />
                      <span className="font-mono text-sm tabular-nums text-muted-foreground">
                        {count} detectors
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </Card>
        </section>

        {/* HISTOGRAM ========================================================== */}
        <Card className="p-5">
          <div className="mb-4">
            <h2 className="text-base font-semibold">
              Reconciliation Delta Distribution — spot-checked before trusting
            </h2>
            <p className="text-xs text-muted-foreground">
              Negative buckets = underpaid · amber = overpaid · green = exact match
            </p>
          </div>
          {isLoading || !data ? (
            <Skeleton className="h-72" />
          ) : (
            <DeltaHistogram data={data.delta_distribution} />
          )}
        </Card>
      </main>
    </div>
  );
}
