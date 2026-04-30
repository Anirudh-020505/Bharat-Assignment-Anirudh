import { createFileRoute, Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { fetchWorker, type FlagKey } from "@/lib/api";
import { AppHeader } from "@/components/AppHeader";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ConfidenceBadge } from "@/components/ConfidenceBadge";
import { FlagChip } from "@/components/FlagChip";
import { Money, DeltaCell } from "@/components/Money";
import { FLAG_LABELS, FLAG_DESCRIPTIONS } from "@/lib/flags";
import { ArrowLeft, Phone, MapPin, Briefcase, Calendar } from "lucide-react";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/worker/$workerId")({
  head: ({ params }) => ({
    meta: [
      { title: `Worker ${params.workerId} — Bharat Recon` },
      { name: "description", content: `Reconciliation drill-down for worker ${params.workerId}.` },
    ],
  }),
  component: WorkerDetailPage,
});

function WorkerDetailPage() {
  const { workerId } = Route.useParams();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["worker", workerId],
    queryFn: () => fetchWorker(workerId),
  });

  return (
    <div className="min-h-screen">
      <AppHeader />
      <main className="mx-auto max-w-[1400px] space-y-6 px-6 py-6">
        <Link
          to="/triage"
          className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="h-4 w-4" /> Back to triage
        </Link>

        {isError ? (
          <Card className="border-negative/40 bg-negative-bg p-4 text-sm">
            Failed to load worker: {(error as Error).message}
          </Card>
        ) : null}

        {isLoading || !data ? (
          <Skeleton className="h-32 rounded-lg" />
        ) : (
          <Card className="p-6">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <div className="font-mono text-xs uppercase tracking-wider text-muted-foreground">
                  {data.worker_id}
                </div>
                <h1 className="mt-1 text-2xl font-semibold">{data.worker_name}</h1>
                <div className="mt-3 flex flex-wrap gap-x-5 gap-y-1.5 text-sm text-muted-foreground">
                  <Info icon={<MapPin className="h-3.5 w-3.5" />}>{data.state}</Info>
                  <Info icon={<Briefcase className="h-3.5 w-3.5" />}>
                    {data.role}
                    {data.seniority ? ` · ${data.seniority}` : ""}
                  </Info>
                  {data.phone ? (
                    <Info icon={<Phone className="h-3.5 w-3.5" />}>{data.phone}</Info>
                  ) : null}
                  {data.registered_on ? (
                    <Info icon={<Calendar className="h-3.5 w-3.5" />}>
                      Registered {data.registered_on}
                    </Info>
                  ) : null}
                </div>
              </div>
            </div>
          </Card>
        )}

        {data ? (
          <Tabs defaultValue="reconciliation" className="space-y-4">
            <TabsList>
              <TabsTrigger value="reconciliation">Reconciliation</TabsTrigger>
              <TabsTrigger value="shifts">Shifts ({data.shifts.length})</TabsTrigger>
              <TabsTrigger value="transfers">Transfers ({data.transfers.length})</TabsTrigger>
              <TabsTrigger value="flags">Flags</TabsTrigger>
            </TabsList>

            {/* RECONCILIATION ============================================== */}
            <TabsContent value="reconciliation" className="space-y-3">
              {data.monthly_reconciliation.length === 0 ? (
                <EmptyState>No reconciliation periods on record.</EmptyState>
              ) : (
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
                  {data.monthly_reconciliation.map((m) => (
                    <Card key={m.period} className="p-4">
                      <div className="flex items-center justify-between">
                        <div className="font-mono text-sm text-muted-foreground">{m.period}</div>
                        <DeltaCell paise={m.delta_paise} />
                      </div>
                      <div className="mt-3 grid grid-cols-2 gap-2 text-sm">
                        <Stat label="Expected" value={<Money paise={m.expected_paise} />} />
                        <Stat label="Paid" value={<Money paise={m.paid_paise} />} />
                      </div>
                      {m.flags.length > 0 ? (
                        <div className="mt-3 flex flex-wrap gap-1">
                          {m.flags.map((f) => (
                            <FlagChip key={f} flag={f} />
                          ))}
                        </div>
                      ) : null}
                    </Card>
                  ))}
                </div>
              )}
            </TabsContent>

            {/* SHIFTS ====================================================== */}
            <TabsContent value="shifts">
              <Card className="overflow-hidden p-0">
                <DataTable
                  rows={data.shifts}
                  empty="No shifts recorded."
                  columns={[
                    { h: "Shift ID", c: (s) => <span className="font-mono text-xs">{s.shift_id}</span> },
                    { h: "Date", c: (s) => s.date },
                    { h: "Start", c: (s) => <span className="font-mono">{s.start}</span> },
                    { h: "End", c: (s) => <span className="font-mono">{s.end}</span> },
                    { h: "Hours", c: (s) => <span className="font-mono tabular-nums">{s.hours}</span> },
                    { h: "Rate", c: (s) => <Money paise={s.rate_paise} /> },
                    {
                      h: "Flags",
                      c: (s) =>
                        s.flags?.length ? (
                          <div className="flex gap-1">
                            {s.flags.map((f) => (
                              <FlagChip key={f} flag={f} />
                            ))}
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        ),
                    },
                  ]}
                  rowClass={(s) =>
                    s.flags?.includes("invalid_hours") || s.flags?.includes("timezone_boundary_risk")
                      ? "bg-negative-bg/40"
                      : ""
                  }
                />
              </Card>
            </TabsContent>

            {/* TRANSFERS =================================================== */}
            <TabsContent value="transfers">
              <Card className="overflow-hidden p-0">
                <DataTable
                  rows={data.transfers}
                  empty="No transfers recorded."
                  columns={[
                    { h: "Transfer ID", c: (t) => <span className="font-mono text-xs">{t.transfer_id}</span> },
                    { h: "UTR", c: (t) => <span className="font-mono text-xs">{t.utr}</span> },
                    { h: "Date", c: (t) => t.date },
                    { h: "Amount", c: (t) => <Money paise={t.amount_paise} /> },
                    {
                      h: "Flags",
                      c: (t) =>
                        t.flags?.length ? (
                          <div className="flex gap-1">
                            {t.flags.map((f) => (
                              <FlagChip key={f} flag={f} />
                            ))}
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        ),
                    },
                  ]}
                  rowClass={(t) =>
                    t.flags?.includes("suspected_unit_error") ? "bg-negative-bg/60" : ""
                  }
                />
              </Card>
            </TabsContent>

            {/* FLAGS ======================================================= */}
            <TabsContent value="flags" className="space-y-3">
              {(() => {
                // Union of flags from monthly + shifts + transfers + evidence keys
                const flagSet = new Set<FlagKey>();
                data.monthly_reconciliation.forEach((m) => m.flags.forEach((f) => flagSet.add(f)));
                data.shifts.forEach((s) => s.flags?.forEach((f) => flagSet.add(f)));
                data.transfers.forEach((t) => t.flags?.forEach((f) => flagSet.add(f)));
                Object.keys(data.flag_evidence ?? {}).forEach((f) => flagSet.add(f as FlagKey));
                const flags = Array.from(flagSet);
                if (flags.length === 0) return <EmptyState>No flags raised for this worker.</EmptyState>;
                return flags.map((flag) => {
                  const ev = data.flag_evidence?.[flag];
                  return (
                    <Card key={flag} className="p-5">
                      <div className="flex items-start justify-between gap-4">
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-semibold">{FLAG_LABELS[flag]}</h3>
                          </div>
                          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
                            {ev?.explanation ?? FLAG_DESCRIPTIONS[flag]}
                          </p>
                        </div>
                      </div>
                      {(ev?.log_ids?.length || ev?.utrs?.length) ? (
                        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
                          {ev?.log_ids?.length ? (
                            <Evidence label="Log IDs" items={ev.log_ids} mono />
                          ) : null}
                          {ev?.utrs?.length ? <Evidence label="UTRs" items={ev.utrs} mono /> : null}
                        </div>
                      ) : null}
                      {ev?.claim ? (
                        <div className="mt-4 rounded-md border border-info/30 bg-info/10 p-3 text-sm">
                          <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-info">
                            Reproducible claim
                          </div>
                          {ev.claim}
                        </div>
                      ) : null}
                    </Card>
                  );
                });
              })()}
            </TabsContent>
          </Tabs>
        ) : null}
      </main>
    </div>
  );
}

function Info({ icon, children }: { icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {icon}
      {children}
    </span>
  );
}

function Stat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="mt-0.5">{value}</div>
    </div>
  );
}

function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <Card className="p-10 text-center text-sm text-muted-foreground">{children}</Card>
  );
}

function Evidence({ label, items, mono }: { label: string; items: string[]; mono?: boolean }) {
  return (
    <div>
      <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="flex flex-wrap gap-1">
        {items.map((it) => (
          <Badge
            key={it}
            variant="outline"
            className={cn("text-xs", mono && "font-mono")}
          >
            {it}
          </Badge>
        ))}
      </div>
    </div>
  );
}

interface DataTableCol<T> {
  h: string;
  c: (row: T) => React.ReactNode;
}
function DataTable<T>({
  rows,
  columns,
  empty,
  rowClass,
}: {
  rows: T[];
  columns: DataTableCol<T>[];
  empty: string;
  rowClass?: (row: T) => string;
}) {
  if (rows.length === 0) {
    return <div className="px-4 py-12 text-center text-sm text-muted-foreground">{empty}</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="bg-muted/40 text-xs uppercase tracking-wider text-muted-foreground">
          <tr>
            {columns.map((c) => (
              <th key={c.h} className="px-3 py-2.5 text-left font-medium">
                {c.h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.map((r, i) => (
            <tr key={i} className={cn("transition-colors hover:bg-accent/30", rowClass?.(r))}>
              {columns.map((c, j) => (
                <td key={j} className="px-3 py-2.5 align-middle">
                  {c.c(r)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
