import { createFileRoute, useNavigate, Link } from "@tanstack/react-router";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { useMemo } from "react";
import { z } from "zod";
import { zodValidator, fallback } from "@tanstack/zod-adapter";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import {
  fetchReconciled,
  exportCsvUrl,
  type FlagKey,
  type Confidence,
  type ReconciledItem,
  type ReconciledFilters,
} from "@/lib/api";
import { AppHeader } from "@/components/AppHeader";
import { DeltaCell, Money } from "@/components/Money";
import { FlagChip } from "@/components/FlagChip";
import { ConfidenceBadge } from "@/components/ConfidenceBadge";
import { parseFlags, FLAG_LABELS } from "@/lib/flags";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Card } from "@/components/ui/card";
import { ChevronLeft, ChevronRight, Download, X } from "lucide-react";

// ----------------------------------------------------------------------------
// URL state schema — every filter lives in the URL so the view is shareable.
// ----------------------------------------------------------------------------
const FLAG_KEYS = [
  "suspected_unit_error",
  "rate_overlap",
  "timezone_boundary_risk",
  "invalid_hours",
  "backdated_crosses_cycle",
  "no_matching_transfer",
  "ambiguous_name_match",
  "rate_precision_anomaly",
] as const;

const triageSearchSchema = z.object({
  state: fallback(z.string(), "").default(""),
  role: fallback(z.string(), "").default(""),
  period: fallback(z.string(), "").default(""),
  flag: fallback(z.enum(FLAG_KEYS).or(z.literal("")), "").default(""),
  delta_sign: fallback(z.enum(["neg", "pos", "zero", ""]), "").default(""),
  confidence: fallback(z.enum(["HIGH", "MEDIUM", "LOW", ""]), "").default(""),
  needs_review: fallback(z.boolean(), false).default(false),
  page: fallback(z.number().int().min(1), 1).default(1),
});

export const Route = createFileRoute("/triage")({
  validateSearch: zodValidator(triageSearchSchema),
  head: () => ({
    meta: [
      { title: "Triage — Bharat Recon" },
      { name: "description", content: "Filter and triage worker payouts flagged for review." },
    ],
  }),
  component: TriagePage,
});

const PAGE_SIZE = 50;

function TriagePage() {
  const search = Route.useSearch();
  const navigate = useNavigate({ from: "/triage" });

  // Build the filter object passed to the API + react-query key.
  const filters: ReconciledFilters = useMemo(
    () => ({
      state: search.state || undefined,
      role: search.role || undefined,
      period: search.period || undefined,
      flag: search.flag || undefined,
      delta_sign: (search.delta_sign as "neg" | "pos" | "zero" | "") || undefined || undefined,
      confidence: (search.confidence as Confidence | "") || undefined || undefined,
      needs_review: search.needs_review || undefined,
      page: search.page,
      page_size: PAGE_SIZE,
    }),
    [search],
  );

  const { data, isLoading, isFetching, isError, error } = useQuery({
    queryKey: ["reconciled", filters],
    queryFn: () => fetchReconciled(filters),
    placeholderData: keepPreviousData,
  });

  // ---- helpers to update URL state ---------------------------------------
  const setSearch = (patch: Partial<typeof search>) => {
    navigate({
      search: (prev) => ({ ...prev, ...patch, page: patch.page ?? 1 }),
    });
  };

  const clearAll = () => {
    navigate({
      search: {
        state: "",
        role: "",
        period: "",
        flag: "",
        delta_sign: "",
        confidence: "",
        needs_review: false,
        page: 1,
      },
    });
  };

  // ---- TanStack Table columns -------------------------------------------
  const columns = useMemo<ColumnDef<ReconciledItem>[]>(
    () => [
      {
        header: "Worker",
        accessorKey: "worker_id",
        cell: (c) => (
          <div className="min-w-0">
            <div className="font-mono text-xs text-muted-foreground">
              {c.row.original.worker_id}
            </div>
            <div className="truncate text-sm font-medium">{c.row.original.worker_name}</div>
          </div>
        ),
      },
      {
        header: "State",
        accessorKey: "state",
        cell: (c) => <span className="text-sm">{c.getValue<string>()}</span>,
      },
      {
        header: "Role",
        accessorKey: "role",
        cell: (c) => <span className="text-sm">{c.getValue<string>()}</span>,
      },
      {
        header: "Period",
        accessorKey: "period",
        cell: (c) => <span className="font-mono text-xs">{c.getValue<string>()}</span>,
      },
      {
        header: () => <div className="text-right">Expected</div>,
        accessorKey: "expected_paise",
        cell: (c) => (
          <div className="text-right">
            <Money paise={c.getValue<string>()} />
          </div>
        ),
      },
      {
        header: () => <div className="text-right">Paid</div>,
        accessorKey: "paid_paise",
        cell: (c) => (
          <div className="text-right">
            <Money paise={c.getValue<string>()} />
          </div>
        ),
      },
      {
        header: () => <div className="text-right">Delta</div>,
        accessorKey: "delta_paise",
        cell: (c) => (
          <div className="flex justify-end">
            <DeltaCell paise={c.getValue<string>()} />
          </div>
        ),
      },
      {
        header: "Flags",
        accessorKey: "review_reason",
        cell: (c) => {
          const flags = parseFlags(c.getValue<string>());
          if (flags.length === 0)
            return <span className="text-xs text-muted-foreground">—</span>;
          return (
            <div className="flex flex-wrap gap-1">
              {flags.slice(0, 3).map((f) => (
                <FlagChip key={f} flag={f} />
              ))}
              {flags.length > 3 ? (
                <Badge variant="outline" className="text-[10px]">
                  +{flags.length - 3}
                </Badge>
              ) : null}
            </div>
          );
        },
      },
      {
        header: "Conf.",
        accessorKey: "confidence",
        cell: (c) => <ConfidenceBadge level={c.getValue<Confidence>()} />,
      },
      {
        id: "actions",
        header: "",
        cell: (c) => (
          <Link
            to="/worker/$workerId"
            params={{ workerId: c.row.original.worker_id }}
            className="text-xs font-medium text-primary hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            View →
          </Link>
        ),
      },
    ],
    [],
  );

  const table = useReactTable({
    data: data?.items ?? [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  const total = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  // Active filter chips
  const activeChips = [
    search.state && { key: "state", label: `State: ${search.state}` },
    search.role && { key: "role", label: `Role: ${search.role}` },
    search.period && { key: "period", label: `Period: ${search.period}` },
    search.flag && { key: "flag", label: `Flag: ${FLAG_LABELS[search.flag as FlagKey]}` },
    search.delta_sign && { key: "delta_sign", label: `Δ: ${search.delta_sign}` },
    search.confidence && { key: "confidence", label: `Conf: ${search.confidence}` },
    search.needs_review && { key: "needs_review", label: "Needs Review" },
  ].filter(Boolean) as { key: string; label: string }[];

  return (
    <div className="min-h-screen">
      <AppHeader />

      {/* Sticky filter bar */}
      <div className="sticky top-[57px] z-30 border-b border-border bg-background/95 backdrop-blur">
        <div className="mx-auto max-w-[1400px] space-y-3 px-6 py-3">
          <div className="flex flex-wrap items-end gap-2">
            <FilterField label="State">
              <Input
                value={search.state}
                onChange={(e) => setSearch({ state: e.target.value })}
                placeholder="MH, KA…"
                className="h-9 w-28"
              />
            </FilterField>

            <FilterField label="Role">
              <Input
                value={search.role}
                onChange={(e) => setSearch({ role: e.target.value })}
                placeholder="Data Entry…"
                className="h-9 w-40"
              />
            </FilterField>

            <FilterField label="Period">
              <Input
                value={search.period}
                onChange={(e) => setSearch({ period: e.target.value })}
                placeholder="2025-03"
                className="h-9 w-28 font-mono"
              />
            </FilterField>

            <FilterField label="Flag">
              <Select
                value={search.flag || "ALL"}
                onValueChange={(v) =>
                  setSearch({ flag: v === "ALL" ? "" : (v as FlagKey) })
                }
              >
                <SelectTrigger className="h-9 w-56">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">All flags</SelectItem>
                  {FLAG_KEYS.map((f) => (
                    <SelectItem key={f} value={f}>
                      {FLAG_LABELS[f]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </FilterField>

            <FilterField label="Delta">
              <Select
                value={search.delta_sign || "ALL"}
                onValueChange={(v) =>
                  setSearch({
                    delta_sign: v === "ALL" ? "" : (v as "neg" | "pos" | "zero"),
                  })
                }
              >
                <SelectTrigger className="h-9 w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">Any</SelectItem>
                  <SelectItem value="neg">Underpaid</SelectItem>
                  <SelectItem value="pos">Overpaid</SelectItem>
                  <SelectItem value="zero">Exact</SelectItem>
                </SelectContent>
              </Select>
            </FilterField>

            <FilterField label="Confidence">
              <Select
                value={search.confidence || "ALL"}
                onValueChange={(v) =>
                  setSearch({ confidence: v === "ALL" ? "" : (v as Confidence) })
                }
              >
                <SelectTrigger className="h-9 w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">Any</SelectItem>
                  <SelectItem value="HIGH">HIGH</SelectItem>
                  <SelectItem value="MEDIUM">MEDIUM</SelectItem>
                  <SelectItem value="LOW">LOW</SelectItem>
                </SelectContent>
              </Select>
            </FilterField>

            <label className="flex h-9 items-center gap-2 rounded-md border border-input bg-input/30 px-3 text-sm">
              <Checkbox
                checked={search.needs_review}
                onCheckedChange={(v) => setSearch({ needs_review: v === true })}
              />
              Needs review only
            </label>

            <div className="ml-auto flex gap-2">
              {activeChips.length > 0 ? (
                <Button variant="ghost" size="sm" onClick={clearAll}>
                  Clear filters
                </Button>
              ) : null}
              <Button asChild size="sm" variant="default">
                <a href={exportCsvUrl(filters)} download>
                  <Download className="mr-1.5 h-3.5 w-3.5" />
                  Export CSV
                </a>
              </Button>
            </div>
          </div>

          {activeChips.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {activeChips.map((c) => (
                <Badge key={c.key} variant="secondary" className="gap-1 pr-1">
                  {c.label}
                  <button
                    onClick={() =>
                      setSearch({
                        [c.key]: c.key === "needs_review" ? false : "",
                      } as Partial<typeof search>)
                    }
                    className="rounded-sm hover:bg-background/50"
                  >
                    <X className="h-3 w-3" />
                  </button>
                </Badge>
              ))}
            </div>
          ) : null}
        </div>
      </div>

      <main className="mx-auto max-w-[1400px] px-6 py-6">
        {isError ? (
          <Card className="border-negative/40 bg-negative-bg p-4 text-sm">
            Failed to load: {(error as Error).message}
          </Card>
        ) : null}

        <Card className="overflow-hidden p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/40 text-xs uppercase tracking-wider text-muted-foreground">
                {table.getHeaderGroups().map((hg) => (
                  <tr key={hg.id}>
                    {hg.headers.map((h) => (
                      <th key={h.id} className="px-3 py-2.5 text-left font-medium">
                        {flexRender(h.column.columnDef.header, h.getContext())}
                      </th>
                    ))}
                  </tr>
                ))}
              </thead>
              <tbody className="divide-y divide-border">
                {isLoading ? (
                  Array.from({ length: 10 }).map((_, i) => (
                    <tr key={i}>
                      {columns.map((_c, j) => (
                        <td key={j} className="px-3 py-3">
                          <Skeleton className="h-4 w-full" />
                        </td>
                      ))}
                    </tr>
                  ))
                ) : table.getRowModel().rows.length === 0 ? (
                  <tr>
                    <td colSpan={columns.length} className="px-3 py-16 text-center">
                      <div className="text-sm text-muted-foreground">
                        No discrepancies match these filters.
                      </div>
                    </td>
                  </tr>
                ) : (
                  table.getRowModel().rows.map((row) => (
                    <tr
                      key={row.id}
                      className="cursor-pointer transition-colors hover:bg-accent/40"
                      onClick={() =>
                        navigate({
                          to: "/worker/$workerId",
                          params: { workerId: row.original.worker_id },
                        })
                      }
                    >
                      {row.getVisibleCells().map((cell) => (
                        <td key={cell.id} className="px-3 py-3 align-middle">
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </td>
                      ))}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          <div className="flex items-center justify-between border-t border-border bg-muted/20 px-4 py-2.5 text-xs">
            <div className="text-muted-foreground tabular-nums">
              {total > 0 ? (
                <>
                  Showing{" "}
                  <span className="font-medium text-foreground">
                    {(search.page - 1) * PAGE_SIZE + 1}–
                    {Math.min(search.page * PAGE_SIZE, total)}
                  </span>{" "}
                  of <span className="font-medium text-foreground">{total.toLocaleString("en-IN")}</span>
                </>
              ) : isLoading ? (
                "Loading…"
              ) : (
                "No results"
              )}
              {isFetching && !isLoading ? <span className="ml-2 opacity-60">refreshing…</span> : null}
            </div>
            <div className="flex items-center gap-1">
              <Button
                size="sm"
                variant="ghost"
                disabled={search.page <= 1}
                onClick={() => setSearch({ page: search.page - 1 })}
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="px-2 font-mono tabular-nums">
                {search.page} / {totalPages}
              </span>
              <Button
                size="sm"
                variant="ghost"
                disabled={search.page >= totalPages}
                onClick={() => setSearch({ page: search.page + 1 })}
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </Card>
      </main>
    </div>
  );
}

function FilterField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      {children}
    </div>
  );
}
