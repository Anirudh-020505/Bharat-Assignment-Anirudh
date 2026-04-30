import { Link, useLocation } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

export function AppHeader({ lastRefreshed }: { lastRefreshed?: Date | null }) {
  const loc = useLocation();
  const tabs = [
    { to: "/", label: "Dashboard" },
    { to: "/triage", label: "Triage" },
  ] as const;

  return (
    <header className="sticky top-0 z-40 border-b border-border bg-background/80 backdrop-blur">
      <div className="mx-auto flex max-w-[1400px] items-center justify-between px-6 py-3">
        <div className="flex items-center gap-6">
          <Link to="/" className="flex items-center gap-2">
            <div className="h-7 w-7 rounded-md bg-gradient-to-br from-primary to-warning ring-1 ring-primary/40" />
            <div className="leading-tight">
              <div className="text-sm font-semibold">Bharat Intelligence</div>
              <div className="text-[10px] uppercase tracking-widest text-muted-foreground">
                Wage Reconciliation
              </div>
            </div>
          </Link>
          <nav className="flex gap-1">
            {tabs.map((t) => {
              const active =
                t.to === "/" ? loc.pathname === "/" : loc.pathname.startsWith(t.to);
              return (
                <Link
                  key={t.to}
                  to={t.to}
                  className={cn(
                    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    active
                      ? "bg-accent text-foreground"
                      : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                  )}
                >
                  {t.label}
                </Link>
              );
            })}
          </nav>
        </div>
        <div className="text-right text-xs text-muted-foreground tabular-nums">
          {lastRefreshed ? (
            <>
              <div>Last refresh</div>
              <div className="font-mono text-foreground">
                {lastRefreshed.toLocaleTimeString()}
              </div>
            </>
          ) : (
            <span className="opacity-60">—</span>
          )}
        </div>
      </div>
    </header>
  );
}
