import { paiseToInr, paiseSign } from "@/lib/money";
import { cn } from "@/lib/utils";

/** Pure formatter — no color, used in detail tables/cards. */
export function Money({ paise, className }: { paise: string | number | bigint; className?: string }) {
  return <span className={cn("font-mono tabular-nums", className)}>{paiseToInr(paise)}</span>;
}

/** Color-coded delta cell: red bg if negative, amber if positive, muted if zero. */
export function DeltaCell({ paise }: { paise: string | number | bigint }) {
  const sign = paiseSign(typeof paise === "number" ? BigInt(Math.round(paise)) : paise);
  const cls =
    sign < 0
      ? "bg-negative-bg text-negative"
      : sign > 0
        ? "bg-warning-bg text-warning"
        : "bg-muted text-muted-foreground";
  return (
    <span
      className={cn(
        "inline-flex min-w-[7rem] justify-end rounded-md px-2 py-1 font-mono text-sm tabular-nums",
        cls,
      )}
    >
      {paiseToInr(paise)}
    </span>
  );
}
