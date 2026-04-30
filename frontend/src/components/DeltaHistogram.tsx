import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { bucketSortKey } from "@/lib/money";

interface Datum {
  bucket: string;
  count: number;
}

/**
 * Bar color logic:
 *   - Bucket label contains a leading "-" (or "neg") → negative → red
 *   - Bucket starts with "0" or contains "zero"     → neutral → green
 *   - Otherwise                                     → positive → amber
 */
function colorFor(bucket: string): string {
  const b = bucket.toLowerCase();
  if (b.startsWith("-") || b.includes("neg")) return "var(--color-negative)";
  if (b === "0" || b.includes("zero")) return "var(--color-positive)";
  return "var(--color-warning)";
}

export function DeltaHistogram({ data }: { data: Datum[] }) {
  const sorted = [...data].sort((a, b) => bucketSortKey(a.bucket) - bucketSortKey(b.bucket));

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={sorted} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <XAxis
            dataKey="bucket"
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            axisLine={{ stroke: "var(--color-border)" }}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            axisLine={{ stroke: "var(--color-border)" }}
            tickLine={false}
            allowDecimals={false}
          />
          <Tooltip
            cursor={{ fill: "var(--color-accent)", opacity: 0.3 }}
            contentStyle={{
              background: "var(--color-popover)",
              border: "1px solid var(--color-border)",
              borderRadius: 8,
              color: "var(--color-popover-foreground)",
              fontSize: 12,
            }}
          />
          <Bar dataKey="count" radius={[4, 4, 0, 0]}>
            {sorted.map((d) => (
              <Cell key={d.bucket} fill={colorFor(d.bucket)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
