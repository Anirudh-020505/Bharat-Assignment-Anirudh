import { cn } from "@/lib/utils";
import { Card } from "@/components/ui/card";
import type { ReactNode } from "react";

export type KPITone = "negative" | "warning" | "positive" | "info" | "neutral";

interface KPICardProps {
  title: string;
  value: ReactNode;
  subtitle?: ReactNode;
  tone?: KPITone;
  icon?: ReactNode;
}

const TONE_CLASSES: Record<KPITone, { value: string; ring: string }> = {
  negative: { value: "text-negative", ring: "ring-negative/30" },
  warning: { value: "text-warning", ring: "ring-warning/30" },
  positive: { value: "text-positive", ring: "ring-positive/30" },
  info: { value: "text-info", ring: "ring-info/30" },
  neutral: { value: "text-foreground", ring: "ring-border" },
};

export function KPICard({ title, value, subtitle, tone = "neutral", icon }: KPICardProps) {
  const t = TONE_CLASSES[tone];
  return (
    <Card className={cn("relative overflow-hidden p-5 ring-1", t.ring)}>
      <div className="flex items-start justify-between">
        <div className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {title}
        </div>
        {icon ? <div className="text-muted-foreground">{icon}</div> : null}
      </div>
      <div className={cn("mt-3 font-mono text-3xl font-semibold tabular-nums", t.value)}>
        {value}
      </div>
      {subtitle ? (
        <div className="mt-1 text-sm text-muted-foreground tabular-nums">{subtitle}</div>
      ) : null}
    </Card>
  );
}
