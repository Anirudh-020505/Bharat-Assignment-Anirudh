import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { Confidence } from "@/lib/api";

const STYLES: Record<Confidence, string> = {
  HIGH: "bg-positive-bg text-positive border-positive/40",
  MEDIUM: "bg-warning-bg text-warning border-warning/40",
  LOW: "bg-muted text-muted-foreground border-border",
};

export function ConfidenceBadge({ level }: { level: Confidence }) {
  return (
    <Badge variant="outline" className={cn("font-mono text-[10px] tracking-wide", STYLES[level])}>
      {level}
    </Badge>
  );
}
