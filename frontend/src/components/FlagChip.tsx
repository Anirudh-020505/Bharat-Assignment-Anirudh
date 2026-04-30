import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { FLAG_LABELS, FLAG_DESCRIPTIONS } from "@/lib/flags";
import type { FlagKey } from "@/lib/api";

export function FlagChip({ flag }: { flag: FlagKey }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge variant="secondary" className="cursor-help text-[10px] font-medium">
          {FLAG_LABELS[flag]}
        </Badge>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        <p className="text-xs">{FLAG_DESCRIPTIONS[flag]}</p>
      </TooltipContent>
    </Tooltip>
  );
}
