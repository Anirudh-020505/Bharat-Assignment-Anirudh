// ============================================================================
// Money formatting — paise (1/100 INR) as BigInt. NEVER use Number for math.
// Backend always returns Decimal-as-string; we keep that contract end-to-end.
// ============================================================================

/** Convert a paise value (string|number|bigint) to a localized "₹X,XXX.XX" string with sign. */
export function paiseToInr(paise: string | number | bigint): string {
  let p: bigint;
  if (typeof paise === "bigint") p = paise;
  else if (typeof paise === "string") p = BigInt(paise);
  else p = BigInt(Math.round(paise));

  const negative = p < 0n;
  const abs = negative ? -p : p;
  const rupees = abs / 100n;
  const remainder = abs % 100n;

  // Indian grouping: 1,23,45,678.90  (last 3 digits, then groups of 2)
  const rupeesStr = rupees.toString();
  const grouped = formatIndianGrouping(rupeesStr);
  const paiseStr = remainder.toString().padStart(2, "0");

  return `${negative ? "-" : ""}₹${grouped}.${paiseStr}`;
}

/** Indian numbering system grouping (lakh/crore). */
function formatIndianGrouping(n: string): string {
  if (n.length <= 3) return n;
  const last3 = n.slice(-3);
  const rest = n.slice(0, -3);
  return rest.replace(/\B(?=(\d{2})+(?!\d))/g, ",") + "," + last3;
}

/** Sign of a paise value, without converting to Number. */
export function paiseSign(paise: string | bigint): -1 | 0 | 1 {
  const p = typeof paise === "bigint" ? paise : BigInt(paise);
  if (p < 0n) return -1;
  if (p > 0n) return 1;
  return 0;
}

/** Bucket label → numeric midpoint (Number is fine here, charts only). */
export function bucketSortKey(bucket: string): number {
  const m = bucket.match(/-?\d+/g);
  if (!m) return 0;
  const nums = m.map(Number);
  if (nums.length === 1) return nums[0];
  return (nums[0] + nums[1]) / 2;
}
