/**
 * Exponential backoff with "full jitter" (AWS's term for it) — jitter is
 * applied across the *whole* range [0, cap], not just added as noise on top
 * of the exponential value. Additive jitter still lets retries cluster;
 * full jitter is what actually breaks synchronized retry storms when many
 * payments fail at once (e.g. a gateway blip during a traffic spike).
 */
export function computeBackoffMs(
  attempt: number, // 1-indexed retry attempt
  baseMs = 500,
  capMs = 60_000,
): number {
  const exp = Math.min(capMs, baseMs * 2 ** (attempt - 1));
  return Math.floor(Math.random() * exp);
}
