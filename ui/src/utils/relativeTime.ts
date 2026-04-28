const DEFAULT_LOCALE = "en";

const rtf = new Intl.RelativeTimeFormat(DEFAULT_LOCALE, { numeric: "auto" });

type RelativeUnit =
  | "year"
  | "month"
  | "week"
  | "day"
  | "hour"
  | "minute"
  | "second";

const STEPS: ReadonlyArray<{ unit: RelativeUnit; seconds: number }> = [
  { unit: "year", seconds: 365 * 24 * 60 * 60 },
  { unit: "month", seconds: 30 * 24 * 60 * 60 },
  { unit: "week", seconds: 7 * 24 * 60 * 60 },
  { unit: "day", seconds: 24 * 60 * 60 },
  { unit: "hour", seconds: 60 * 60 },
  { unit: "minute", seconds: 60 },
  { unit: "second", seconds: 1 },
];

export function formatRelativeTime(
  timestampMs: number,
  nowMs: number = Date.now(),
): string {
  if (!Number.isFinite(timestampMs)) {
    return "";
  }

  const diffSeconds = (timestampMs - nowMs) / 1000;
  const absSeconds = Math.abs(diffSeconds);

  if (absSeconds < 5) {
    return "just now";
  }

  for (const step of STEPS) {
    if (absSeconds >= step.seconds || step.unit === "second") {
      const value = Math.round(diffSeconds / step.seconds);
      return rtf.format(value, step.unit);
    }
  }

  return "";
}
