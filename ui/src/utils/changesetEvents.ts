import { changesetEventsUrl } from "../api/client";
import type { ChangesetEvent } from "../types/api";

const EVENT_TYPES = [
  "changeset_progress",
  "changeset_status",
  "changeset_start",
  "log",
  "heartbeat",
] as const;

export function subscribeChangesetEvents(
  changesetId: number,
  onEvent: (event: ChangesetEvent) => void,
  onError?: (event: Event) => void,
) {
  const source = new EventSource(changesetEventsUrl(changesetId));
  const handler = (event: MessageEvent) => {
    try {
      const payload = JSON.parse(event.data) as ChangesetEvent;
      onEvent(payload);
    } catch {
      // ignore malformed event
    }
  };

  EVENT_TYPES.forEach((type) => source.addEventListener(type, handler));
  if (onError) {
    source.onerror = onError;
  }

  return () => {
    EVENT_TYPES.forEach((type) => source.removeEventListener(type, handler));
    source.close();
  };
}
