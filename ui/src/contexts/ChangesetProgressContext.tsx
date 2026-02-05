import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { Changeset, ChangesetEvent, ChangesetStatus } from "../types/api";
import { subscribeChangesetEvents } from "../utils/changesetEvents";

export interface ChangesetProgress {
  id: number;
  message: string | null;
  logMessage: string | null;
  status: ChangesetStatus;
  data: Record<string, unknown> | null;
  queued: number | null;
  running: number | null;
  finished: number | null;
  kind: string | null;
}

type ProgressUpdate = Partial<ChangesetProgress> & { id: number };

interface ProgressContextValue {
  active: ChangesetProgress[];
  startTracking: (changeset: Changeset) => void;
  stopTracking: (id: number) => void;
  seedActive: (changesets: Changeset[]) => void;
}

const ChangesetProgressContext = createContext<ProgressContextValue | undefined>(undefined);

interface InternalTracker {
  cleanup: () => void;
}

export const ChangesetProgressProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [active, setActive] = useState<ChangesetProgress[]>([]);
  const trackers = useRef<Map<number, InternalTracker>>(new Map());

  const removeTracker = useCallback((id: number) => {
    const current = trackers.current.get(id);
    if (current) {
      current.cleanup();
      trackers.current.delete(id);
    }
    setActive((prev) => prev.filter((item) => item.id !== id));
  }, []);

  const upsertProgress = useCallback((update: ProgressUpdate) => {
    setActive((prev) => {
      const idx = prev.findIndex((p) => p.id === update.id);
      const base: ChangesetProgress =
        idx === -1
          ? {
              id: update.id,
              message: null,
              logMessage: null,
              status: "in_progress",
              data: null,
              queued: null,
              running: null,
              finished: null,
              kind: null,
            }
          : prev[idx];
      const merged: ChangesetProgress = { ...base };
      (Object.entries(update) as [keyof ChangesetProgress, any][]).forEach(([key, val]) => {
        if (val !== undefined) {
          (merged as any)[key] = val;
        }
      });
      if (idx === -1) {
        return [...prev, merged];
      }
      const next = [...prev];
      next[idx] = merged;
      return next;
    });
  }, []);

  const handleStatusEvent = useCallback(
    (payload: Changeset) => {
      upsertProgress({
        id: payload.id,
        message: payload.message ?? null,
        status: payload.status,
        data: (payload.data as Record<string, unknown> | null) ?? null,
      });
      if (payload.status !== "in_progress") {
        removeTracker(payload.id);
      }
    },
    [removeTracker, upsertProgress],
  );

  const handleEvent = useCallback(
    (evt: ChangesetEvent) => {
      if (evt.event === "changeset_progress") {
        const payload = evt.payload ?? {};
        upsertProgress({
          id: evt.changeset_id,
          queued:
            typeof payload["queued"] === "number" ? (payload["queued"] as number) : null,
          running:
            typeof payload["running"] === "number" ? (payload["running"] as number) : null,
          finished:
            typeof payload["finished"] === "number" ? (payload["finished"] as number) : null,
          kind: typeof payload["kind"] === "string" ? (payload["kind"] as string) : null,
        });
        return;
      }
      if (evt.event === "log") {
        const payload = evt.payload ?? {};
        const message = payload["message"];
        if (typeof message === "string" && message.trim().length > 0) {
          upsertProgress({
            id: evt.changeset_id,
            logMessage: message,
          });
        }
        return;
      }
      if (evt.event === "changeset_status" || evt.event === "changeset_start") {
        handleStatusEvent(evt.payload as Changeset);
      }
    },
    [handleStatusEvent, upsertProgress],
  );

  const startTracking = useCallback(
    (changeset: Changeset) => {
      if (changeset.status !== "in_progress") {
        return;
      }
      if (trackers.current.has(changeset.id)) {
        return;
      }

      const progress: ChangesetProgress = {
        id: changeset.id,
        message: changeset.message ?? null,
        logMessage: null,
        status: changeset.status,
        data: (changeset.data as Record<string, unknown> | null) ?? null,
        queued: null,
        running: null,
        finished: null,
        kind: null,
      };
      upsertProgress(progress);

      const cleanup = subscribeChangesetEvents(changeset.id, handleEvent);
      trackers.current.set(changeset.id, { cleanup });
    },
    [handleEvent, handleStatusEvent, removeTracker, upsertProgress],
  );

  const stopTracking = useCallback(
    (id: number) => {
      removeTracker(id);
    },
    [removeTracker],
  );

  const seedActive = useCallback(
    (changesets: Changeset[]) => {
      changesets
        .filter((c) => c.status === "in_progress")
        .forEach((c) => startTracking(c));
    },
    [startTracking],
  );

  useEffect(
    () => () => {
      trackers.current.forEach((tracker) => tracker.cleanup());
      trackers.current.clear();
    },
    [],
  );

  const value = useMemo(
    () => ({
      active,
      startTracking,
      stopTracking,
      seedActive,
    }),
    [active, startTracking, stopTracking, seedActive],
  );

  return (
    <ChangesetProgressContext.Provider value={value}>
      {children}
    </ChangesetProgressContext.Provider>
  );
};

export function useChangesetProgress() {
  const ctx = useContext(ChangesetProgressContext);
  if (!ctx) {
    throw new Error("useChangesetProgress must be used within ChangesetProgressProvider");
  }
  return ctx;
}
