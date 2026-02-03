import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { changesetEventsUrl } from "../api/client";
import type { Changeset, ChangesetStatus } from "../types/api";

export interface ChangesetProgress {
  id: number;
  message: string | null;
  status: ChangesetStatus;
  data: Record<string, unknown> | null;
  queued: number | null;
  running: number | null;
  finished: number | null;
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
  source: EventSource;
  progress: ChangesetProgress;
}

export const ChangesetProgressProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [active, setActive] = useState<ChangesetProgress[]>([]);
  const trackers = useRef<Map<number, InternalTracker>>(new Map());

  const removeTracker = useCallback((id: number) => {
    const current = trackers.current.get(id);
    if (current) {
      current.source.close();
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
              status: "in_progress",
              data: null,
              queued: null,
              running: null,
              finished: null,
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

  const parseProgressLog = (line: string) => {
    const match = line.match(/tasks_progress\s+queued=(\d+)\s+running=(\d+)\s+finished=(\d+)/);
    if (!match) {
      return null;
    }
    return {
      queued: Number(match[1]),
      running: Number(match[2]),
      finished: Number(match[3]),
    };
  };

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
        status: changeset.status,
        data: (changeset.data as Record<string, unknown> | null) ?? null,
        queued: null,
        running: null,
        finished: null,
      };
      upsertProgress(progress);

      const url = changesetEventsUrl(changeset.id);
      const source = new EventSource(url);

      const handleLog = (event: MessageEvent) => {
        const line = typeof event.data === "string" ? event.data : String(event.data);
        const parsed = parseProgressLog(line);
        if (!parsed) {
          return;
        }
        upsertProgress({ id: progress.id, ...parsed });
      };

      const handleChangeset = (event: MessageEvent) => {
        try {
          const payload = JSON.parse(event.data) as Changeset;
          upsertProgress({
            id: payload.id,
            message: payload.message ?? progress.message,
            status: payload.status,
            data: (payload.data as Record<string, unknown> | null) ?? progress.data,
          });
          if (payload.status !== "in_progress") {
            removeTracker(payload.id);
          }
        } catch {
          // ignore malformed update
        }
      };

      source.addEventListener("log", handleLog);
      source.addEventListener("changeset", handleChangeset);
      source.onerror = () => {
        // transient errors: keep progress but close source to avoid loops
        source.close();
        trackers.current.delete(changeset.id);
      };

      trackers.current.set(changeset.id, { source, progress });
    },
    [removeTracker, upsertProgress],
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
      trackers.current.forEach((tracker) => tracker.source.close());
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
