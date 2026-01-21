import { useEffect, useState } from "react";
import { fetchMetadataRegistry, fetchActors } from "../api/client";
import type { MetadataRegistryEntry, Actor, MetadataRegistryResponse } from "../types/api";

type RegistryData = {
  metadataById: Record<number, MetadataRegistryEntry>;
  actorsById: Record<number, Actor>;
};

let registryCache: RegistryData | null = null;
let registryPromise: Promise<RegistryData> | null = null;

async function loadRegistry(): Promise<RegistryData> {
  const [metadataResponse, actorsResponse] = await Promise.all([
    fetchMetadataRegistry(),
    fetchActors(),
  ]);

  const actorsById = actorsResponse.actors.reduce<Record<number, Actor>>((acc, actor) => {
    acc[actor.id] = actor;
    return acc;
  }, {});

  return {
    metadataById: (metadataResponse as MetadataRegistryResponse).registry,
    actorsById,
  };
}

export async function getRegistry(): Promise<RegistryData> {
  if (registryCache) {
    return registryCache;
  }

  if (!registryPromise) {
    registryPromise = loadRegistry().then((data) => {
      registryCache = data;
      return data;
    });
  }

  return registryPromise;
}

export function useRegistry(): {
  data: RegistryData | null;
  loading: boolean;
  error: Error | null;
} {
  const [data, setData] = useState<RegistryData | null>(registryCache);
  const [loading, setLoading] = useState<boolean>(!registryCache);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let isActive = true;

    getRegistry()
      .then((result) => {
        if (!isActive) return;
        setData(result);
        setLoading(false);
      })
      .catch((err: Error) => {
        if (!isActive) return;
        setError(err);
        setLoading(false);
      });

    return () => {
      isActive = false;
    };
  }, []);

  return { data, loading, error };
}

export function clearRegistryCache(): void {
  registryCache = null;
  registryPromise = null;
}
