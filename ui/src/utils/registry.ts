import { useEffect, useState } from "react";
import { fetchMetadataRegistry, fetchProviders } from "../api/client";
import type { MetadataRegistryEntry, Provider, MetadataRegistryResponse } from "../types/api";

type RegistryData = {
  metadataById: Record<number, MetadataRegistryEntry>;
  providersById: Record<number, Provider>;
};

let registryCache: RegistryData | null = null;
let registryPromise: Promise<RegistryData> | null = null;

async function loadRegistry(): Promise<RegistryData> {
  const [metadataResponse, providersResponse] = await Promise.all([
    fetchMetadataRegistry(),
    fetchProviders(),
  ]);

  const providersById = providersResponse.providers.reduce<Record<number, Provider>>(
    (acc, provider) => {
      acc[provider.id] = provider;
      return acc;
    },
    {},
  );

  return {
    metadataById: (metadataResponse as MetadataRegistryResponse).registry,
    providersById,
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
