import { useCallback, useEffect } from "react";
import type { Dispatch, SetStateAction } from "react";
import { useSearchParams } from "react-router-dom";

export function useStringSearchParamState(
  key: string,
  defaultValue: string,
): [string, Dispatch<SetStateAction<string>>] {
  const [searchParams, setSearchParams] = useSearchParams();
  const value = searchParams.get(key) ?? defaultValue;

  useEffect(() => {
    if (searchParams.get(key) !== null) {
      return;
    }
    const nextSearchParams = new URLSearchParams(searchParams);
    nextSearchParams.set(key, defaultValue);
    setSearchParams(nextSearchParams, { replace: true });
  }, [defaultValue, key, searchParams, setSearchParams]);

  const setValue = useCallback<Dispatch<SetStateAction<string>>>(
    (nextValueOrUpdater) => {
      const currentValue = searchParams.get(key) ?? defaultValue;
      const nextValue =
        typeof nextValueOrUpdater === "function"
          ? nextValueOrUpdater(currentValue)
          : nextValueOrUpdater;
      const nextSearchParams = new URLSearchParams(searchParams);
      nextSearchParams.set(key, nextValue);
      setSearchParams(nextSearchParams, { replace: true });
    },
    [defaultValue, key, searchParams, setSearchParams],
  );

  return [value, setValue];
}
