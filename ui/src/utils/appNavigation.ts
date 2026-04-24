import type { NavigateFunction } from "react-router-dom";

let navigateImpl: NavigateFunction | null = null;

const normalizeBasePath = (basePath: string): string => {
  if (!basePath || basePath === "/") {
    return "";
  }

  return basePath.endsWith("/") ? basePath.slice(0, -1) : basePath;
};

const isAbsoluteUrl = (to: string): boolean => /^(?:[a-z]+:)?\/\//i.test(to);

export const getAppHref = (to: string): string => {
  if (!to) {
    return normalizeBasePath(import.meta.env.BASE_URL || "/") || "/";
  }

  if (isAbsoluteUrl(to)) {
    return to;
  }

  const basePath = normalizeBasePath(import.meta.env.BASE_URL || "/");
  const normalizedTo = to.startsWith("/") ? to : `/${to}`;
  return `${basePath}${normalizedTo}` || "/";
};

export const setAppNavigate = (navigate: NavigateFunction | null) => {
  navigateImpl = navigate;
};

export const navigateInApp = (to: string) => {
  if (isAbsoluteUrl(to)) {
    window.location.assign(to);
    return;
  }

  if (navigateImpl) {
    navigateImpl(to);
    return;
  }

  window.location.assign(getAppHref(to));
};
