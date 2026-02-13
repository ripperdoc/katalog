from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "igshid",
    "_hsenc",
    "_hsmi",
    "mc_cid",
    "mc_eid",
    "cvid",
    "oicd",
}


def canonicalize_web_url(value: str) -> str:
    """Conservatively canonicalize HTTP/HTTPS URLs for stable identity."""
    url = value.strip()
    if not url:
        return ""

    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        return url
    if not parts.netloc:
        return url

    host = parts.hostname.lower() if parts.hostname else ""
    if not host:
        return url

    port = parts.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        netloc = host
    elif port is not None:
        netloc = f"{host}:{port}"
    else:
        netloc = host

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    filtered = [
        (k, v)
        for k, v in query_pairs
        if not k.lower().startswith("utm_")
        and not k.lower().startswith("stm_")
        and k.lower() not in TRACKING_QUERY_KEYS
    ]
    query = urlencode(filtered, doseq=True)

    return urlunsplit((scheme, netloc, parts.path or "", query, ""))
