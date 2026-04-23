# Plugin Authorization in Katalog

This page explains how authorization works for source plugins in Katalog, what plugin authors must
implement, and what users must provide.

## Scope

- Authorization currently applies to `SourcePlugin` actors.
- Authorization is a write operation (`@requires_write_access`), so it is unavailable in read-only
  runtime modes.
- CLI, HTTP, and UI all go through the same API behavior.

## Authorization Flow

1. User triggers authorization for a source actor.
2. Katalog calls `auth_api(actor)` in `katalog.api.system`.
3. The source plugin instance is loaded and `plugin.authorize()` is called without callback data.
4. Plugin returns either:
   - an auth URL (`http`/`https`) if user interaction is required, or
   - a non-URL value (for example `"authorized"`) if already authorized.
5. User completes provider consent at the returned URL.
6. Provider redirects back to Katalog:
   - `GET/POST /api/auth/{actor}`
7. Katalog detects callback query params (`code` / `error`) and calls
   `plugin.authorize(authorization_response=<full callback URL>)`.
8. Plugin exchanges code for token and persists credentials.

## Current Endpoint

- Dual-mode auth endpoint:
  - `GET /api/auth/{actor}`
  - `POST /api/auth/{actor}`
- Behavior:
  - No callback params: starts auth flow (may redirect to provider URL).
  - With callback params (`code` or `error`): handles OAuth callback and token exchange.

## Plugin Author Requirements

Implement `SourcePlugin.authorize(self, **kwargs) -> str` with two modes:

1. Start mode (`authorization_response` not provided):
   - Return auth URL if login/consent is required.
   - Return `"authorized"` (or any non-URL string) if token is already usable.

2. Callback mode (`authorization_response` provided):
   - Parse callback URL, fetch token, persist token.
   - Return `"authorized"` when successful.

Also implement `is_ready()` so scans fail fast with a clear message if credentials are
missing/invalid.

## Recommended Storage Pattern

- Store per-actor credentials under `actor_path(actor_id)`, for example:
  - `token.json`
  - `client_secret.json`
- Optionally support `client_secret_path` in actor config for external secret locations.

## User Responsibilities

Users must:

1. Configure the source actor (including any required client secret path/file).
2. Trigger authorization (`Authorize` link/button, HTTP, or CLI).
3. Complete provider consent.
4. Retry scan (`Run`) after callback success.

## Minimal `authorize()` Skeleton

```python
def authorize(self, **kwargs) -> str:
    if "authorization_response" in kwargs:
        # callback mode: exchange code -> token, persist token
        return "authorized"

    # start mode
    if self._has_valid_token():
        return "authorized"
    return self._build_authorization_url()
```

## Notes for OAuth Library Behavior

- Some OAuth libraries are strict about:
  - HTTP callbacks (`localhost` dev),
  - returned scope superset warnings.
- If your provider/library requires it, handle those settings explicitly in plugin code for local
  development.
