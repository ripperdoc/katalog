# Using Katalog MCP with ChatGPT

This guide shows how to connect a running `katalog` server as a custom MCP app in ChatGPT.

## What ChatGPT currently supports

- MCP custom apps are configured in ChatGPT "Developer mode".
- Local MCP endpoints are not supported by ChatGPT right now.
- MCP custom apps are currently web-only in ChatGPT (not mobile).

Because of this, your `katalog` MCP endpoint must be reachable over HTTPS from the internet or your
workspace network.

## 1. Run Katalog with MCP enabled

Start the normal API server and enable MCP:

```bash
katalog -w /path/to/workspace server --with-mcp
```

MCP is mounted at:

`http://127.0.0.1:8000/mcp`

## 2. Expose `/mcp` on HTTPS

ChatGPT cannot connect directly to `localhost`, so expose your server through a secure remote URL.

For example, you might put `katalog` behind a reverse proxy and map:

- `https://your-domain.example/mcp` -> `http://127.0.0.1:8000/mcp`

Make sure:

- HTTPS is enabled.
- Any required auth method is configured consistently with your ChatGPT app setup.
- The endpoint is reachable from the browser where ChatGPT runs.

## 3. Enable developer mode in ChatGPT

In ChatGPT web:

1. Open `Settings -> Apps -> Advanced Settings`.
2. Enable Developer mode.

For workspace plans, your admin may need to enable this first.

## 4. Create the custom MCP app

In ChatGPT:

1. Go to `Settings -> Apps -> Create`.
2. Enter your MCP endpoint URL, for example:
   - `https://your-domain.example/mcp`
3. Choose the authentication method (if any) and complete setup.
4. Save/create the app.

On workspace plans, admins/owners may need to publish the app before everyone can use it.

## 5. Use it in chat

Open a new chat and select your app from the tools picker (or invoke it in prompt text). ChatGPT
can then call the read tools exposed by `katalog`.

## Current Katalog MCP scope

Current tools are read-only:

- Views: list/get/list assets
- Assets: list/get/grouped list
- Collections: list/get/list assets
- Actors: list/get
- Metadata: editable schema/registry

## Troubleshooting

- "Cannot connect" or timeout:
  - Verify the remote URL is reachable in browser.
  - Confirm your proxy forwards `/mcp` correctly.
- App not visible in chat:
  - Ensure developer mode is enabled for your account.
  - On workspace plans, confirm the app is published and access is granted.
- Calls fail after server changes:
  - Refresh/review app actions in ChatGPT workspace settings.

## References

- https://help.openai.com/en/articles/11487775-connectors-in-chatgpt
- https://help.openai.com/en/articles/12584461
