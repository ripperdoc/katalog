# KATALOG

Katalog is an app intended for people and companies that have that feeling that their digital
content is hard to keep track of: unstructured, duplicated, spread over filesystems, external
drives, cloud storage and publishing tools. Over time, it's easy to lose control and oversight.

There are many apps in this space focusing on different aspects. This is our take on what Katalog
should provide:

- Ability to catalogue (scan) user created digital assets, e.g. documents, photos, media or
  ultimately, any file.
- Can connect many content sources; local folders, network drivers, external drives, cloud storage,
  FTPs, CMS:es and support an easy plugin-architecture to connect more. These sources can be scanned
  once or repeatedly, depending on use case.
- Can analyze and process each asset with plugins, AI or commands, in order to build metadata or
  generated content.
- Provide an interface to search, browse and filter the assets
- Analyze the catalog in order to organize it; uncover forgotten content, de-duplicate, build
  version histories
- Provide various overviews, such as where assets tend to be, how well it's backed up, what projects
  dominate, etc.
- Ultimately, to also provide the ability to see where content has been published and make it easy
  to manage assets across all systems

## What it is not:

- A new Spotlight. There are many great UIs for quickly finding and using files on an OS. These are
  better suited for quick access, and often also aim at finding all possible files and executables
  on a system.
- A backup or file management solution. There are great, robust tools for backup, syncing, copying
  and cloning. Katalog does not aim to recreate these - but it might provide shortcuts or interfaces
  to such tools.

## Who is it for?

- Content creators who are losing overview of all their content, especially with cloud and remote
  storage becoming the norm
- Archivists and data hoarders who want to manage their archives across many types of media
- Small businesses needing a flexible Digital Asset Management solution

## Principles for user experience

- Performant
- Don't load/process when not needed (e.g. cache)
- Give full access to data, don't dumb down
- Good default settings, but advanced customization possible
- Feels and is safe to use:
  - Can see in advance what large operations will do
  - Can undo small and large operations

# Usage

## Local UI

The `ui/` folder contains a lightweight React single-page app (Vite + TypeScript) that runs next to
the FastAPI backend for local exploration.

1. Start the backend via the CLI so FastAPI exposes `http://localhost:8000`:

```bash
python -m katalog.cli ./hg_workspace
```

Replace `./hg_workspace` with any workspace directory that includes `katalog.toml`. 2. Install UI
dependencies and launch the dev server (served on <http://localhost:5173>):

```bash
cd ui
npm install
npm run dev
```

The Vite dev proxy forwards `/api/*` calls to the FastAPI server, so no extra CORS setup is
needed. 3. (Optional) When serving the built UI elsewhere, set `VITE_API_BASE_URL` before
`npm run dev` or `npm run build` so API calls target the correct backend, e.g.:

```bash
VITE_API_BASE_URL="http://localhost:8000" npm run build
```

The UI currently lets you enter a source id, query `/files/{provider_id}` with the `flat` or
`complete` view, and render each file record with its metadata payload.
