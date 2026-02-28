_Current status: WIP, get in touch if you want to collaborate_

# KATALOG

Katalog is a tool for tracking digital assets, files, posts, web pages and more. Files tend gather
like dust, across file systems, old drives, cloud actors, websites. Finding something is hard, you
see duplication, versionitis, poor folder structures and naming schemes.

There is an endless stream of tools that try to attack specific parts of this problem: file
searching, syncing, file sorters, AI analyzers, digital asset management, and more. My observation
has been that they tend to solve a specific task for a specific storage system, without exposing the
data to other tools. A lot of them need to re-invent the same wheel over and over - databases,
syncing, fetching, scanning, file conversion.

Instead, if we find a common way of storing the metadata about digital assets, we can make a
pluggable system that both can ingest data (`sources`), that can process that data (`processors`)
and visualize that data (`views`). It allows us to keep the data consistent and over time just add
(or) remove processors. With AI, this pattern becomes even more powerful, as the data is what
carries true value, and users (with or without AI help) can easily create snippets of code that
ingest, process or analyze parts or all of that data. Often a thin wrapper around best-practice
open-source tools and SDKs.

## CLI (Quick Start)

The `katalog` command now includes both server and client modes.

Start the server:

```bash
katalog -w /path/to/workspace server
```

Start the server with MCP endpoint enabled (mounted at `/mcp`):

```bash
katalog -w /path/to/workspace server --with-mcp
```

For ChatGPT connector setup, see `docs/mcp-chatgpt.md`.

Use client commands (no server needed) to call the API layer directly:

```bash
katalog -w /path/to/workspace actors list
katalog -w /path/to/workspace actors show 1
```

### ONNX runtime for local embeddings (macOS)

Kreuzberg local embeddings depend on ONNX Runtime. Install it with `uv`:

```bash
uv add onnxruntime
```

In some macOS environments, you also need to set `ORT_DYLIB_PATH` so the runtime can find the
library:

```bash
export ORT_DYLIB_PATH="$(uv run python -c 'import onnxruntime, pathlib; print(next(pathlib.Path(onnxruntime.__file__).parent.rglob(\"libonnxruntime*.dylib\")))')"
```

## Example use cases

- Catalogue (scan) all digital assets you've created across both local and remote storage actors
- Find files using filters, text search or semantic search (vector search)
- Letting AI access your data across providers, creating a local-first RAG solution
- Browse and list files faster and with more detail, even if the cloud actor is slow, broken or
  offline
- Merge metadata for the same files from multiple sources, like the posts from a webpage with the
  images in a Dropbox and the PDFs in a local drive
- Efficiently process metadata, like making thumbnails, tags, fix filenames or more, and connect AI
  APIs to do this with less coding and better outcomes
- Find sets of files and batch export in some other format
- Be a digital archeologist, find forgotten files and file formats, download or create plugins to
  convert them to new formats, de-duplicate, build version histories
- Visualize files in different dimensions - as tables, folders, events over time, image galleries
- Provide useful statistics, such as typical and total file size, type, projects, folders
- Connect to backup and sync actors, to gather metadata over time, tracking how files have changed
  over the years
- Ultimately, to also provide the ability to see where content has been published and make it easy
  to manage assets across all systems

## Who is it for?

- Content creators
- Archivists and data hoarders
- Small businesses
- Data analysts
- Data forensics
- OSINT

## Design principles

- Open source and open, pluggable ecosystem
- Local-first, helps you control _your_ data
- Fast and responsive
- Power-user and developer focused
- Keeps data safe: any operation can be undone

## Feature roadmap

### Source plugins

_Supported sources of digital assets_

- ✓ Local filesystems
- ✓ Google Drive
- ✓ List of URLs (web scanning seeds)
- ✓ JSON list of documents (URLs + metadata mappings)
- ✓ HTTP URL recursive metadata fetcher (used during source recursion)
- ⏳ (planned) Compressed archives
- ⏳ (planned) List of data (e.g CSVs, Google Sheets)
- ⏳ (planned) Dropbox
- ⏳ (planned) OneDrive
- ⏳ (planned) S3 and compatible cloud storage
- ⏳ (planned) Wikis
- ⏳ (planned) Wordpress
- ⏳ (planned) Git repositories
- ⏳ (planned) Backups apps

### Processor plugins

_Supported processors of digital assets._

- ✓ MD5 content fingerprinting
- ✓ File type detection (magic)
- ✓ Duplicate finder
- ⏳ (planned) Create semantic embeddings
- ⏳ (planned) AI prompted file cleaning
- ⏳ (planned) Rule-based file renaming
- ⏳ (planned) Rule-based folder organizer
- ⏳ (planned) Summarize text content
- ⏳ (planned) Translate text content
- ⏳ (planned) Junk file detector
- ⏳ (planned) Thumbnail creator
- ⏳ (planned) Extract text content
- ⏳ (planned) Office docs reader
- ⏳ (planned) PDF reader

### Core features

- ✓ Web-based minimalistic UI
- ✓ Changeset-based append-only data model - any change can be undone
- ✓ Efficiently scan or crawl REST APIs
- ✓ Extensible metadata model
- ✓ Discover, import and configure plugins
- ✓ Efficient processing pipelines that stream results to UI
- ✓ Asset table view with free text search
- ✓ Can ingest 500k files and 10M metadata points
- ⏳ (planned) Semantic search using embeddings
- ⏳ (planned) Full filtering and sorting for all metadata
- ⏳ (planned) User-editable metadata
- ⏳ (planned) Custom grouped views, ability to define and save groups of files
- ⏳ (planned) Stats for assets
- ⏳ (planned) Export tools for tables and file data
- ⏳ (planned) Tools to write changes back to actors (e.g. rename in source)
- ⏳ (planned) Customized processing pipelines

See `TODO.md` for more details.

# Usage

## Local UI

The `ui/` folder contains a lightweight React single-page app (Vite + TypeScript) that runs next to
the FastAPI backend for local exploration.

1. Start the backend via the CLI so FastAPI exposes `http://localhost:8000`:

```bash
python -m katalog.cli --workspace workspace/path server
```

To expose MCP on the same process and port, add `--with-mcp`. The MCP endpoint will be available at
`http://localhost:8000/mcp`.

Replace `workspace/path` with any workspace directory that includes `katalog.db`.

2. Install UI dependencies and launch the dev server (served on <http://localhost:5173>):

```bash
cd ui
npm install
npm run dev
```

The Vite dev proxy forwards `/api/*` calls to the FastAPI server, so no extra CORS setup is needed.

3. (Optional) When serving the built UI elsewhere, set `VITE_API_BASE_URL` before `npm run dev` or
   `npm run build` so API calls target the correct backend, e.g.:

```bash
VITE_API_BASE_URL="http://localhost:8000" npm run build
```

The UI currently lets you enter a source id, query `/files/{actor_id}` with the `flat` or `complete`
view, and render each file record with its metadata payload.

## Python workflow API

Workflow runtime functions accept either a workflow TOML path or an in-memory `WorkflowSpec`
instance.

```python
from katalog.models import ActorType
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file

spec = WorkflowSpec(
    file_name="in-memory.workflow.toml",
    file_path="<in-memory>",
    workflow_id="library-ingest",
    name="Library ingest",
    description=None,
    version="1.0.0",
    actors=[
        WorkflowActorSpec(
            name="JSON document list",
            plugin_id="katalog.sources.json_list.JsonListSource",
            actor_type=ActorType.SOURCE,
            config={"json_file": "ingest/documents.json"},
            disabled=False,
        ),
        WorkflowActorSpec(
            name="HTTP recursive",
            plugin_id="katalog.sources.http_url.HttpUrlSource",
            actor_type=ActorType.SOURCE,
            config={},
            disabled=False,
        ),
    ],
)

# Run source scan and sync actors first.
result = await run_workflow_file(spec, sync_first=True)
```

When `HttpUrlSource` is present in the same workspace/workflow, URL assets emitted by `UrlListSource`
or `JsonListSource` can be recursively scanned to fetch HTTP metadata.

# AI policy

As a developer for 25+ years, I have very mixed feelings for AI-tools for coding. Some things are
great, others are terrible. With that said, I use them daily and this project is no exception. But
it's a very iterative process - bounce ideas, get proposal, reject proposal, new proposal, rewrite
manually, and so on.
