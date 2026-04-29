"""Shared user-facing help text for CLI and MCP surfaces."""

CLI_APP_HELP = "Katalog CLI"

WORKSPACE_OPTION_HELP = (
    "Path to workspace folder to use (or set via KATALOG_WORKSPACE)."
)
JSON_OPTION_HELP = "Output JSON instead of formatted text"
READ_ONLY_OPTION_HELP = "Run in read-only mode (runtime capabilities are read-only)"
SERVER_COMMAND_HELP = "Start the local API/UI server for the current workspace."

ACTORS_GROUP_HELP = (
    "Configure and run data connectors (actors), e.g. local folders or cloud sources."
)
ASSETS_GROUP_HELP = "List and inspect tracked files (assets)."
COLLECTIONS_GROUP_HELP = (
    "View saved asset groups (collections) and what they contain."
)
CHANGESETS_GROUP_HELP = (
    "Inspect operation history (changesets), e.g. scans and processor runs."
)
PROCESSORS_GROUP_HELP = (
    "Run processing jobs on assets, e.g. hashing or metadata extraction."
)
WORKFLOWS_GROUP_HELP = "Run workflow pipelines from workflow TOML files."
METADATA_GROUP_HELP = (
    "Search asset properties (metadata), e.g. path, MIME type, and timestamps."
)
VIEWS_GROUP_HELP = "List and inspect asset views, including runtime/plugin-defined views."

MCP_INSTRUCTIONS = (
    "Read-only access to katalog workspace data. "
    "Use these tools to list and inspect tracked files (assets), saved asset groups "
    "(collections), data connectors (actors), asset properties (metadata), and views."
)

MCP_VIEWS_LIST_DESC = "List available asset views."
MCP_VIEWS_GET_DESC = "Get asset view configuration by view id."
MCP_ASSETS_LIST_DESC = (
    "List tracked files (assets) for a view, with pagination, filtering, and sorting."
)
MCP_ASSETS_GROUPED_DESC = "List grouped assets by a group_by column key."
MCP_ASSETS_GET_DESC = (
    "Get one tracked file (asset) with all metadata entries, including removed rows."
)
MCP_COLLECTIONS_LIST_DESC = "List saved asset groups (collections)."
MCP_COLLECTIONS_GET_DESC = "Get collection details by id."
MCP_COLLECTIONS_LIST_ASSETS_DESC = "List assets that belong to a collection."
MCP_ACTORS_LIST_DESC = "List data connectors (actors)."
MCP_ACTORS_GET_DESC = (
    "Get data connector (actor) details by id, including related changesets."
)
MCP_METADATA_SCHEMA_EDITABLE_DESC = "Get editable metadata JSON schema and UI schema."
MCP_METADATA_REGISTRY_DESC = "Get metadata registry indexed by numeric key id."
MCP_METADATA_SEARCH_DESC = (
    "Search asset properties (metadata) with FTS or semantic/hybrid modes."
)
MCP_CHANGESETS_LIST_DESC = "List operation history entries (changesets)."
MCP_CHANGESETS_GET_DESC = "Get one changeset with buffered log events and running status."
MCP_CHANGESETS_CHANGES_DESC = (
    "List raw or diff metadata changes for a changeset or inclusive changeset range."
)
MCP_SYSTEM_STATS_DESC = "Get workspace and database size statistics."
MCP_PLUGINS_LIST_DESC = "List discovered plugins."
MCP_PLUGINS_GET_CONFIG_SCHEMA_DESC = "Get plugin configuration schema by plugin id."
MCP_ACTORS_GET_CONFIG_SCHEMA_DESC = (
    "Get actor configuration schema and current values by actor id."
)
MCP_WORKFLOWS_LIST_DESC = "List discovered workflows with status details."
MCP_WORKFLOWS_GET_DESC = "Get workflow details by file name."
