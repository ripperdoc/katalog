from katalog.constants.metadata import ASSET_CANONICAL_URI, ASSET_EXTERNAL_ID, ASSET_ID

asset_filter_fields = {
    str(ASSET_ID): ("a.id", "int"),
    str(ASSET_EXTERNAL_ID): ("a.external_id", "str"),
    str(ASSET_CANONICAL_URI): ("a.canonical_uri", "str"),
}

asset_sort_fields = {
    str(ASSET_ID): "a.id",
    # Actor sort temporarily disabled to avoid expensive lookups; see list_assets_for_view.
    # str(ASSET_ACTOR_ID): "asset_actor_id",
    str(ASSET_EXTERNAL_ID): "a.external_id",
    str(ASSET_CANONICAL_URI): "a.canonical_uri",
}
