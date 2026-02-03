from katalog.constants.metadata import (
    ASSET_CANONICAL_URI,
    ASSET_ACTOR_ID,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    ASSET_NAMESPACE,
)

asset_filter_fields = {
    str(ASSET_ID): ("a.id", "int"),
    str(ASSET_ACTOR_ID): ("a.actor_id", "int"),
    str(ASSET_NAMESPACE): ("a.namespace", "str"),
    str(ASSET_EXTERNAL_ID): ("a.external_id", "str"),
    str(ASSET_CANONICAL_URI): ("a.canonical_uri", "str"),
}

asset_sort_fields = {
    str(ASSET_ID): "a.id",
    str(ASSET_ACTOR_ID): "a.actor_id",
    str(ASSET_NAMESPACE): "a.namespace",
    str(ASSET_EXTERNAL_ID): "a.external_id",
    str(ASSET_CANONICAL_URI): "a.canonical_uri",
}
