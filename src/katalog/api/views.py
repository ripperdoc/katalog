from katalog.models.views import ViewSpec, get_view, list_views
from katalog.api.helpers import ApiError


async def get_view_api(view_id: str) -> ViewSpec:
    """Return a view spec by id or raise API not found."""
    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")
    return view


async def list_views_api() -> list[ViewSpec]:
    """List available view specs."""
    return list_views()
