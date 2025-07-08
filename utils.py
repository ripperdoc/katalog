import datetime
from processors.base import Processor as _Processor


def timestamp_to_utc(ts: float | None) -> datetime.datetime | None:
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts)

def sort_processors(proc_map: dict[str, type[_Processor]]) -> list[type[_Processor]]:
    """
    Topologically sort processors by their data-field dependencies.
    Each processor declares .dependencies (FileRecord fields it reads)
    and .outputs (fields it writes). Producers must run before consumers.
    """
    # Map each field to processors that produce it
    field_to_producers: dict[str, set[str]] = {}
    for name, cls in proc_map.items():
        for out in getattr(cls, 'outputs', []):
            field_to_producers.setdefault(out, set()).add(name)
    # Build dependency graph: proc_name -> set of producer proc_names
    deps: dict[str, set[str]] = {name: set() for name in proc_map}
    for name, cls in proc_map.items():
        for field in getattr(cls, 'dependencies', []):
            producers = field_to_producers.get(field, set())
            deps[name].update(producers)
    # Kahn's algorithm
    sorted_list: list[type[_Processor]] = []
    while deps:
        ready = [n for n, d in deps.items() if not d]
        if not ready:
            raise RuntimeError(f"Circular dependency among processors: {deps}")
        for n in ready:
            sorted_list.append(proc_map[n])
            deps.pop(n)
            for other in deps.values():
                other.discard(n)
    return sorted_list