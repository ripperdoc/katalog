import importlib
from datetime import datetime, timezone
from typing import Optional

from katalog.clients.base import SourceClient
from katalog.models import FileRecord
from katalog.processors.base import Processor


def import_processor_class(package_path: str) -> type[Processor]:
    parts = package_path.rsplit(".", 1)
    module = importlib.import_module(parts[0])
    ProcessorClass = getattr(module, parts[1])
    return ProcessorClass


def import_client_class(package_path: str) -> type[SourceClient]:
    parts = package_path.rsplit(".", 1)
    module = importlib.import_module(parts[0])
    ClientClass = getattr(module, parts[1])
    return ClientClass


def populate_accessor(record: FileRecord, source_map: dict[str, SourceClient]) -> None:
    if not record or not source_map:
        return None
    client = source_map.get(record.source_id)
    if not client:
        return None
    record.attach_accessor(client.get_accessor(record))


def timestamp_to_utc(ts: float | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.utcfromtimestamp(ts)


def sort_processors(
    proc_map: dict[str, type[Processor]],
) -> list[tuple[str, type[Processor]]]:
    """
    Topologically sort processors by their data-field dependencies.
    Each processor declares .dependencies (FileRecord fields it reads)
    and .outputs (fields it writes). Producers must run before consumers.
    """
    # Map each field to processors that produce it
    field_to_producers: dict[str, set[str]] = {}
    for name, cls in proc_map.items():
        for out in cls.outputs:
            field_to_producers.setdefault(out, set()).add(name)
    # Build dependency graph: proc_name -> set of producer proc_names
    deps: dict[str, set[str]] = {name: set() for name in proc_map}
    for name, cls in proc_map.items():
        for field in cls.dependencies:
            producers = field_to_producers.get(field, set())
            deps[name].update(producers)
    # Kahn's algorithm
    sorted_list: list[tuple[str, type[Processor]]] = []
    while deps:
        ready = [n for n, d in deps.items() if not d]
        if not ready:
            raise RuntimeError(f"Circular dependency among processors: {deps}")
        for n in ready:
            sorted_list.append((n, proc_map[n]))
            deps.pop(n)
            for other in deps.values():
                other.discard(n)
    return sorted_list


def parse_google_drive_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a Google Drive ISO8601 date string (e.g. '2017-10-24T15:01:04.000Z') to a Python datetime (UTC).
    Returns None if input is None or invalid.
    """
    if not dt_str:
        return None
    try:
        # Google returns ISO8601 with 'Z' for UTC
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        try:
            # Fallback: sometimes no microseconds
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return None
