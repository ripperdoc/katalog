import importlib
from datetime import datetime, timezone
from typing import Optional

from katalog.clients.base import SourceClient
from katalog.models import FileRecord
from katalog.processors.base import Processor


def import_processor_class(package_path: str) -> type[Processor]:
    module_name, class_name = package_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if module_name.startswith("katalog."):
            raise
        module = importlib.import_module(f"katalog.{module_name}")
    ProcessorClass = getattr(module, class_name)
    return ProcessorClass


def import_client_class(package_path: str) -> type[SourceClient]:
    parts = package_path.rsplit(".", 1)
    module_name, class_name = parts[0], parts[1]
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if module_name.startswith("katalog."):
            raise
        module = importlib.import_module(f"katalog.{module_name}")
    ClientClass = getattr(module, class_name)
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
