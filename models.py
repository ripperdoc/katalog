from sqlmodel import SQLModel, Field
from typing import Optional
import datetime

class FileRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    path: str
    size: Optional[int] = None
    mtime: Optional[float] = None  # Unix timestamp
    ctime: Optional[float] = None  # Unix timestamp
    is_file: Optional[bool] = True
    error: Optional[str] = None
    scanned_at: Optional[datetime.datetime] = None
