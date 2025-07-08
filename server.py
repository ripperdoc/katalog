from fastapi import FastAPI
from client_localfs import LocalFSClient
from sqlmodel import SQLModel, Session, create_engine, select
from models import FileRecord
import datetime
from sqlalchemy import delete

app = FastAPI()

DATABASE_URL = "sqlite:///katalog.db"
engine = create_engine(DATABASE_URL, echo=False)

# Create tables if they don't exist
SQLModel.metadata.create_all(engine)

@app.post("/scan/local")
def scan_local_source(path: str):
    # Overwrite all records in the table
    with Session(engine) as session:
        session.execute(delete(FileRecord))
        session.commit()
        client = LocalFSClient(path)
        now = datetime.datetime.utcnow()
        for file_info in client.scan():
            if not file_info.get("path"):
                continue
            record = FileRecord(
                path=str(file_info.get("path")),
                size=file_info.get("size"),
                mtime=file_info.get("mtime"),
                ctime=file_info.get("ctime"),
                is_file=file_info.get("is_file", True),
                error=file_info.get("error"),
                scanned_at=now
            )
            session.add(record)
        session.commit()
    return {"status": "scan complete"}

@app.get("/scan/local/list")
def list_local_files():
    with Session(engine) as session:
        results = session.exec(select(FileRecord)).all()
        return [r.model_dump() for r in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)
