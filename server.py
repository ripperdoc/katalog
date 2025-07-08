
from fastapi import FastAPI
from utils import timestamp_to_utc
from sqlmodel import SQLModel, Session, create_engine, select
from models import FileRecord
import tomllib
import importlib

app = FastAPI()

DATABASE_URL = "sqlite:///katalog.db"
engine = create_engine(DATABASE_URL, echo=False)

# Create tables if they don't exist
SQLModel.metadata.create_all(engine)


# Read sources from katalog.toml and scan all sources
@app.post("/initialize")
def initialize_sources():
    with open("katalog.toml", "rb") as f:
        config = tomllib.load(f)
    sources = config.get("sources", [])
    # Drop and recreate only the FileRecord table
    from sqlmodel import SQLModel
    table = SQLModel.metadata.tables.get("filerecord")
    if table is not None:
        table.drop(engine, checkfirst=True)
        table.create(engine, checkfirst=True)
    with Session(engine) as session:
        for source in sources:
            source_type = source.get("type")
            if source_type == "localfs":
                # Import the client dynamically (for plugin architecture)
                module = importlib.import_module("client_localfs")
                ClientClass = getattr(module, "FilesystemClient")
                client = ClientClass(source["root_path"])
                for record in client.scan(source_name="localfs", timestamp_to_utc=timestamp_to_utc):
                    if not getattr(record, "path", None):
                        continue
                    session.add(record)
        session.commit()
    return {"status": "scan complete"}

@app.get("/list")
def list_local_files():
    with Session(engine) as session:
        results = session.exec(select(FileRecord)).all()
        return [r.model_dump() for r in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)
