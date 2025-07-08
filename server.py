from fastapi import FastAPI, HTTPException
from utils import timestamp_to_utc, sort_processors
from sqlmodel import SQLModel, Session, create_engine, select
from models import FileRecord, ProcessorResult
import tomllib
import importlib
import datetime

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
        # Processing phase: import processors listed in TOML and order by dependencies
        proc_cfgs = config.get("processors", [])
        proc_map: dict[str, type] = {}
        for proc in proc_cfgs:
            mod_name = proc.get("module")
            cls_name = proc.get("class")
            try:
                module = importlib.import_module(mod_name)
                ProcCls = getattr(module, cls_name)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Cannot load processor {mod_name}.{cls_name}: {e}")
            proc_map[ProcCls.name] = ProcCls
        ordered = sort_processors(proc_map)
        # fetch all files
        files = session.exec(select(FileRecord)).all()
        for record in files:
            # load prior results for this file
            prevs = {pr.processor: pr for pr in session.exec(
                select(ProcessorResult).where(ProcessorResult.file_id == record.id)
            ).all()}
            for ProcCls in ordered:
                proc = ProcCls()
                prev = prevs.get(proc.name)
                prev_cache = prev.cache_key if prev else None
                if proc.should_run(record, prev_cache):
                    out = proc.run(record)
                    # update fields on FileRecord
                    for k, v in out.items():
                        setattr(record, k, v)
                    session.add(record)
                    # upsert ProcessorResult
                    new_key = proc.cache_key(record)
                    if prev:
                        prev.cache_key = new_key
                        prev.result = out
                        prev.ran_at = datetime.datetime.utcnow()
                        session.add(prev)
                    else:
                        pr = ProcessorResult(
                            file_id=record.id,  # type: ignore
                            processor=proc.name,
                            cache_key=new_key,
                            result=out
                        )
                        session.add(pr)
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
