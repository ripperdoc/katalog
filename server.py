import json
from fastapi import FastAPI
from utils import sort_processors, import_client_class, import_processor_class
from sqlmodel import SQLModel, Session, create_engine, select
from models import FileRecord, ProcessorResult
import tomllib
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
    processors = config.get("processors", [])
    # Drop and recreate only the FileRecord table
    from sqlmodel import SQLModel
    table = SQLModel.metadata.tables.get("filerecord")
    if table is not None:
        table.drop(engine, checkfirst=True)
        table.create(engine, checkfirst=True)
    with Session(engine) as session:
        source_map = {}
        for source in sources:
            id = source.get("id", None)
            if not id:
                raise ValueError(f"Source must have an 'id' field: {json.dumps(source)}")
            SourceClass = import_client_class(source.get("class"))
            client = SourceClass(**source)
            if id in source_map:
                raise ValueError(f"Duplicate source ID: {id}")
            source_map[id] = client
            for record in client.scan():
                session.add(record)
                
        session.commit()
        
        # Processing phase: import processors listed in TOML and order by dependencies
        proc_map: dict[str, type] = {}
        for proc in processors:
            package_path = proc.get("class")
            ProcessorClass = import_processor_class(package_path)
            if package_path in proc_map:
                raise ValueError(f"Duplicate processor class: {package_path}")
            proc_map[package_path] = ProcessorClass
        ordered = sort_processors(proc_map)
        # fetch all files
        files = session.exec(select(FileRecord)).all()
        for record in files:
            # load prior results for this file
            prevs = {pr.processor_id: pr for pr in session.exec(
                select(ProcessorResult).where(ProcessorResult.file_id == record.id)
            ).all()}
            for proc_id, ProcessorClass in ordered:
                processor = ProcessorClass()
                prev = prevs.get(proc_id)
                prev_cache = prev.cache_key if prev else None
                if processor.should_run(record, prev_cache):
                    new_record = processor.run(record)
                    session.add(new_record)
                    # upsert ProcessorResult
                    new_key = processor.cache_key(record)
                    if prev:
                        prev.cache_key = new_key
                        prev.ran_at = datetime.datetime.utcnow()
                        session.add(prev)
                    else:
                        pr = ProcessorResult(
                            file_id=record.id,  # type: ignore
                            processor_id=proc_id,
                            cache_key=new_key,
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
