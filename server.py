from fastapi import FastAPI, BackgroundTasks
from typing import List, Dict, Any, Optional
from client_localfs import LocalFSClient  # changed to absolute import
import uuid

app = FastAPI()

# In-memory job store for demonstration
jobs: Dict[str, Dict[str, Any]] = {}

@app.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    return jobs.get(job_id, {"status": "not found"})

@app.post("/scan/local")
def scan_local_source(
    path: str,
    background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "results": []}
    client = LocalFSClient(path)
    def scan_job():
        jobs[job_id]["status"] = "running"
        results = []
        for file_info in client.scan():
            results.append(file_info)
        jobs[job_id]["status"] = "done"
        jobs[job_id]["results"] = results
    background_tasks.add_task(scan_job)
    return {"job_id": job_id}

@app.get("/scan/local/list")
def list_local_files(path: str):
    client = LocalFSClient(path)
    return list(client.scan())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)
