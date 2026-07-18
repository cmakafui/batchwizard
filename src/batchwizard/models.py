# models.py
from pathlib import Path

from pydantic import BaseModel


class BatchJob(BaseModel):
    id: str
    status: str
    input_file_id: str
    output_file_id: str | None = None


class BatchJobResult(BaseModel):
    job_id: str
    success: bool
    output_file_path: Path | None = None
