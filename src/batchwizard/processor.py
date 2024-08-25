# processor.py
import asyncio
from pathlib import Path
from typing import List, Optional

import aiofiles
from loguru import logger
from openai import AsyncOpenAI

from .config import config
from .models import BatchJob, BatchJobResult


class BatchProcessor:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=config.get_api_key())
        self.settings = config.settings

    async def upload_file(self, file_path: Path) -> Optional[str]:
        try:
            async with aiofiles.open(file_path, "rb") as file:
                file_content = await file.read()

            response = await self.client.files.create(
                file=(file_path.name, file_content), purpose="batch"
            )
            logger.info(
                f"File uploaded successfully: {response.id}, Filename: {file_path.name}"
            )
            return response.id
        except Exception as e:
            logger.error(f"Error uploading file {file_path.name}: {str(e)}")
            return None

    async def create_batch_job(self, input_file_id: str) -> Optional[BatchJob]:
        try:
            batch_job = await self.client.batches.create(
                input_file_id=input_file_id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
            logger.info(f"Created batch job with ID: {batch_job.id}")
            return BatchJob(
                id=batch_job.id,
                status=self.normalize_status(batch_job.status),
                input_file_id=input_file_id,
            )
        except Exception as e:
            logger.error(f"Error creating batch job: {str(e)}")
            return None

    async def check_batch_status(self, batch_id: str) -> Optional[str]:
        try:
            batch_job = await self.client.batches.retrieve(batch_id)
            return self.normalize_status(batch_job.status)
        except Exception as e:
            logger.error(f"Error checking batch status: {str(e)}")
            return None

    def normalize_status(self, status: str) -> str:
        """Normalize the status string to lowercase with underscores."""
        return status.lower().replace(" ", "_")

    async def download_batch_results(self, batch_job, output_file_path: Path) -> bool:
        try:
            if batch_job.status == "completed" and batch_job.output_file_id:
                result = await self.client.files.content(batch_job.output_file_id)
                async with aiofiles.open(output_file_path, "wb") as file:
                    await file.write(result.content)
                logger.info(f"Downloaded results to {output_file_path}")
                return True
            else:
                logger.warning(
                    f"Batch job not completed or missing output file. Status: {batch_job.status}"
                )
                return False
        except Exception as e:
            logger.error(f"Error downloading batch results: {str(e)}")
            return False

    async def process_batch_job(
        self, batch_job: BatchJob, output_dir: Path
    ) -> BatchJobResult:
        check_interval = self.settings.check_interval
        while True:
            status = await self.check_batch_status(batch_job.id)
            if status == "completed":
                try:
                    batch_job = await self.client.batches.retrieve(batch_job.id)
                    if batch_job.output_file_id:
                        output_file = output_dir / f"{batch_job.id}_results.jsonl"
                        if await self.download_batch_results(batch_job, output_file):
                            logger.info(
                                f"Successfully processed batch job {batch_job.id}"
                            )
                            return BatchJobResult(
                                job_id=batch_job.id,
                                success=True,
                                output_file_path=output_file,
                            )
                    else:
                        logger.error(
                            f"No output file ID found for completed batch job {batch_job.id}"
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing completed batch job {batch_job.id}: {str(e)}"
                    )
            elif status in ["failed", "expired", "cancelled"]:
                logger.error(f"Batch job {batch_job.id} {status}")
                return BatchJobResult(job_id=batch_job.id, success=False)
            elif status is None:
                logger.error(f"Failed to retrieve status for batch job {batch_job.id}")
                return BatchJobResult(job_id=batch_job.id, success=False)

            await asyncio.sleep(check_interval)
            check_interval = min(
                check_interval * 1.5, 60
            )  # Implement exponential backoff

    async def process_inputs(
        self, input_paths: List[Path], output_dir: Path
    ) -> List[BatchJobResult]:
        input_files = []
        for path in input_paths:
            if path.is_dir():
                input_files.extend(path.glob("*.jsonl"))
            elif path.suffix.lower() == ".jsonl":
                input_files.append(path)
            else:
                logger.warning(f"Skipping non-JSONL file: {path}")

        if not input_files:
            logger.warning("No input files found in the provided paths")
            return []

        semaphore = asyncio.Semaphore(self.settings.max_concurrent_jobs)

        async def process_file(input_file: Path) -> Optional[BatchJobResult]:
            async with semaphore:
                file_id = await self.upload_file(input_file)
                if file_id:
                    batch_job = await self.create_batch_job(file_id)
                    if batch_job:
                        return await self.process_batch_job(batch_job, output_dir)
            return None

        tasks = [process_file(file) for file in input_files]
        results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]

    async def close(self):
        await self.client.close()
