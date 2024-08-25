# cli.py
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import BatchWizardSettings, config
from .processor import BatchProcessor
from .ui import BatchWizardUI
from .utils import get_api_key, set_api_key, setup_logger

app = typer.Typer(help="BatchWizard: Manage OpenAI batch processing jobs with ease")
console = Console()
logger = setup_logger(console)


@app.command()
def process(
    input_paths: list[Path] = typer.Argument(
        ..., help="Paths to input files or directories for processing"
    ),
    output_directory: Optional[Path] = typer.Option(
        None, help="Directory to store output files"
    ),
    max_concurrent_jobs: int = typer.Option(
        5, help="Maximum number of concurrent jobs"
    ),
    check_interval: int = typer.Option(
        5, help="Initial interval (in seconds) between job status checks"
    ),
):
    """Process batch jobs from input files or directories."""
    if not output_directory:
        output_directory = Path.cwd() / "results"
    output_directory.mkdir(parents=True, exist_ok=True)

    api_key = get_api_key()
    if not api_key:
        logger.error(
            "API key not set. Please set it using 'openaibatch configure --set-key YOUR_API_KEY'"
        )
        raise typer.Exit(code=1)

    config.settings.max_concurrent_jobs = max_concurrent_jobs
    config.settings.check_interval = check_interval
    config.save()

    processor = BatchProcessor()
    ui = BatchWizardUI(Console())

    async def run_and_close():
        try:
            await ui.run_processing(processor, input_paths, output_directory)
        finally:
            await processor.close()

    asyncio.run(run_and_close())



@app.command()
def configure(
    set_key: Optional[str] = typer.Option(
        None, "--set-key", help="Set the OpenAI API key"
    ),
    show: bool = typer.Option(False, "--show", help="Show the current configuration"),
    reset: bool = typer.Option(
        False, "--reset", help="Reset the configuration to default values"
    ),
):
    """Manage BatchWizard configuration."""
    if set_key:
        set_api_key(set_key)
        console.print("[green]API key set successfully.[/green]")
    elif show:
        api_key = get_api_key()
        masked_key = f"{api_key[:4]}...{api_key[-4:]}" if api_key else "Not set"
        console.print(f"API Key: {masked_key}")
        console.print(f"Max Concurrent Jobs: {config.settings.max_concurrent_jobs}")
        console.print(f"Check Interval: {config.settings.check_interval} seconds")
    elif reset:
        config.settings = BatchWizardSettings()
        config.save()
        console.print("[yellow]Configuration reset to default values.[/yellow]")
    else:
        console.print(
            "Use --set-key, --show, or --reset options to manage configuration."
        )


@app.command()
def list_jobs(
    limit: int = typer.Option(100, help="Number of jobs to display"),
    all: bool = typer.Option(False, "--all", help="Display all jobs"),
):
    """List recent batch jobs."""

    async def fetch_jobs():
        processor = BatchProcessor()
        console = Console()  # Create a Console object directly
        try:
            jobs = await processor.client.batches.list(limit=None if all else limit)
            table = Table(title="Batch Jobs")
            table.add_column("Job ID", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Created At", style="green")
            table.add_column("Completed", style="blue")
            table.add_column("Failed", style="red")

            for job in jobs.data:
                created_at = datetime.fromtimestamp(job.created_at).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                table.add_row(
                    job.id,
                    job.status,
                    created_at,
                    str(job.request_counts.completed),
                    str(job.request_counts.failed),
                )
            console.print(table)  # Use the console object to print the table
        finally:
            await processor.close()

    asyncio.run(fetch_jobs())


@app.command()
def cancel(
    job_id: str = typer.Argument(..., help="ID of the batch job to cancel"),
):
    """Cancel a specific batch job."""

    async def cancel_job():
        processor = BatchProcessor()
        try:
            await processor.client.batches.cancel(job_id)
            console.print(f"[green]Job {job_id} cancelled successfully.[/green]")
        except Exception as e:
            console.print(f"[red]Error cancelling job {job_id}: {str(e)}[/red]")
        finally:
            await processor.close()

    asyncio.run(cancel_job())


@app.command()
def download(
    job_id: str = typer.Argument(
        ..., help="ID of the batch job to download results for"
    ),
    output_file: Path = typer.Option(
        None, help="Path to save the output file (default: <job_id>_results.jsonl)"
    ),
):
    """Download results for a completed batch job."""
    if not output_file:
        output_file = Path(f"{job_id}_results.jsonl")

    async def download_results():
        processor = BatchProcessor()
        try:
            batch_job = await processor.client.batches.retrieve(job_id)
            if batch_job.status != "completed":
                console.print(
                    f"[yellow]Job {job_id} is not completed (status: {batch_job.status}). Cannot download results.[/yellow]"
                )
                return

            success = await processor.download_batch_results(batch_job, output_file)
            if success:
                console.print(
                    f"[green]Results for job {job_id} downloaded successfully to {output_file}[/green]"
                )
            else:
                console.print(f"[red]Failed to download results for job {job_id}[/red]")
        except Exception as e:
            console.print(
                f"[red]Error downloading results for job {job_id}: {str(e)}[/red]"
            )
        finally:
            await processor.close()

    asyncio.run(download_results())


if __name__ == "__main__":
    app()
