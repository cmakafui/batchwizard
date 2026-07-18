# cli.py
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .config import BatchWizardSettings, config
from .models import TERMINAL_STATES, CollectionState
from .processor import BatchOrchestrator
from .providers import get_provider
from .store import JobStore
from .ui import Dashboard
from .utils import get_api_key, set_api_key, setup_logger

app = typer.Typer(help="BatchWizard: Manage LLM batch processing jobs with ease")
console = Console()
logger = setup_logger(console)

DEFAULT_ENDPOINT = "/v1/chat/completions"


def _require_api_key() -> None:
    if not get_api_key():
        logger.error(
            "API key not set. Please set it using 'batchwizard configure --set-key YOUR_API_KEY'"
        )
        raise typer.Exit(code=1)


def _default_output_dir(output_directory: Path | None) -> Path:
    out = output_directory or Path.cwd() / "results"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _run(coro) -> None:
    asyncio.run(coro)


def _print_submitted(jobs) -> None:
    if not jobs:
        console.print("[yellow]Nothing submitted.[/yellow]")
        return
    table = Table(title="Submitted Batches")
    table.add_column("Batch ID", style="cyan")
    table.add_column("Input", style="green")
    for job in jobs:
        table.add_row(job.batch_id, job.input_path)
    console.print(table)
    console.print(
        "Run [bold]batchwizard watch[/bold] any time to poll and download results."
    )


@app.command()
def process(
    input_paths: list[Path] = typer.Argument(
        ..., help="Paths to input files or directories for processing"
    ),
    output_directory: Path | None = typer.Option(
        None, help="Directory to store output files"
    ),
    max_concurrent_jobs: int = typer.Option(
        5, help="Maximum number of concurrent jobs"
    ),
    check_interval: int = typer.Option(
        5, help="Initial interval (in seconds) between job status checks"
    ),
    endpoint: str = typer.Option(
        DEFAULT_ENDPOINT,
        help="Batch endpoint (e.g. /v1/chat/completions, /v1/responses, /v1/embeddings)",
    ),
    submit_only: bool = typer.Option(
        False,
        "--submit-only",
        help="Submit the batches and exit without waiting for completion",
    ),
):
    """Process batch jobs from input files or directories."""
    _require_api_key()
    output_dir = _default_output_dir(output_directory)
    provider = get_provider()
    store = JobStore(config.db_file)

    if submit_only:

        async def submit():
            orchestrator = BatchOrchestrator(provider, store)
            try:
                return await orchestrator.submit_paths(input_paths, endpoint)
            finally:
                await provider.close()

        _print_submitted(asyncio.run(submit()))
        return

    dashboard = Dashboard(Console(), title="BatchWizard Processing")

    async def run():
        orchestrator = BatchOrchestrator(
            provider,
            store,
            on_event=dashboard.on_event,
            check_interval=check_interval,
            max_concurrent_jobs=max_concurrent_jobs,
        )
        try:
            with dashboard:
                await orchestrator.process(input_paths, output_dir, endpoint)
        finally:
            await provider.close()

    _run(run())
    dashboard.print_summary()


@app.command()
def submit(
    input_paths: list[Path] = typer.Argument(
        ..., help="Paths to input files or directories to submit"
    ),
    endpoint: str = typer.Option(DEFAULT_ENDPOINT, help="Batch endpoint"),
):
    """Submit batch jobs and exit immediately (use 'watch' to collect results later)."""
    _require_api_key()
    provider = get_provider()
    store = JobStore(config.db_file)

    async def run():
        orchestrator = BatchOrchestrator(provider, store)
        try:
            return await orchestrator.submit_paths(input_paths, endpoint)
        finally:
            await provider.close()

    _print_submitted(asyncio.run(run()))


@app.command()
def watch(
    output_directory: Path | None = typer.Option(
        None, help="Directory to store output files"
    ),
    check_interval: int = typer.Option(
        5, help="Initial interval (in seconds) between job status checks"
    ),
):
    """Advance remote jobs and retry any uncollected terminal artifacts."""
    _require_api_key()
    output_dir = _default_output_dir(output_directory)
    store = JobStore(config.db_file)
    actionable = store.actionable()
    if not actionable:
        console.print("[yellow]No actionable jobs in the manifest.[/yellow]")
        return
    provider = get_provider()

    dashboard = Dashboard(Console(), title="BatchWizard Watch")
    for job in actionable:  # seed the table before polling starts
        dashboard.jobs[job.batch_id] = job

    async def run():
        orchestrator = BatchOrchestrator(
            provider, store, on_event=dashboard.on_event, check_interval=check_interval
        )
        try:
            with dashboard:
                await orchestrator.watch(actionable, output_dir)
        finally:
            await provider.close()

    _run(run())
    dashboard.print_summary()


@app.command()
def status(
    all: bool = typer.Option(
        False, "--all", help="Show all jobs (default: actionable jobs only)"
    ),
):
    """Show jobs tracked in the local manifest."""
    store = JobStore(config.db_file)
    jobs = store.list() if all else store.actionable()
    if not jobs:
        console.print("[yellow]No jobs in the manifest.[/yellow]")
        return
    table = Table(title="Tracked Batch Jobs")
    table.add_column("Provider", style="blue")
    table.add_column("Batch ID", style="cyan")
    table.add_column("Remote", style="magenta")
    table.add_column("Artifacts")
    table.add_column("Requests", justify="right")
    table.add_column("Input", style="green")
    table.add_column("Updated", style="dim")
    table.add_column("Detail", style="red")
    for job in jobs:
        table.add_row(
            job.provider,
            job.batch_id,
            job.provider_status or job.state,
            job.collection_state,
            (
                f"{job.completed_count} ok / {job.failed_count} failed"
                if job.total_count
                else ""
            ),
            Path(job.input_path).name,
            job.updated_at,
            job.last_local_error or job.error_summary or "",
        )
    console.print(table)


@app.command()
def configure(
    set_key: str | None = typer.Option(
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
        console.print(f"Job manifest: {config.db_file}")
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
    limit: int = typer.Option(20, help="Number of jobs to display"),
):
    """List recent batch jobs from the provider (not the local manifest)."""
    _require_api_key()
    provider = get_provider()

    async def fetch_jobs():
        try:
            jobs = await provider.list_jobs(limit=limit)
            table = Table(title="Batch Jobs (provider)")
            table.add_column("Job ID", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Created At", style="green")
            table.add_column("Completed", style="blue")
            table.add_column("Failed", style="red")
            for job in jobs:
                created_at = (
                    datetime.fromtimestamp(job.created_at).strftime("%Y-%m-%d %H:%M:%S")
                    if job.created_at is not None
                    else ""
                )
                table.add_row(
                    job.batch_id,
                    job.provider_status,
                    created_at,
                    str(job.completed_count),
                    str(job.failed_count),
                )
            console.print(table)
        finally:
            await provider.close()

    _run(fetch_jobs())


@app.command()
def cancel(
    job_id: str = typer.Argument(..., help="ID of the batch job to cancel"),
):
    """Cancel a specific batch job."""
    _require_api_key()
    provider = get_provider()
    store = JobStore(config.db_file)

    async def cancel_job():
        try:
            orchestrator = BatchOrchestrator(provider, store)
            status = await orchestrator.request_cancel(job_id)
            console.print(
                f"[green]Cancellation requested for {job_id} "
                f"(status: {status.provider_status}).[/green]"
            )
        except Exception as e:
            console.print(f"[red]Error cancelling job {job_id}: {e}[/red]")
        finally:
            await provider.close()

    _run(cancel_job())


@app.command()
def download(
    job_id: str = typer.Argument(
        ..., help="ID of the batch job to download results for"
    ),
    output_directory: Path | None = typer.Option(
        None, help="Directory to store output files"
    ),
):
    """Download results (and error file, if any) for a batch job."""
    _require_api_key()
    output_dir = _default_output_dir(output_directory)
    provider = get_provider()
    store = JobStore(config.db_file)

    async def download_results():
        try:
            job = store.get(job_id)
            if job and job.state in TERMINAL_STATES:
                orchestrator = BatchOrchestrator(provider, store)
                job = await orchestrator.collect(job, output_dir)
                if job.collection_state == CollectionState.FAILED:
                    console.print(f"[red]{job.last_local_error}[/red]")
                    return
                output_path = job.output_path
                error_path = job.error_path
            else:
                results = await provider.fetch_results(job_id, output_dir)
                output_path = str(results.output_path) if results.output_path else None
                error_path = str(results.error_path) if results.error_path else None
            if output_path:
                console.print(f"[green]Results saved to {output_path}[/green]")
            if error_path:
                console.print(
                    f"[yellow]Per-request errors saved to {error_path}[/yellow]"
                )
            if not output_path and not error_path:
                status = await provider.status(job_id)
                console.print(
                    f"[yellow]No files available for {job_id} "
                    f"(status: {status.provider_status}).[/yellow]"
                )
                if status.error_summary:
                    console.print(f"[red]{status.error_summary}[/red]")
        except Exception as e:
            console.print(f"[red]Error downloading results for {job_id}: {e}[/red]")
        finally:
            await provider.close()

    _run(download_results())


if __name__ == "__main__":
    app()
