# cli.py
from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .config import BatchWizardSettings, config
from .models import TERMINAL_STATES, CollectionState
from .processor import BatchOrchestrator, JobEvent
from .providers import available_providers, get_provider
from .store import AmbiguousJobError, JobStore
from .ui import Dashboard
from .utils import get_api_key, set_api_key, setup_logger

app = typer.Typer(
    help="BatchWizard: Manage durable batch jobs across OpenAI and Anthropic"
)
console = Console()
logger = setup_logger(console)


def _validate_provider(provider: str) -> str:
    if provider not in available_providers():
        available = ", ".join(available_providers())
        raise typer.BadParameter(
            f"Unknown provider {provider!r}. Available: {available}",
            param_hint="provider",
        )
    return provider


def _require_api_key(provider: str) -> None:
    _validate_provider(provider)
    if not get_api_key(provider):
        logger.error(
            f"{provider.title()} API key not set. Configure it with "
            f"'batchwizard configure --provider {provider} --set-key YOUR_API_KEY'"
        )
        raise typer.Exit(code=1)


def _validate_submission_options(provider: str, endpoint: str | None) -> None:
    _validate_provider(provider)
    if provider != "openai" and endpoint is not None:
        raise typer.BadParameter(
            "--endpoint is OpenAI-specific and cannot be used with Anthropic",
            param_hint="endpoint",
        )


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
    table.add_column("Provider", style="blue")
    table.add_column("Batch ID", style="cyan")
    table.add_column("Input", style="green")
    for job in jobs:
        table.add_row(job.provider, job.batch_id, job.input_path)
    console.print(table)
    console.print(
        "Run [bold]batchwizard watch[/bold] any time to poll and download results."
    )


def _request_summary(job) -> str:
    if not job.total_count:
        return ""
    parts = [f"{job.completed_count} ok"]
    for count, label in (
        (job.failed_count, "failed"),
        (job.cancelled_count, "cancelled"),
        (job.expired_count, "expired"),
    ):
        if count:
            parts.append(f"{count} {label}")
    return " / ".join(parts)


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
    provider: str = typer.Option(
        "openai", "--provider", help="Batch provider: openai or anthropic"
    ),
    endpoint: str | None = typer.Option(
        None,
        help="Batch endpoint (e.g. /v1/chat/completions, /v1/responses, /v1/embeddings)",
    ),
    submit_only: bool = typer.Option(
        False,
        "--submit-only",
        help="Submit the batches and exit without waiting for completion",
    ),
):
    """Process batch jobs from input files or directories."""
    _validate_submission_options(provider, endpoint)
    _require_api_key(provider)
    output_dir = _default_output_dir(output_directory)
    batch_provider = get_provider(provider)
    store = JobStore(config.db_file)

    if submit_only:

        async def submit():
            orchestrator = BatchOrchestrator(batch_provider, store)
            try:
                return await orchestrator.submit_paths(input_paths, endpoint)
            finally:
                await batch_provider.close()

        _print_submitted(asyncio.run(submit()))
        return

    dashboard = Dashboard(Console(), title="BatchWizard Processing")

    async def run():
        orchestrator = BatchOrchestrator(
            batch_provider,
            store,
            on_event=dashboard.on_event,
            check_interval=check_interval,
            max_concurrent_jobs=max_concurrent_jobs,
        )
        try:
            with dashboard:
                await orchestrator.process(input_paths, output_dir, endpoint)
        finally:
            await batch_provider.close()

    _run(run())
    dashboard.print_summary()


@app.command()
def submit(
    input_paths: list[Path] = typer.Argument(
        ..., help="Paths to input files or directories to submit"
    ),
    provider: str = typer.Option(
        "openai", "--provider", help="Batch provider: openai or anthropic"
    ),
    endpoint: str | None = typer.Option(None, help="OpenAI Batch endpoint"),
):
    """Submit batch jobs and exit immediately (use 'watch' to collect results later)."""
    _validate_submission_options(provider, endpoint)
    _require_api_key(provider)
    batch_provider = get_provider(provider)
    store = JobStore(config.db_file)

    async def run():
        orchestrator = BatchOrchestrator(batch_provider, store)
        try:
            return await orchestrator.submit_paths(input_paths, endpoint)
        finally:
            await batch_provider.close()

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
    output_dir = _default_output_dir(output_directory)
    store = JobStore(config.db_file)
    actionable = store.actionable()
    if not actionable:
        console.print("[yellow]No actionable jobs in the manifest.[/yellow]")
        return
    dashboard = Dashboard(Console(), title="BatchWizard Watch")
    for job in actionable:  # seed the table before polling starts
        dashboard.jobs[(job.provider, job.batch_id)] = job

    async def run():
        groups: dict[str, list] = defaultdict(list)
        for job in actionable:
            groups[job.provider].append(job)

        async def watch_provider(provider_name: str, jobs: list) -> None:
            if provider_name not in available_providers():
                message = f"Provider {provider_name!r} is not installed"
                for job in jobs:
                    job.last_local_error = message
                    store.update(job)
                    dashboard.on_event(
                        JobEvent(kind="attention", job=job, message=message)
                    )
                return
            if not get_api_key(provider_name):
                message = f"Missing {provider_name.upper()} API key; configure it and rerun watch"
                for job in jobs:
                    job.last_local_error = message
                    store.update(job)
                    dashboard.on_event(
                        JobEvent(kind="attention", job=job, message=message)
                    )
                return

            batch_provider = get_provider(provider_name)
            orchestrator = BatchOrchestrator(
                batch_provider,
                store,
                on_event=dashboard.on_event,
                check_interval=check_interval,
            )
            try:
                await orchestrator.watch(jobs, output_dir)
            finally:
                await batch_provider.close()

        with dashboard:
            await asyncio.gather(
                *(watch_provider(name, jobs) for name, jobs in groups.items())
            )

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
            _request_summary(job),
            Path(job.input_path).name,
            job.updated_at,
            job.last_local_error or job.error_summary or "",
        )
    console.print(table)


@app.command()
def configure(
    set_key: str | None = typer.Option(
        None, "--set-key", help="Set an API key for the selected provider"
    ),
    provider: str = typer.Option("openai", "--provider", help="API provider"),
    show: bool = typer.Option(False, "--show", help="Show the current configuration"),
    reset: bool = typer.Option(
        False, "--reset", help="Reset the configuration to default values"
    ),
):
    """Manage BatchWizard configuration."""
    _validate_provider(provider)
    if set_key:
        set_api_key(set_key, provider)
        console.print(f"[green]{provider.title()} API key set successfully.[/green]")
    elif show:
        api_key = get_api_key(provider)
        masked_key = f"{api_key[:4]}...{api_key[-4:]}" if api_key else "Not set"
        console.print(f"{provider.title()} API Key: {masked_key}")
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
    provider: str = typer.Option("openai", "--provider", help="Batch provider"),
):
    """List recent batch jobs from the provider (not the local manifest)."""
    _require_api_key(provider)
    batch_provider = get_provider(provider)

    async def fetch_jobs():
        try:
            jobs = await batch_provider.list_jobs(limit=limit)
            table = Table(title=f"Batch Jobs ({provider})")
            table.add_column("Job ID", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Created At", style="green")
            table.add_column("Requests")
            for job in jobs:
                created_at = (
                    job.created_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                    if job.created_at is not None
                    else ""
                )
                table.add_row(
                    job.batch_id,
                    job.provider_status,
                    created_at,
                    _request_summary(job),
                )
            console.print(table)
        finally:
            await batch_provider.close()

    _run(fetch_jobs())


@app.command()
def cancel(
    job_id: str = typer.Argument(..., help="ID of the batch job to cancel"),
    provider: str | None = typer.Option(
        None, "--provider", help="Provider (inferred for tracked jobs)"
    ),
):
    """Cancel a specific batch job."""
    store = JobStore(config.db_file)
    try:
        tracked = store.get(job_id, provider=provider)
    except AmbiguousJobError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error
    provider_name = provider or (tracked.provider if tracked else "openai")
    _require_api_key(provider_name)
    batch_provider = get_provider(provider_name)

    async def cancel_job():
        try:
            orchestrator = BatchOrchestrator(batch_provider, store)
            status = await orchestrator.request_cancel(job_id)
            console.print(
                f"[green]Cancellation requested for {job_id} "
                f"(status: {status.provider_status}).[/green]"
            )
        except Exception as e:
            console.print(f"[red]Error cancelling job {job_id}: {e}[/red]")
        finally:
            await batch_provider.close()

    _run(cancel_job())


@app.command()
def download(
    job_id: str = typer.Argument(
        ..., help="ID of the batch job to download results for"
    ),
    output_directory: Path | None = typer.Option(
        None, help="Directory to store output files"
    ),
    provider: str | None = typer.Option(
        None, "--provider", help="Provider (inferred for tracked jobs)"
    ),
):
    """Download results (and error file, if any) for a batch job."""
    output_dir = _default_output_dir(output_directory)
    store = JobStore(config.db_file)
    try:
        tracked = store.get(job_id, provider=provider)
    except AmbiguousJobError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error
    provider_name = provider or (tracked.provider if tracked else "openai")
    _require_api_key(provider_name)
    batch_provider = get_provider(provider_name)

    async def download_results():
        try:
            job = tracked
            if job and job.state in TERMINAL_STATES:
                orchestrator = BatchOrchestrator(batch_provider, store)
                job = await orchestrator.collect(job, output_dir)
                if job.collection_state == CollectionState.FAILED:
                    console.print(f"[red]{job.last_local_error}[/red]")
                    return
                if job.collection_state == CollectionState.UNAVAILABLE:
                    console.print(f"[magenta]{job.error_summary}[/magenta]")
                    return
                output_path = job.output_path
                error_path = job.error_path
            else:
                results = await batch_provider.fetch_results(job_id, output_dir)
                output_path = str(results.output_path) if results.output_path else None
                error_path = str(results.error_path) if results.error_path else None
            if output_path:
                console.print(f"[green]Results saved to {output_path}[/green]")
            if error_path:
                console.print(
                    f"[yellow]Per-request errors saved to {error_path}[/yellow]"
                )
            if not output_path and not error_path:
                status = await batch_provider.status(job_id)
                console.print(
                    f"[yellow]No files available for {job_id} "
                    f"(status: {status.provider_status}).[/yellow]"
                )
                if status.error_summary:
                    console.print(f"[red]{status.error_summary}[/red]")
        except Exception as e:
            console.print(f"[red]Error downloading results for {job_id}: {e}[/red]")
        finally:
            await batch_provider.close()

    _run(download_results())


if __name__ == "__main__":
    app()
