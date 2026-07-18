# cli.py
from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from .config import BatchWizardSettings, config
from .models import TERMINAL_STATES, CollectionState, JobState
from .processor import BatchOrchestrator, JobEvent
from .providers import available_providers, get_provider
from .store import AmbiguousJobError, JobStore
from .ui import Dashboard
from .utils import get_api_key, set_api_key, setup_logger

app = typer.Typer(help="Manage LLM batch jobs across OpenAI and Anthropic")
console = Console()
logger = setup_logger(console)

DEFAULT_MAX_CONCURRENT_JOBS = config.settings.max_concurrent_jobs
DEFAULT_CHECK_INTERVAL = config.settings.check_interval


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


def _provider_for_job_id(job_id: str, explicit: str | None, tracked) -> str:
    if explicit is not None:
        return _validate_provider(explicit)
    if tracked is not None:
        return tracked.provider
    if job_id.startswith("msgbatch_"):
        return "anthropic"
    if job_id.startswith("batch_"):
        return "openai"
    raise typer.BadParameter(
        "Cannot infer the provider for an untracked job; specify --provider",
        param_hint="provider",
    )


def _format_age(timestamp: str, now: datetime | None = None) -> str:
    try:
        updated = datetime.fromisoformat(timestamp)
    except ValueError:
        return timestamp
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    seconds = max(0, int(((now or datetime.now(UTC)) - updated).total_seconds()))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86_400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86_400}d ago"


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
        table.add_row(job.provider, job.reference, job.input_path)
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
    input_paths: Annotated[
        list[Path],
        typer.Argument(help="Paths to input files or directories for processing"),
    ],
    output_directory: Annotated[
        Path | None, typer.Option(help="Directory to store output files")
    ] = None,
    max_concurrent_jobs: Annotated[
        int, typer.Option(min=1, help="Maximum number of concurrent jobs")
    ] = DEFAULT_MAX_CONCURRENT_JOBS,
    check_interval: Annotated[
        int,
        typer.Option(
            min=0,
            help="Initial interval (in seconds) between job status checks",
        ),
    ] = DEFAULT_CHECK_INTERVAL,
    provider: Annotated[
        str, typer.Option("--provider", help="Batch provider: openai or anthropic")
    ] = "openai",
    endpoint: Annotated[
        str | None,
        typer.Option(
            help="Batch endpoint (e.g. /v1/chat/completions, /v1/responses, /v1/embeddings)"
        ),
    ] = None,
    submit_only: Annotated[
        bool,
        typer.Option(
            "--submit-only",
            help="Submit the batches and exit without waiting for completion",
        ),
    ] = False,
):
    """Process batch jobs from input files or directories."""
    _validate_submission_options(provider, endpoint)
    _require_api_key(provider)
    output_dir = _default_output_dir(output_directory)
    batch_provider = get_provider(provider)
    store = JobStore(config.db_file)

    if submit_only:

        async def submit():
            orchestrator = BatchOrchestrator(
                batch_provider, store, max_concurrent_jobs=max_concurrent_jobs
            )
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
    input_paths: Annotated[
        list[Path], typer.Argument(help="Paths to input files or directories to submit")
    ],
    provider: Annotated[
        str, typer.Option("--provider", help="Batch provider: openai or anthropic")
    ] = "openai",
    endpoint: Annotated[str | None, typer.Option(help="OpenAI Batch endpoint")] = None,
    max_concurrent_jobs: Annotated[
        int,
        typer.Option(min=1, help="Maximum number of concurrent submissions"),
    ] = DEFAULT_MAX_CONCURRENT_JOBS,
):
    """Submit batch jobs and exit immediately (use 'watch' to collect results later)."""
    _validate_submission_options(provider, endpoint)
    _require_api_key(provider)
    batch_provider = get_provider(provider)
    store = JobStore(config.db_file)

    async def run():
        orchestrator = BatchOrchestrator(
            batch_provider, store, max_concurrent_jobs=max_concurrent_jobs
        )
        try:
            return await orchestrator.submit_paths(input_paths, endpoint)
        finally:
            await batch_provider.close()

    _print_submitted(asyncio.run(run()))


@app.command()
def watch(
    output_directory: Annotated[
        Path | None, typer.Option(help="Directory to store output files")
    ] = None,
    check_interval: Annotated[
        int,
        typer.Option(
            min=0,
            help="Initial interval (in seconds) between job status checks",
        ),
    ] = DEFAULT_CHECK_INTERVAL,
    max_concurrent_jobs: Annotated[
        int,
        typer.Option(
            min=1,
            help="Maximum number of concurrently watched jobs per provider",
        ),
    ] = DEFAULT_MAX_CONCURRENT_JOBS,
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
        dashboard.jobs[(job.provider, job.reference)] = job

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
                max_concurrent_jobs=max_concurrent_jobs,
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
    all_jobs: Annotated[
        bool,
        typer.Option("--all", help="Show all jobs (default: actionable jobs only)"),
    ] = False,
):
    """Show jobs tracked in the local manifest."""
    store = JobStore(config.db_file)
    jobs = store.list() if all_jobs else store.actionable()
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
            job.reference,
            job.provider_status or job.state,
            job.collection_state,
            _request_summary(job),
            Path(job.input_path).name,
            _format_age(job.updated_at),
            job.last_local_error or job.error_summary or "",
        )
    console.print(table)


@app.command()
def configure(
    set_key: Annotated[
        str | None,
        typer.Option("--set-key", help="Set an API key for the selected provider"),
    ] = None,
    provider: Annotated[str, typer.Option("--provider", help="API provider")] = (
        "openai"
    ),
    show: Annotated[
        bool, typer.Option("--show", help="Show the current configuration")
    ] = False,
    reset: Annotated[
        bool, typer.Option("--reset", help="Reset the configuration to defaults")
    ] = False,
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
def reconcile(
    intent_id: Annotated[
        str | None,
        typer.Argument(
            help="Submission intent to reconcile (omit to list unresolved intents)"
        ),
    ] = None,
    batch_id: Annotated[
        str | None,
        typer.Option(
            "--batch-id",
            help="Provider batch ID when automatic metadata matching is unavailable",
        ),
    ] = None,
    discard: Annotated[
        bool,
        typer.Option(
            "--discard",
            help="Delete an intent after confirming that no provider batch exists",
        ),
    ] = False,
):
    """Attach an uncertain submission intent to its provider batch."""
    store = JobStore(config.db_file)
    if intent_id is None:
        unresolved = store.unresolved()
        if not unresolved:
            console.print("[green]No unresolved submission intents.[/green]")
            return
        table = Table(title="Unresolved Submission Intents")
        table.add_column("Intent", style="cyan")
        table.add_column("Provider", style="blue")
        table.add_column("Input", style="green")
        table.add_column("Created", style="dim")
        table.add_column("Detail", style="red")
        for job in unresolved:
            table.add_row(
                job.intent_id,
                job.provider,
                job.input_path,
                _format_age(job.created_at),
                job.last_local_error or "",
            )
        console.print(table)
        console.print(
            "Run [bold]batchwizard reconcile INTENT[/bold] for OpenAI metadata "
            "matching, or add [bold]--batch-id ID[/bold] after identifying the job."
        )
        return

    job = store.get_intent(intent_id)
    if job is None:
        console.print(f"[red]Unknown submission intent {intent_id!r}.[/red]")
        raise typer.Exit(code=2)
    if job.batch_id is not None:
        console.print(
            f"[yellow]Intent {intent_id} is already attached to {job.batch_id}.[/yellow]"
        )
        return
    if discard:
        if batch_id is not None:
            raise typer.BadParameter(
                "--discard cannot be combined with --batch-id", param_hint="discard"
            )
        store.delete(job)
        console.print(f"[yellow]Discarded unresolved intent {intent_id}.[/yellow]")
        return

    _require_api_key(job.provider)
    batch_provider = get_provider(job.provider)

    async def resolve():
        candidate = batch_id
        endpoint = job.endpoint
        try:
            if candidate is None:
                recent = await batch_provider.list_jobs(limit=100)
                match = next(
                    (item for item in recent if item.intent_id == intent_id), None
                )
                if match is None:
                    raise ValueError(
                        "No provider batch with this intent metadata was found; "
                        "identify it with list-jobs and pass --batch-id"
                    )
                candidate = match.batch_id
                endpoint = match.endpoint or endpoint

            status = await batch_provider.status(candidate)
            job.batch_id = candidate
            job.endpoint = endpoint
            job.state = JobState.PENDING
            orchestrator = BatchOrchestrator(batch_provider, store)
            orchestrator._apply_status(job, status)
            store.update(job)
            return job
        finally:
            await batch_provider.close()

    try:
        resolved = asyncio.run(resolve())
    except Exception as error:
        console.print(f"[red]Could not reconcile {intent_id}: {error}[/red]")
        raise typer.Exit(code=1) from error
    console.print(
        f"[green]Attached intent {intent_id} to {resolved.provider} "
        f"batch {resolved.batch_id}.[/green]"
    )


@app.command()
def list_jobs(
    limit: Annotated[int, typer.Option(min=1, help="Number of jobs to display")] = 20,
    provider: Annotated[str, typer.Option("--provider", help="Batch provider")] = (
        "openai"
    ),
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
            table.add_column("Intent", style="dim")
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
                    job.intent_id or "",
                    _request_summary(job),
                )
            console.print(table)
        finally:
            await batch_provider.close()

    _run(fetch_jobs())


@app.command()
def cancel(
    job_id: Annotated[str, typer.Argument(help="ID of the batch job to cancel")],
    provider: Annotated[
        str | None,
        typer.Option("--provider", help="Provider (inferred for tracked jobs)"),
    ] = None,
):
    """Cancel a specific batch job."""
    store = JobStore(config.db_file)
    if provider is not None:
        _validate_provider(provider)
    try:
        tracked = store.get(job_id, provider=provider)
    except AmbiguousJobError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error
    provider_name = _provider_for_job_id(job_id, provider, tracked)
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
    job_id: Annotated[
        str, typer.Argument(help="ID of the batch job to download results for")
    ],
    output_directory: Annotated[
        Path | None, typer.Option(help="Directory to store output files")
    ] = None,
    provider: Annotated[
        str | None,
        typer.Option("--provider", help="Provider (inferred for tracked jobs)"),
    ] = None,
):
    """Download results (and error file, if any) for a batch job."""
    output_dir = _default_output_dir(output_directory)
    store = JobStore(config.db_file)
    if provider is not None:
        _validate_provider(provider)
    try:
        tracked = store.get(job_id, provider=provider)
    except AmbiguousJobError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error
    provider_name = _provider_for_job_id(job_id, provider, tracked)
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
