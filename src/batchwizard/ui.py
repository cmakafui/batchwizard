# ui.py
from __future__ import annotations

from collections import deque

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .models import JobRecord, JobState
from .processor import JobEvent

_STATE_COLORS = {
    JobState.COMPLETED: "green",
    JobState.FAILED: "red",
    JobState.EXPIRED: "red",
    JobState.CANCELLED: "red",
    JobState.PENDING: "yellow",
}


class Dashboard:
    """Live terminal dashboard driven by orchestrator JobEvents."""

    def __init__(self, console: Console | None = None, title: str = "BatchWizard"):
        self.console = console or Console()
        self.title = title
        self.jobs: dict[str, JobRecord] = {}
        self.progress: dict[str, str] = {}
        self.logs: deque[str] = deque(maxlen=10)
        self._live: Live | None = None

    # -- event sink -----------------------------------------------------------

    def on_event(self, event: JobEvent) -> None:
        if event.kind == "log":
            self.logs.append(event.message)
        elif event.job is not None:
            self.jobs[event.job.batch_id] = event.job
            if event.kind == "submitted":
                self.logs.append(f"Submitted {event.job.batch_id}")
            elif event.kind == "status" and event.message:
                self.progress[event.job.batch_id] = event.message
            elif event.kind == "finished":
                self.logs.append(f"Job {event.job.batch_id}: {event.job.state}")
                if event.job.error_summary:
                    self.logs.append(f"  ↳ {event.job.error_summary}")
                if event.job.output_path:
                    self.logs.append(f"  ↳ results: {event.job.output_path}")
                if event.job.error_path:
                    self.logs.append(f"  ↳ errors:  {event.job.error_path}")
        if self._live:
            self._live.update(self._render())

    # -- rendering ------------------------------------------------------------

    def _job_table(self) -> Table:
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Batch ID", style="dim", no_wrap=True)
        table.add_column("Status", style="bold")
        table.add_column("Progress", justify="right")
        for batch_id, job in self.jobs.items():
            color = _STATE_COLORS.get(job.state, "yellow")
            label = job.provider_status or job.state
            table.add_row(
                batch_id,
                f"[{color}]{label}[/{color}]",
                self.progress.get(batch_id, ""),
            )
        return table

    def _stats_table(self) -> Table:
        done = sum(1 for j in self.jobs.values() if j.state == JobState.COMPLETED)
        failed = sum(
            1
            for j in self.jobs.values()
            if j.state in (JobState.FAILED, JobState.EXPIRED, JobState.CANCELLED)
        )
        table = Table(show_header=False, expand=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        table.add_row("Total Jobs", str(len(self.jobs)))
        table.add_row("Completed", f"[green]{done}[/green]")
        table.add_row(
            "In Progress", f"[yellow]{len(self.jobs) - done - failed}[/yellow]"
        )
        table.add_row("Failed", f"[red]{failed}[/red]")
        return table

    def _render(self) -> Layout:
        layout = Layout()
        layout.split_column(Layout(name="header", size=3), Layout(name="body"))
        layout["body"].split_row(
            Layout(name="jobs", ratio=2), Layout(name="sidebar", ratio=1)
        )
        layout["body"]["sidebar"].split_column(
            Layout(name="stats", ratio=1), Layout(name="logs", ratio=2)
        )
        layout["header"].update(
            Panel(Text(self.title, style="bold blue"), border_style="blue")
        )
        layout["body"]["jobs"].update(
            Panel(self._job_table(), title="Job Status", border_style="magenta")
        )
        layout["body"]["sidebar"]["stats"].update(
            Panel(self._stats_table(), title="Statistics", border_style="cyan")
        )
        layout["body"]["sidebar"]["logs"].update(
            Panel("\n".join(self.logs), title="Logs", border_style="green")
        )
        return layout

    # -- lifecycle ------------------------------------------------------------

    def __enter__(self) -> Dashboard:
        self._live = Live(
            self._render(), console=self.console, screen=True, refresh_per_second=4
        )
        self._live.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        if self._live:
            self._live.__exit__(*exc)
            self._live = None

    def print_summary(self) -> None:
        done = sum(1 for j in self.jobs.values() if j.state == JobState.COMPLETED)
        failed = (
            len(self.jobs)
            - done
            - sum(1 for j in self.jobs.values() if j.state == JobState.PENDING)
        )
        self.console.print("[bold green]Processing completed![/bold green]")
        self.console.print(f"Total jobs: {len(self.jobs)}")
        self.console.print(f"Completed jobs: {done}")
        self.console.print(f"Failed jobs: {failed}")
        for job in self.jobs.values():
            if job.error_summary:
                self.console.print(f"[red]{job.batch_id}[/red]: {job.error_summary}")
