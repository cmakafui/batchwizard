# ui.py
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List

from loguru import logger
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn,
                           TaskProgressColumn, TextColumn, TimeElapsedColumn)
from rich.table import Table
from rich.text import Text

from .processor import BatchProcessor


class BatchWizardUI:
    def __init__(self, console: Console):
        self.console = console
        self.job_table = self.create_job_table()
        self.stats_table = self.create_stats_table()
        self.log_messages = []
        self.jobs = {}  # Dictionary to store job data
        self.total_jobs = 0
        self.completed_jobs = 0
        self.failed_jobs = 0

    def create_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(Layout(name="header", size=3), Layout(name="body"))
        layout["body"].split_row(
            Layout(name="main", ratio=2), Layout(name="sidebar", ratio=1)
        )
        layout["body"]["main"].split_column(
            Layout(name="progress", ratio=1), Layout(name="job_status", ratio=2)
        )
        layout["body"]["sidebar"].split_column(
            Layout(name="stats", ratio=1), Layout(name="logs", ratio=2)
        )
        return layout

    def create_progress_bars(self):
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
        )
        job_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
        )
        return overall_progress, job_progress

    def create_job_table(self):
        job_table = Table(show_header=True, header_style="bold magenta", expand=True)
        job_table.add_column("Job ID", style="dim", no_wrap=True)
        job_table.add_column("Status", style="bold")
        job_table.add_column("Progress", justify="right")
        return job_table

    def update_job_status(self, job_id: str, status: str, progress: str):
        color = (
            "green"
            if status == "completed"
            else "red" if status in ["failed", "expired", "cancelled"] else "yellow"
        )
        self.jobs[job_id] = (f"[{color}]{status}", progress)
        self.job_table = self.create_job_table()
        for job_id, (status, progress) in self.jobs.items():
            self.job_table.add_row(job_id, status, progress)

    def create_stats_table(self):
        stats_table = Table(show_header=False, expand=True)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", justify="right")
        stats_table.add_row("Total Jobs", "0")
        stats_table.add_row("Completed", "0")
        stats_table.add_row("In Progress", "0")
        stats_table.add_row("Failed", "0")
        return stats_table

    def update_stats(self):
        self.stats_table.rows.clear()
        self.stats_table.add_row("Total Jobs", str(self.total_jobs))
        self.stats_table.add_row("Completed", f"[green]{self.completed_jobs}[/green]")
        self.stats_table.add_row(
            "In Progress",
            f"[yellow]{self.total_jobs - self.completed_jobs - self.failed_jobs}[/yellow]",
        )
        self.stats_table.add_row("Failed", f"[red]{self.failed_jobs}[/red]")

    def add_log(self, message: str):
        self.log_messages.append(message)
        if len(self.log_messages) > 10:  # Keep only the last 10 log messages
            self.log_messages.pop(0)
        logger.info(message)

    def get_log_panel(self):
        return Panel("\n".join(self.log_messages), title="Logs", border_style="green")

    def update_layout(
        self,
        layout: Layout,
        header: str,
        overall_progress: Progress,
        job_table: Table,
        stats_table: Table,
        log_panel: Panel,
    ):
        layout["header"].update(
            Panel(Text(header, style="bold blue"), border_style="blue")
        )
        layout["body"]["main"]["progress"].update(
            Panel(overall_progress, title="Overall Progress", border_style="green")
        )
        layout["body"]["main"]["job_status"].update(
            Panel(job_table, title="Job Status", border_style="magenta")
        )
        layout["body"]["sidebar"]["stats"].update(
            Panel(stats_table, title="Statistics", border_style="cyan")
        )
        layout["body"]["sidebar"]["logs"].update(log_panel)

    async def run_processing(
        self, processor: BatchProcessor, input_paths: List[Path], output_dir: Path
    ):
        layout = self.create_layout()
        overall_progress, job_progress = self.create_progress_bars()

        overall_task = overall_progress.add_task("[bold blue]Processing", total=100)
        upload_task = overall_progress.add_task("[green]Uploading files", visible=False)
        process_task = overall_progress.add_task("[cyan]Processing jobs", visible=False)

        async def update_ui():
            self.update_layout(
                layout,
                "BatchWizard Processing",
                overall_progress,
                self.job_table,
                self.stats_table,
                self.get_log_panel(),
            )

        with Live(layout, console=self.console, screen=True, refresh_per_second=4):
            await update_ui()

            input_files = []
            for path in input_paths:
                if path.is_dir():
                    input_files.extend(path.glob("*.jsonl"))
                elif path.suffix.lower() == ".jsonl":
                    input_files.append(path)
                else:
                    self.add_log(f"Skipping non-JSONL file: {path}")

            if not input_files:
                self.add_log("No input files found in the provided paths")
                await update_ui()
                return

            overall_progress.update(upload_task, total=len(input_files), visible=True)
            overall_progress.update(process_task, total=len(input_files), visible=True)

            self.total_jobs = len(input_files)

            async def process_file(input_file: Path):
                file_id = await processor.upload_file(input_file)
                overall_progress.update(upload_task, advance=1)
                if file_id:
                    batch_job = await processor.create_batch_job(file_id)
                    if batch_job:
                        self.update_job_status(batch_job.id, batch_job.status, "0%")
                        self.add_log(f"Job created: {batch_job.id}")
                        await update_ui()

                        result = await processor.process_batch_job(
                            batch_job, output_dir
                        )
                        if result.success:
                            self.completed_jobs += 1
                            self.update_job_status(batch_job.id, "completed", "100%")
                            self.add_log(f"Job completed: {batch_job.id}")
                            if result.output_file_path:
                                self.add_log(
                                    f"Results saved to: {result.output_file_path}"
                                )
                        else:
                            self.failed_jobs += 1
                            self.update_job_status(batch_job.id, "failed", "100%")
                            self.add_log(f"Job failed: {batch_job.id}")

                        overall_progress.update(process_task, advance=1)
                        self.update_stats()
                        await update_ui()
                    else:
                        self.add_log(
                            f"Failed to create batch job for {input_file.name}"
                        )
                else:
                    self.add_log(f"Failed to upload file {input_file.name}")
                await update_ui()

            tasks = [process_file(file) for file in input_files]
            await asyncio.gather(*tasks)

            overall_progress.update(overall_task, completed=100)
            await update_ui()

        self.console.print("[bold green]Processing completed![/bold green]")
        self.console.print(f"Total jobs: {self.total_jobs}")
        self.console.print(f"Completed jobs: {self.completed_jobs}")
        self.console.print(f"Failed jobs: {self.failed_jobs}")
        if self.completed_jobs > 0:
            self.console.print(f"Results saved in: {output_dir}")

    def display_job_list(self, jobs: List[dict]):
        table = Table(title="Batch Jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Created At", style="green")
        table.add_column("Completed", style="blue")
        table.add_column("Failed", style="red")

        for job in jobs:
            created_at = datetime.fromtimestamp(job["created_at"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            table.add_row(
                job["id"],
                job["status"],
                created_at,
                str(job["request_counts"]["completed"]),
                str(job["request_counts"]["failed"]),
            )

        self.console.print(table)

    def display_cancel_result(self, job_id: str, success: bool):
        if success:
            self.console.print(f"[green]Job {job_id} cancelled successfully.[/green]")
        else:
            self.console.print(f"[red]Failed to cancel job {job_id}.[/red]")

    def display_download_result(self, job_id: str, output_file: Path, success: bool):
        if success:
            self.console.print(
                f"[green]Results for job {job_id} downloaded successfully to {output_file}[/green]"
            )
        else:
            self.console.print(
                f"[red]Failed to download results for job {job_id}[/red]"
            )
