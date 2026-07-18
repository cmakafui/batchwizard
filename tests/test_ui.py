from rich.console import Console

from batchwizard.models import CollectionState, JobRecord
from batchwizard.processor import JobEvent
from batchwizard.ui import Dashboard


def test_dashboard_renders_local_attention_separately_from_remote_state():
    dashboard = Dashboard(Console(width=120, record=True))
    job = JobRecord(batch_id="batch_1", input_path="/tmp/input.jsonl")
    job.collection_state = CollectionState.FAILED
    job.last_local_error = "Artifact collection failed: disk full"

    dashboard.on_event(
        JobEvent(kind="attention", job=job, message=job.last_local_error)
    )
    rendered = dashboard._render()

    assert rendered is not None
    assert "needs attention" in "\n".join(dashboard.logs)
    assert dashboard.jobs[(job.provider, job.batch_id)].is_actionable


def test_dashboard_table_and_summary_are_provider_aware():
    console = Console(width=160, record=True)
    dashboard = Dashboard(console)
    openai_job = JobRecord(
        provider="openai", batch_id="batch_1", input_path="/tmp/openai.jsonl"
    )
    anthropic_job = JobRecord(
        provider="anthropic",
        batch_id="msgbatch_1",
        input_path="/tmp/anthropic.jsonl",
    )
    anthropic_job.last_local_error = "network unavailable"
    dashboard.jobs = {
        (openai_job.provider, openai_job.reference): openai_job,
        (anthropic_job.provider, anthropic_job.reference): anthropic_job,
    }

    console.print(dashboard._job_table())
    dashboard.print_summary()
    rendered = console.export_text()

    assert "Provider" in rendered
    assert "openai" in rendered
    assert "anthropic" in rendered
    assert "openai: 1 jobs" in rendered
    assert "anthropic: 1 jobs" in rendered
    assert "anthropic:msgbatch_1" in rendered
